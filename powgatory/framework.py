#!/usr/bin/env python3
"""
b2luigi Framework Base Classes for POWHEG-BOX Workflow

This module provides the base task classes and workflow definitions
that leverage b2luigi for workflow orchestration.
"""

import os
import shutil
import luigi
import b2luigi
import subprocess
from pathlib import Path
from typing import Iterable, List, Tuple

from luigi.task import TASK_ID_INVALID_CHAR_REGEX

from .config import POWHEGConfig


def get_stage_number(stage: str) -> int:
    """The number of a stage: 1-4 for stage1-stage4, 3 for stage3_fullgrid, 4 for addweights."""
    if stage.startswith("stage") and not stage.endswith("_fullgrid"):
        return int(stage.replace("stage", ""))
    elif stage == "addweights":
        return 4
    elif stage == "stage3_fullgrid":
        return 3
    else:
        raise ValueError(f"Unknown stage: {stage}")


def get_stage_str(stage_number: int) -> str:
    """The name of stage 1-4, e.g. stage2."""
    assert stage_number in [1, 2, 3, 4], "Stage number must be between 1 and 4"

    return f"stage{stage_number}"


def seed_runs(seeds: Iterable[int]) -> List[Tuple[int, int]]:
    """The seeds as runs of consecutive numbers (first, last): e.g. (1, 2, 3, 5) -> [(1, 3), (5, 5)]."""
    runs = []
    for seed in sorted(seeds):
        if runs and seed == runs[-1][1] + 1:
            runs[-1] = (runs[-1][0], seed)
        else:
            runs.append((seed, seed))
    return runs


class SharedDictParameter(b2luigi.DictParameter):
    """
    A DictParameter whose equal values share one frozen dict and one serialized string per process.

    luigi freezes a new copy of the dict for every task, and the scheduler keeps a JSON string of it
    per task. With thousands of branches per config, and many configs in one POWgatory, those copies
    fill the memory. The caches are keyed by the JSON text, which is what the task ID is computed
    from: FrozenOrderedDict compares equal regardless of the key order, the JSON does not.
    """
    _frozen_by_text = {}
    _shared_texts   = {}

    def normalize(self, value):
        frozen = super().normalize(value)
        return self._frozen_by_text.setdefault(super().serialize(frozen), frozen)

    def serialize(self, value):
        text = super().serialize(value)
        return self._shared_texts.setdefault(text, text)


class Task(b2luigi.Task):
    """
    Base task class for all POWHEG tasks.

    Provides:
    - Version parameter for output organization
    - Local target helpers for storing outputs
    - Common configuration access
    """

    version = luigi.Parameter(
        default="v1",
        description="Output version identifier for organizing results"
    )

    def store_parts(self):
        """
        Define the directory structure for task outputs.
        Override in subclasses for custom organization.
        """
        # return (self.__class__.__name__, self.version)
        return ()

    def base_path(self):
        raise NotImplementedError("Subclasses must implement base_path() to define output paths.")

    def local_path(self, *path):
        """
        Build local path for task outputs.
        Relative to base_path(), i.e. the run directory from the configuration.
        Arguments:
            *path (list[str]): Additional path components.
        """
        base_dir = self.base_path()
        parts    = (base_dir,) + self.store_parts() + path
        return os.path.join(*parts)

    def local_target(self, *path):
        """
        Create a local file target for task outputs.
        """
        return b2luigi.LocalTarget(self.local_path(*path))

    def __name__(self):
        """
        Return the name of the task class. (Can be overridden for custom naming.)
        """
        return self.__class__.__name__

    def publish_message(self, message: str):
        """
        Log a message to the user.
        """
        print(f"[{self.__name__}] {message}")


class POWHEGBaseTask(Task):
    """
    Base class for all POWHEG tasks.

    Provides:
    - Configuration loading
    - Container management
    - Script generation utilities
    """

    stage          = b2luigi.Parameter(
        default="stage1",
        description="POWHEG stage number (stage1, stage2, stage3, stage4, stage3_fullgrid, lhef, pythia, addweights)"
    )
    grid_iteration = b2luigi.IntParameter(
        default=1,
        description="Grid iteration number (1, 2, 3, ...)"
    )

    config = SharedDictParameter(hashed=True,
                                 default={},
                                 description="POWHEG configuration dictionary",
                                )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # luigi's task ID shows the first 16 characters of the first three parameters by name, e.g.
        # POWHEGStage__1__5__10__6__8____powheg_executa_1_34f94f46de. Its hash of all parameters
        # keeps the ID unique; put a readable label before it instead. Batch jobs find their task
        # by this ID, so it may depend on nothing but the parameters.
        parameter_hash = self.task_id.rsplit("_", 1)[1]
        label          = TASK_ID_INVALID_CHAR_REGEX.sub("_", self.task_label())
        self.task_id   = f"{self.get_task_family()}_{label}_{parameter_hash}"

    def task_label(self) -> str:
        """The label of the task ID: the stage and grid iteration as in file names, e.g. p1-x2."""
        return self.stage_code(self.stage, self.grid_iteration)

    @property
    def retry_count(self) -> int:
        """
        luigi disables a task after this many failures, so 1 means luigi never retries it.
        luigi cannot tell why a task failed, and a POWHEG failure would only fail again with the
        same seed and input. b2luigi itself resubmits Slurm jobs that were lost, stuck in
        COMPLETING or hung (see POWHEGStage.slurm_max_resubmissions and job_settings.retries).
        """
        return 1

    def __repr__(self):
        """
        Overwrite the default string representation of the task to not print the whole configuration.
        Just print the name of the file instead.

        Build a task representation like `MyTask(param1=1.5, param2='5', config_file='config.yaml')`.
        """
        params = self.get_params()
        param_values = self.get_param_values(params, [], self.param_kwargs)

        # Build up task id
        repr_parts = []
        param_objs = dict(params)
        # Avoid printing of 'grid_iteration' parameter for stages != 1:
        # grid_iteration_stage True for grid iteration stage.
        grid_iteration_stage = False
        for param_name, param_value in param_values:
            if param_name == "stage" and param_value == "stage1":
                grid_iteration_stage = True
                break

        for param_name, param_value in param_values:
            if param_objs[param_name].significant:
                if param_name == "config":
                    # For the config parameter, just show the config file name instead of the whole dictionary
                    config_file = Path(param_value.get("config_file", "unknown_config.yaml"))
                    # Express the config file path relative to the current working directory for better readability
                    config_file = config_file.relative_to(Path(self.config["cwd"]))
                    repr_parts.append(f"{param_name:s}={str(config_file):s}")
                elif param_name == "grid_iteration" and not grid_iteration_stage:
                    continue
                elif param_name == "version":
                    continue  # Skip version in the representation, since its printed once at the beginning of the workflow
                elif isinstance(param_value, tuple):
                    # A group of seeds, e.g. branch_id=1-3,5,8-10
                    runs = ",".join(f"{first}-{last}" if last > first else f"{first}" for first, last in seed_runs(param_value))
                    repr_parts.append(f"{param_name:s}={runs:s}")
                else:
                    repr_parts.append(f"{param_name:s}={param_objs[param_name].serialize(param_value):s}")

        task_str = f"{self.get_task_family()}({', '.join(repr_parts)})"

        return task_str

    def base_path(self, *path):
        """
        Return the base path for task outputs, derived from the configuration.
        """
        return self.config["job_settings"]["run_dir"]

    @property
    def stage_number(self) -> int:
        """Return the numeric stage number for resource lookup."""
        return get_stage_number(self.stage)

    @property
    def stage_name(self) -> str:
        """Stage name for resource lookup. Override in subclasses."""
        raise NotImplementedError

    def get_config(self) -> POWHEGConfig:
        """
        Convert the stored dictionary back to a POWHEGConfig instance.
        """
        if isinstance(self.config, POWHEGConfig):
            return self.config
        else:
            return POWHEGConfig(**self.config)

    def get_log_file_dir(self):
        # Directory where b2luigi task and log files are stored
        path = os.path.join(self.config["job_settings"]["log_dir"])
        return path

    def setup_container(self):
        """
        Setup container image if needed.
        - If container_image is empty: run without container
        - If container_image is a local path: copy to powheg.sif,
          or use it in place if job_settings.copy_container_image is false
        - If container_image is a registry URL: pull to powheg.sif

        Returns:
            Path to the container image (powheg.sif, or the local image used in place) if used, else None.
        """
        container_image = self.config['job_settings']['container_image']
        sif_path = Path(self.config['job_settings']['run_dir']) / 'powheg.sif'

        if not container_image:
            self.publish_message("No container image specified - will run directly")
            return None

        # Many runs can share one local image, instead of a copy per run directory.
        if "://" not in container_image and not self.config['job_settings']['copy_container_image']:
            local_path = Path(container_image).resolve()
            if not local_path.exists():
                raise FileNotFoundError(f"Local container image not found: {local_path}")
            self.publish_message(f"Using local container in place: {local_path}")
            return local_path

        if sif_path.exists():
            self.publish_message(f"Container already exists at {sif_path}")
            return sif_path

        # Check if it's a local path:
        # If the container_image string contains "://", treat it as a registry URL; otherwise, treat it as a local path.
        if "://" not in container_image:
            local_path = Path(container_image).resolve()
            # if not local_path.is_absolute():
            #     local_path = Path(self.config["cwd"]) / local_path

            if local_path.exists():
                self.publish_message(f"Copying local container from {local_path}")
                shutil.copy2(local_path, sif_path)
            else:
                raise FileNotFoundError(f"Local container image not found: {local_path}")
        else:
            # It's a registry URL - pull it
            self.publish_message(f"Pulling container image: {container_image}")
            result = subprocess.run(
                ['apptainer', 'pull', str(sif_path), container_image],
                cwd=self.config["job_settings"]["run_dir"],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                raise RuntimeError(f"Failed to pull container: {result.stderr}")

        return sif_path

    def format_branch_id(self, seed: int | tuple) -> str:
        """
        The seed, or a group of seeds (a tuple), for job names and file names: e.g. s7, or
        s1to3_5_8to10 for (1, 2, 3, 5, 8, 9, 10).
        """
        if isinstance(seed, tuple):
            return "s" + "_".join(f"{first}to{last}" if last > first else f"{first}" for first, last in seed_runs(seed))
        else:
            return f"s{seed}"

    def stage_code(self, stage: str, grid: int = -1) -> str:
        """
        Generate unique stage identifier for job naming.

        Args:
            stage (str): Stage name (e.g., "stage1", "stage2", "stage3", "stage4", "addweights", "analysis").
            grid (int): Grid iteration number (default: -1, meaning no grid iteration).
        Returns:
            str: Unique stage identifier (e.g., "p1-x1" for stage1 grid iteration 1).
        """
        if isinstance(stage, str) and stage.startswith("stage") and not stage.endswith("_fullgrid"):
            # stage_number = int(stage.replace("stage", ""))
            if grid > 0:
                return f"p{get_stage_number(stage)}-x{grid}"
            else:
                return f"p{get_stage_number(stage)}"
        else:
            return f"{stage}"

    def job_code(self, stage: str, grid: int = -1, seed: int | tuple = 1) -> str:
        """
        Generate unique job identifier.
        Args:
            stage (str): Stage name (e.g., "stage1", "stage2", "stage3", "stage4", "lhef").
            grid (int): Grid iteration number (default: -1, meaning no grid iteration).
            seed (int | tuple): Seed of the job, or a group of seeds (default: 1).
        Returns:
            str: Unique job identifier (e.g., "p1-x1-s1" for stage1 grid iteration 1 with seed 1,
                 or "p1-x1-s1to10" for a group).
        """
        return f"{self.stage_code(stage, grid)}-{self.format_branch_id(seed)}"


class POWHEGWrapperTask(b2luigi.WrapperTask):
    """
    Main workflow task that orchestrates all stages.

    This is a wrapper task that requires the appropriate final stage
    based on the run.yaml configuration.
    """

    version = b2luigi.Parameter(
        default="v1",
        description="Output version identifier for organizing results",
        hashed=True
    )

    config = SharedDictParameter(hashed=True,
                                 default={},
                                 description="POWHEG configuration dictionary"
                                )

    def get_config(self) -> POWHEGConfig:
        """
        Convert the stored dictionary back to a POWHEGConfig instance.
        """
        if isinstance(self.config, POWHEGConfig):
            return self.config
        else:
            return POWHEGConfig(**self.config)
