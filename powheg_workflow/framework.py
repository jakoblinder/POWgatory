#!/usr/bin/env python3
"""
b2luigi Framework Base Classes for POWHEG-BOX Workflow

This module provides the base task classes and workflow definitions
that leverage b2luigi for workflow orchestration.
"""

import os
import math
import sys
import shutil
import luigi
import b2luigi
import subprocess
from pathlib import Path

from .config import POWHEGConfig


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
        # return os.environ.get('POWHEG_RUN_DIR', os.getcwd())
        raise NotImplementedError("Subclasses must implement base_path() to define output paths.")

    def local_path(self, *path):
        """
        Build local path for task outputs.
        Uses POWHEG_OUTPUT_DIR environment variable or current directory.
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
        description="POWHEG stage number (stage1, stage2, stage3, stage4, stage3_gridcombine, analysis, addweights)"
    )
    grid_iteration = b2luigi.IntParameter(
        default=1,
        description="Grid iteration number (1, 2, 3, ...)"
    )

    config = b2luigi.DictParameter(hashed=True,
                                   default={},
                                   description="POWHEG configuration dictionary",
                                  )

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
        for param_name, param_value in param_values:
            if param_objs[param_name].significant:
                if param_name == "config":
                    # For the config parameter, just show the config file name instead of the whole dictionary
                    config_file = Path(param_value.get("config_file", "unknown_config.yaml"))
                    # Express the config file path relative to the current working directory for better readability
                    config_file = config_file.relative_to(Path(self.config["cwd"]))
                    repr_parts.append(f"{param_name:s}={str(config_file):s}")
                elif param_name == "version":
                    continue  # Skip version in the representation, since its printed once at the beginning of the workflow
                else:
                    repr_parts.append(f"{param_name:s}={param_objs[param_name].serialize(param_value):s}")

        task_str = f"{self.get_task_family()}({', '.join(repr_parts)})"

        return task_str

    def base_path(self, *path):
        """
        Return the base path for task outputs, derived from the configuration.
        """
        return self.config["job_settings"]["run_dir"]

    @staticmethod
    def get_stage_number(stage:str) -> int:
        """Return the numeric stage number for resource lookup."""
        if stage.startswith("stage") and not stage.endswith("_gridcombine"):
            return int(stage.replace("stage", ""))
        elif stage == "addweights":
            return 4
        elif stage == "analysis":
            return 1  # Analysis stage doesn't have a specific number
        elif stage == "stage3_gridcombine":
            return 3
        else:
            raise ValueError(f"Unknown stage: {stage}")

    @property
    def stage_number(self) -> int:
        """Return the numeric stage number for resource lookup."""
        return self.get_stage_number(self.stage)

    @staticmethod
    def get_stage_str(stage_number: int) -> str:
        """Convert numeric stage number to string representation."""
        assert stage_number in [1, 2, 3, 4], "Stage number must be between 1 and 4"

        return f"stage{stage_number}"

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
        # Directorry where b2luigi task and log files are stored
        path = os.path.join(self.config["job_settings"]["log_dir"])
        return path

    def setup_container(self):
        """
        Setup container image if needed.
        - If container_image is empty: run without container
        - If container_image is a local path: copy to powheg.sif
        - If container_image is a registry URL: pull to powheg.sif

        Returns:
            Path to the container image (powheg.sif) if used, else None.
        """
        container_image = self.config['job_settings']['container_image']
        sif_path = Path(self.config['job_settings']['run_dir']) / 'powheg.sif'

        if not container_image:
            self.publish_message("No container image specified - will run directly")
            return None

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

    def stage_code(self, stage: str, grid: int = -1) -> str:
        """Generate unique stage identifier for job naming."""
        if isinstance(stage, str) and stage.startswith("stage") and not stage.endswith("_gridcombine"):
            # stage_number = int(stage.replace("stage", ""))
            if grid > 0:
                return f"p{self.get_stage_number(stage)}-x{grid}"
            else:
                return f"p{self.get_stage_number(stage)}"
        else:
            return f"{stage}"

    def job_code(self, stage: str, grid: int = -1, seed: int = 1) -> str:
        """Generate unique job identifier."""
        return f"{self.stage_code(stage, grid)}-s{seed}"

    def get_job_time(self) -> str:
        """
        Get the maximum runtime for the job in SLURM format (HH:MM:SS).
        Currently hardcoded to 24 hours, but can be made configurable.
        """

        max_runtime = 24
        # Calculate time in SLURM format (HH:MM:SS) - max 23:59:59
        total_seconds = min(int(max_runtime * 3600), 86399)  # Cap at 23:59:59
        hours    = total_seconds // 3600
        minutes  = (total_seconds % 3600) // 60
        seconds  = total_seconds % 60
        job_time = f"{hours}:{minutes:02d}:{seconds:02d}"

        return job_time


class POWHEGWrapper_template(b2luigi.WrapperTask):
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

    config = b2luigi.DictParameter(hashed=True,
                                   default={},
                                   description="POWHEG configuration dictionary"
                                  )

    @staticmethod
    def get_stage_str(stage_number: int) -> str:
        """Convert numeric stage number to string representation."""
        assert stage_number in [1, 2, 3, 4], "Stage number must be between 1 and 4"

        return f"stage{stage_number}"

    @staticmethod
    def get_stage_number(stage:str) -> int:
        """Return the numeric stage number for resource lookup."""
        if stage.startswith("stage") and not stage.endswith("_gridcombine"):
            return int(stage.replace("stage", ""))
        elif stage == "addweights":
            return 4
        elif stage == "analysis":
            return 1  # Analysis stage doesn't have a specific number
        elif stage == "stage3_gridcombine":
            return 3
        else:
            raise ValueError(f"Unknown stage: {stage}")

    def get_config(self) -> POWHEGConfig:
        """
        Convert the stored dictionary back to a POWHEGConfig instance.
        """
        if isinstance(self.config, POWHEGConfig):
            return self.config
        else:
            return POWHEGConfig(**self.config)
