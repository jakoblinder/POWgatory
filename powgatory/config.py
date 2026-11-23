#!/usr/bin/env python3
"""
POWHEG-BOX b2luigi workflow configuration manager

"""
from pathlib import Path
import functools
import shutil
import yaml
import sys
import subprocess
from typing import Dict, Any, List, Optional

from .merge import METHOD_NAMES
from .grid_contributions import grid_contributions

# Stages that analyse the stage-4 event files, each with its own <stage>_executable.
ANALYSIS_STAGES = ("lhef", "pythia")

# merge_method of a stage whose histogram files are not merged.
NO_MERGE = -1

# The Slurm queries below are cached, so that a POWgatory with many configs sends each of them
# once: the Slurm controller limits the requests per user (rl_enable in SlurmctldParameters), and
# hundreds of them in a row make the commands fail.


@functools.cache
def slurm_partitions() -> Dict[str, Dict[str, Any]]:
    """
    The number of nodes, the CPUs and memory per node, and the time limit of every partition, from
    sinfo. It is not stored in the config: batch jobs rebuild the config to find their task ID and
    would otherwise each have to query sinfo again, and get exactly the same answer.
    """
    output = subprocess.check_output(["sinfo", "--noheader", "-o", "%P %D %c %z %m %l"]).decode()

    partitions: Dict[str, Dict[str, Any]] = {}
    for line in output.strip().splitlines():
        partition, nodes, cpus, sct, memory, time_limit = line.split()
        partition = partition.rstrip("*")  # sinfo marks the default partition with "*"
        cpus = cpus.rstrip("+")  # sinfo marks a lower-bound value with "+" when grouped nodes' CPU counts differ
                                 # Note that this is a delicate situation and can cause problems.
        entry = partitions.setdefault(
            partition,
            {"nodes": 0, "cpus": int(cpus), "sct": sct, "memory": memory, "time_limit": time_limit},
        )
        entry["nodes"] += int(nodes)  # sinfo lists one row per node state within a partition

    return partitions


@functools.cache
def slurm_max_array_size() -> int:
    """
    Slurm's MaxArraySize from `scontrol show config`, or 1 where there is no Slurm (local branches
    are never grouped). Job array indices run from 0 to MaxArraySize - 1, and b2luigi numbers the
    elements from 0, so a job array can have MaxArraySize elements.
    """
    if shutil.which("scontrol") is None:
        return 1
    else:
        output   = subprocess.check_output(["scontrol", "show", "config"]).decode()
        settings = {key.strip(): value.strip() for key, _, value in (line.partition("=") for line in output.splitlines())}
        max_array_size = int(settings["MaxArraySize"])
        print(f"max_grouping_size: auto -> {max_array_size} (Slurm's MaxArraySize)")
        return max_array_size


class POWHEGConfig:
    """
    Configuration manager that loads run.yaml and queries live per-partition Slurm info.
    """

    def __init__(self, **kwargs):
        """
        Args:
            **kwargs: Configuration entries, usually the parsed run.yaml (see from_yaml).
                      They are merged onto the defaults from config/config_default.yaml.
        """
        # Merge provided kwargs onto the defaults, recursively, so that a nested override
        # (e.g. a "resources" block missing a key like "max_grouping_size") doesn't wipe out
        # the sibling defaults for that same section.
        self.config = POWHEGConfig._deep_merge(POWHEGConfig.get_defaults(), kwargs)

        if "cwd" not in self.config:
            # Put the directory where the workflow is run into the config, so that tasks can use it.
            # Do it only once, so that it is not overwritten if it is part of the config file.
            self.config["cwd"] = Path.cwd()

        if "python" not in self.config:
            # Put the path to the python executable into the config, so that tasks can use it.
            # Do it only once, so that it is not overwritten if it is part of the config file.
            self.config["python"] = Path(sys.executable)


        # Make sure the executables, powheg_input_template, and powheg_seeds_template are absolute paths
        for key in ["powheg_executable", "lhef_executable", "pythia_executable", "powheg_input_template", "powheg_seeds_template", "script_dir", "cwd"]:
            if key in self.config:
                self.config[key] = Path(self.config[key]).resolve()

        # Output directories
        for key in ["run_dir", "log_dir"]:
            if key in self.config["job_settings"]:
                self.config["job_settings"][key] = Path(self.config["job_settings"][key]).resolve()

        # Create directories
        self["job_settings"]["run_dir"].mkdir(exist_ok=True)
        self["job_settings"]["log_dir"].mkdir(exist_ok=True)

        # The grid combination runs stage-3 seeds, which all have to run with the same input.
        if "powheg_parameters" in self["stages"]["stage3_fullgrid"]:
            raise ValueError(
                "ERROR: stage3_fullgrid runs stage-3 seeds with stage3's powheg_parameters. "
                "Set them under stages.stage3.powheg_parameters instead."
            )

        # Complete resources and powheg_parameters information for each stage,
        # so that each stage has its own resources and powheg_parameters,
        # either from the stage-specific config or from the global defaults.
        for stage in self["stages"].keys():
            if "resources" not in self["stages"][stage]:
                self["stages"][stage]["resources"] = self["resources"]
            else:
                for resource in self["resources"]:
                    if resource not in self["stages"][stage]["resources"]:
                        self["stages"][stage]["resources"][resource] = self["resources"][resource]
            if "powheg_parameters" not in self["stages"][stage]:
                self["stages"][stage]["powheg_parameters"] = self["powheg_parameters"]
            else:
                for param in self["powheg_parameters"]:
                    if param not in self["stages"][stage]["powheg_parameters"]:
                        self["stages"][stage]["powheg_parameters"][param] = self["powheg_parameters"][param]

        # The grid combination runs the first stage-3 seeds, one per contribution whose grids
        # POWHEG combines (see tasks.grid_combination_seeds). The count is read from the files,
        # so that batch jobs get the same config and task IDs.
        grid_combination = self["stages"]["stage3_fullgrid"]
        grid_combination["powheg_parameters"] = self["stages"]["stage3"]["powheg_parameters"]
        if grid_combination["enabled"] and grid_combination["contributions"] == "auto":
            contributions = grid_contributions(self["powheg_input_template"], grid_combination["powheg_parameters"])
            grid_combination["contributions"] = len(contributions)
            print(f"stage3_fullgrid: the grids of {', '.join(contributions) or 'no contribution'} are combined, "
                  f"according to {self['powheg_input_template']} and stage3's powheg_parameters.")

        exclude_nodes = self["job_settings"]["exclude_nodes"] or []
        if isinstance(exclude_nodes, str):
            exclude_nodes = [node.strip() for node in exclude_nodes.split(",")]
        self["job_settings"]["exclude_nodes"] = exclude_nodes

        for warning in self.validate_workflow_structure():
            print(warning)

        print(f"Configuration loaded from {self['config_file']}:")
        for key, value in self.items():
            print(f"  {key}: {value}")

    @staticmethod
    def _deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge `overrides` onto `base`, without dropping base keys that `overrides` doesn't set."""
        merged = dict(base)
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = POWHEGConfig._deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged

    @staticmethod
    def get_defaults():
        """Set default values for missing configuration keys."""
        # config/ and scripts/ ship inside the package, so they are found the same way
        # in a regular and in an editable install.
        package_path = Path(__file__).parent.resolve()
        default_config_file = package_path / "config/config_default.yaml"
        with open(default_config_file, 'r') as f:
            config_dict = yaml.safe_load(f)
        config_dict['config_file'] = default_config_file

        # Load default pwgseeds.dat-save from workflow config directory
        pwgseeds_default = package_path / "config/pwgseeds.dat-save"
        config_dict['powheg_seeds_template'] = pwgseeds_default.resolve()

        config_dict['script_dir'] = package_path / "scripts"

        if config_dict["powheg_parameters"] == None:
            config_dict["powheg_parameters"] = {}
        if config_dict["resources"] == None:
            config_dict["resources"] = {}

        return config_dict

    @staticmethod
    def _parse_slurm_time(time_str: str) -> Optional[int]:
        """
        Parse a Slurm-style time limit into a number of seconds, or None if unlimited.

        Accepts every format Slurm uses (e.g. sinfo prints "30:00" for a 30 minute limit):
        "MM", "MM:SS", "HH:MM:SS", "D-HH", "D-HH:MM", "D-HH:MM:SS", and "UNLIMITED"/"infinite".
        Raises ValueError for anything else.
        """
        time_str = time_str.strip()
        if time_str.lower() in ("unlimited", "infinite", "infinite*"):
            return None

        days = 0
        if "-" in time_str:
            days_str, time_str = time_str.split("-")
            days = int(days_str)
            # With a day count, the fields are hours[:minutes[:seconds]].
            fields = [int(field) for field in time_str.split(":")]
            if not 1 <= len(fields) <= 3:
                raise ValueError(f"Invalid Slurm time format: {time_str}")
            h, m, s = (fields + [0, 0])[:3]
        else:
            # Without a day count, the fields are [[hours:]minutes:]seconds, except that a
            # single number means minutes.
            fields = [int(field) for field in time_str.split(":")]
            if len(fields) == 1:
                h, m, s = 0, fields[0], 0
            elif len(fields) == 2:
                h, (m, s) = 0, fields
            elif len(fields) == 3:
                h, m, s = fields
            else:
                raise ValueError(f"Invalid Slurm time format: {time_str}")

        return days * 86400 + h * 3600 + m * 60 + s

    @classmethod
    def from_yaml(cls, config_file: str) -> 'POWHEGConfig':
        """Create config instance from YAML file."""
        config_file = Path(config_file).resolve()
        try:
            with open(config_file, 'r') as f:
                config_dict = yaml.safe_load(f)
            config_dict['config_file'] = str(config_file)

            # A block that is missing, or present with every entry commented out (parsed as None),
            # falls back to the defaults via the deep merge in __init__.
            for key in ["powheg_parameters", "resources"]:
                if config_dict.get(key) is None:
                    config_dict[key] = {}

            return cls(**config_dict)

        except yaml.YAMLError as e:
            print(f"ERROR: Failed to parse {config_file}: {e}")
            sys.exit(1)

    def to_dict(self):
        """Return the configuration dictionary."""
        output_dict = self.config.copy()
        # Convert Path objects to strings for serialization
        for key in ["powheg_executable", "lhef_executable", "pythia_executable", "powheg_input_template", "powheg_seeds_template", "script_dir", "cwd", "python"]:
            if key in output_dict:
                output_dict[key] = str(output_dict[key])
        for key in ["run_dir", "log_dir"]:
            if key in output_dict.get("job_settings", {}):
                output_dict["job_settings"][key] = str(output_dict["job_settings"][key])
        return output_dict

    def validate_workflow_structure(self):
        """
        Validate that enabled stages are consistent with their dependencies.

        Stage dependencies (noting that Stage 1 has iterations):
        - Stage 1 (all iterations) → Stage 2
        - Stage 2 → Stage 3 grid combination (stage3_fullgrid, optional) → Stage 3
        - Stage 3 → Stage 4
        - Stage 4 → lhef/pythia/AddWeights (optional)

        Args:
            config: POWHEGConfig instance

        Returns:
            List of warning messages (empty if all valid)
        """
        stages = self['stages']
        warnings = []

        # Helper to check if stage is enabled
        def is_enabled(stage_name):
            stage_config = stages.get(stage_name, {})
            if isinstance(stage_config, dict):
                return stage_config.get('enabled', False)
            return stage_config is True

        # Check Stage 2 requires Stage 1
        if is_enabled('stage2') and not is_enabled('stage1'):
            warnings.append("WARNING: Stage 2 is enabled but Stage 1 is disabled")

        # Check Stage 3 requires Stage 2
        if is_enabled('stage3') and not is_enabled('stage2'):
            warnings.append("WARNING: Stage 3 is enabled but Stage 2 is disabled")

        # Check the Stage 3 grid combination requires Stage 2
        if is_enabled('stage3_fullgrid') and not is_enabled('stage2'):
            warnings.append("WARNING: Stage stage3_fullgrid is enabled but Stage 2 is disabled")

        # The grid combination runs the first seeds of Stage 3, which needs seeds of its own: its
        # timings task fails without any.
        if is_enabled('stage3_fullgrid'):
            contributions = stages['stage3_fullgrid']['contributions']
            stage3_ntasks = stages['stage3']['resources']['ntasks']
            if contributions not in (1, 2, 3):
                raise ValueError(f"ERROR: Stage stage3_fullgrid has {contributions!r} contributions, expected 1, 2 or 3 (or auto).")
            elif stage3_ntasks <= contributions:
                raise ValueError(
                    f"ERROR: Stage stage3_fullgrid runs the first {contributions} seeds of Stage 3, which has only "
                    f"{stage3_ntasks} (ntasks). Give Stage 3 more seeds, or disable stage3_fullgrid."
                )

        # Check Stage 4 requires Stage 3
        if is_enabled('stage4') and not is_enabled('stage3'):
            warnings.append("WARNING: Stage 4 is enabled but Stage 3 is disabled")

        # Check the analysis stages require Stage 4
        for analysis_stage in ANALYSIS_STAGES:
            if is_enabled(analysis_stage) and not is_enabled('stage4'):
                warnings.append(f"WARNING: Stage {analysis_stage} is enabled but Stage 4 is disabled")

        # Check AddWeights requires Stage 4
        if stages.get('addweights', {}).get('enabled', False) and not is_enabled('stage4'):
            warnings.append("WARNING: AddWeights is enabled but Stage 4 is disabled")

        # Check Stage 1 grid iterations configuration
        stage1_config = stages.get('stage1', {})
        if isinstance(stage1_config, dict) and stage1_config.get('enabled', False):
            grid_iters = stage1_config.get('grid_iterations', 3)
            if grid_iters < 1:
                warnings.append(f"WARNING: Stage 1 has {grid_iters} grid iterations (should be >= 1)")

        # The b2luigi workers, and the local branches running at the same time (see cli.py)
        workers = self['job_settings']['workers']
        if not isinstance(workers, int) or workers < 1:
            raise ValueError(f"ERROR: job_settings.workers is {workers!r}, expected a positive number.")

        local_parallel_tasks = self['job_settings']['local_parallel_tasks']
        if local_parallel_tasks != "auto" and (not isinstance(local_parallel_tasks, int) or local_parallel_tasks < 1):
            raise ValueError(f"ERROR: job_settings.local_parallel_tasks is {local_parallel_tasks!r}, expected a positive number or auto.")

        # max_grouping_size: auto becomes Slurm's MaxArraySize (POWHEGStage.max_grouping_size), but an
        # MPI job runs all seeds of its group at the same time, so its size has to be chosen.
        mpi_stages = [
            stage_name for stage_name, stage_config in stages.items()
            if stage_config['enabled']
            and stage_config['resources']['batch_system'] == 'slurm'
            and stage_config['resources']['submission_type'] == 'mpi'
            and stage_config['resources']['max_grouping_size'] == 'auto'
        ]
        if mpi_stages:
            raise ValueError(
                f"ERROR: Stage {', '.join(mpi_stages)} has submission_type mpi and max_grouping_size auto, which "
                f"would put up to Slurm's MaxArraySize seeds into one MPI job. Set max_grouping_size to a number."
            )

        # A user without privileges can only raise the niceness.
        local_niceness = self['job_settings']['local_niceness']
        if not isinstance(local_niceness, int) or not 0 <= local_niceness <= 19:
            raise ValueError(f"ERROR: job_settings.local_niceness is {local_niceness!r}, expected a number from 0 to 19.")

        # Make sure the time limits are valid for each stage
        for stage_name, stage_config in stages.items():
            if not stage_config['enabled']:
                continue

            time_limit = stage_config['resources']['time']
            try:
                total_seconds = self._parse_slurm_time(time_limit)
            except ValueError:
                warnings.append(f"WARNING: Stage {stage_name} has invalid time limit format: {time_limit} (expected HH:MM:SS)")
                continue

            if total_seconds is not None and total_seconds <= 0:
                warnings.append(f"WARNING: Stage {stage_name} has non-positive time limit: {time_limit}")

        # An MPI job requests mem times its ranks per node (POWHEGStage.slurm_settings).
        for stage_name in self.slurm_stages():
            resources = stages[stage_name]['resources']
            if resources['submission_type'] == 'mpi' and not isinstance(resources['mem'], int):
                raise ValueError(
                    f"ERROR: Stage {stage_name} has submission_type mpi and mem {resources['mem']!r}, expected "
                    f"a number of MB per seed, which is multiplied by the seeds per node."
                )

        return warnings

    def slurm_stages(self) -> List[str]:
        """The enabled stages that are submitted to Slurm."""
        return [
            stage_name for stage_name, stage_config in self['stages'].items()
            if stage_config['enabled'] and stage_config['resources']['batch_system'] == 'slurm'
        ]

    def validate_partitions(self, partitions: Dict[str, Dict[str, Any]]) -> List[str]:
        """
        Check every enabled Slurm stage against the partitions from sinfo (slurm_partitions()): the
        partition has to exist, and the time limit and, for mpi, the memory of a full node should fit.
        Only the process that starts the run calls this, so that batch jobs don't query sinfo.

        Returns:
            List of warning messages (empty if all valid)
        """
        warnings = []
        for stage_name in self.slurm_stages():
            resources = self['stages'][stage_name]['resources']
            partition = resources['partition']
            if partition not in partitions:
                raise ValueError(
                    f"ERROR: Stage {stage_name} uses partition '{partition}', which sinfo does not "
                    f"report on this cluster. Available partitions: {', '.join(sorted(partitions))}"
                )

            partition_info = partitions[partition]
            try:
                stage_seconds = self._parse_slurm_time(resources['time'])
            except ValueError:
                stage_seconds = None  # validate_workflow_structure warns about the format
            partition_seconds = self._parse_slurm_time(partition_info['time_limit'])
            if stage_seconds is not None and partition_seconds is not None and stage_seconds > partition_seconds:
                warnings.append(
                    f"WARNING: Stage {stage_name} time limit {resources['time']} exceeds partition "
                    f"'{partition}' limit {partition_info['time_limit']}"
                )

            if resources['submission_type'] == 'mpi':
                # sinfo marks a lower bound with "+" when the nodes of a partition differ.
                node_memory      = int(partition_info['memory'].rstrip('+'))
                full_node_memory = resources['mem'] * partition_info['cpus']
                if full_node_memory > node_memory:
                    warnings.append(
                        f"WARNING: Stage {stage_name} (mpi) requests mem {resources['mem']} MB x {partition_info['cpus']} "
                        f"seeds = {full_node_memory} MB on a full node, but a node of partition '{partition}' has "
                        f"{node_memory} MB, so such a job never starts."
                    )

        return warnings

    def validate_run_directory(self):
        """
        Validate that run directory has necessary files.
        """
        # Check for POWHEG executable
        if not self['powheg_executable'].exists():
            raise FileNotFoundError(f"ERROR: POWHEG executable not found: {self['powheg_executable']}")

        # Check for input template
        if not self['powheg_input_template'].exists():
            raise FileNotFoundError(f"ERROR: POWHEG input template not found: {self['powheg_input_template']}")

        # Check for seeds file template.
        # It None is given, the one taken within the powgatory package will be used.
        # This error is only raised if a user explicitly sets a seeds template that does not exist.
        if not self['powheg_seeds_template'].exists():
            raise FileNotFoundError(f"ERROR: pwgseeds.dat-save not found: {self['powheg_seeds_template']}")

        # Check for the executables of the enabled analysis stages
        for analysis_stage in ANALYSIS_STAGES:
            executable = self[f'{analysis_stage}_executable']
            if self['stages'][analysis_stage]['enabled'] and not executable.exists():
                raise FileNotFoundError(f"ERROR: Executable of stage {analysis_stage} not found: {executable}")

        # Check the merge methods of the stages whose histogram files are merged
        for merged_stage in ("stage2",) + ANALYSIS_STAGES:
            merge_method = self['stages'][merged_stage]['merge_method']
            if merge_method != NO_MERGE and merge_method not in METHOD_NAMES:
                raise ValueError(f"ERROR: Stage {merged_stage} has merge_method {merge_method}, expected one of {sorted(METHOD_NAMES)}, or {NO_MERGE} for no merge.")

    # Dict-like interface delegation to self.config
    def __getitem__(self, key):
        """Enable bracket access: config["key"] → config.config["key"]"""
        return self.config[key]

    def __setitem__(self, key, value):
        """Enable bracket assignment: config["key"] = value"""
        self.config[key] = value

    def __delitem__(self, key):
        """Enable bracket deletion: del config["key"]"""
        del self.config[key]

    def __contains__(self, key):
        """Enable membership test: "key" in config"""
        return key in self.config

    def __iter__(self):
        """Enable iteration: for key in config"""
        return iter(self.config)

    def __len__(self):
        """Enable len(): len(config)"""
        return len(self.config)

    def keys(self):
        """Get all keys: config.keys()"""
        return self.config.keys()

    def values(self):
        """Get all values: config.values()"""
        return self.config.values()

    def items(self):
        """Get all items: config.items()"""
        return self.config.items()

    def get(self, key, default=None):
        """Safe access with default: config.get("key", default)"""
        return self.config.get(key, default)

