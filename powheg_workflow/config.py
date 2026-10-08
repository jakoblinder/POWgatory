#!/usr/bin/env python3
"""
POWHEG-BOX LAW Workflow configuration file manager

"""
from pathlib import Path
import os
import yaml
import sys
import subprocess
from typing import Dict, Any, List, Optional

class POWHEGConfig:
    """
    Configuration manager that loads run.yaml and queries live per-partition Slurm info.
    """

    def __init__(self, **kwargs):
        """
         Args:

        """
        # Merge provided kwargs onto the defaults, recursively, so that a nested override
        # (e.g. a "resources" block missing a key like "max_grouping_size") doesn't wipe out
        # the sibling defaults for that same section.
        self.config = POWHEGConfig._deep_merge(POWHEGConfig.get_defaults(), kwargs)

        if "cwd" not in self.config:
            # Put the directory where the workflow is run into the config, so that tasks can use it.
            # Do it only ones so that if its part of the config file, it is not overwritten.
            self.config["cwd"] = Path.cwd()

        if "python" not in self.config:
            # Put the path to the python executable into the config, so that tasks can use it.
            # Do it only ones so that if its part of the config file, it is not overwritten.
            self.config["python"] = Path(sys.executable)


        # Make sure powheg_executable, powheg_input_template, and powheg_seeds_template are absolute paths
        for key in ["powheg_executable", "powheg_input_template", "powheg_seeds_template", "script_dir", "config_dir", "cwd"]:
            if key in self.config:
                self.config[key] = Path(self.config[key]).resolve()

        # Output directories
        for key in ["run_dir", "log_dir"]:
            if key in self.config["job_settings"]:
                self.config["job_settings"][key] = Path(self.config["job_settings"][key]).resolve()

        # Create directories
        self["job_settings"]["run_dir"].mkdir(exist_ok=True)
        self["job_settings"]["log_dir"].mkdir(exist_ok=True)

        # Complete resources and powheg_parameters information for each stage,
        # so that each staage has its own resources and powheg_parameters,
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

        # Cluster configuration is queried live from Slurm rather than hand-maintained, since a
        # stage's partition is now resolved above. Skip the query entirely for fully-local runs,
        # where sinfo isn't available.
        requires_slurm = any(
            stage_config.get("enabled", False) and stage_config["resources"]["batch_system"] == "slurm"
            for stage_config in self["stages"].values()
        )

        self["cluster_config"] = {"slurm": {}, "local": {}}
        if requires_slurm:
            # Query available partition info from sinfo (hardcoded for now; to be made configurable later)
            self.get_partition_info()

        if not self["job_settings"]["exclude_nodes"]:
            self["job_settings"]["exclude_nodes"] = []

        for warning in self.validate_workflow_structure():
            print(warning)

        self.setup_environment()

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
        package_path = Path(__file__).parent.parent.resolve()
        default_config_file = package_path / "config/config_default.yaml"
        with open(default_config_file, 'r') as f:
            config_dict = yaml.safe_load(f)
        config_dict['config_file'] = default_config_file

        # Load default pwgseeds.dat-save from workflow config directory
        pwgseeds_default = package_path / "config/pwgseeds.dat-save"
        config_dict['powheg_seeds_template'] = pwgseeds_default.resolve()

        config_dict['script_dir'] = package_path / "scripts"
        config_dict['config_dir'] = package_path / "config"

        if config_dict["powheg_parameters"] == None:
            config_dict["powheg_parameters"] = {}
        if config_dict["resources"] == None:
            config_dict["resources"] = {}

        return config_dict

    def get_partition_info(self) -> None:
        """
        Query `sinfo` for the number of nodes, CPUs, memory, and time limit available per
        partition, and store the result in cluster_config["slurm"].
        # TODO: Make this configurable (e.g. skip the query if the config file already
        # provides this info, or let it be overridden from there).
        """
        output = subprocess.check_output(["sinfo", "--noheader", "-o", "%P %D %c %z %m %l"]).decode()

        partition_info: Dict[str, Dict[str, Any]] = {}
        for line in output.strip().splitlines():
            partition, nodes, cpus, sct, memory, time_limit = line.split()
            partition = partition.rstrip("*")  # sinfo marks the default partition with "*"
            cpus = cpus.rstrip("+")  # sinfo marks a lower-bound value with "+" when grouped nodes' CPU counts differ
                                     # Note that this is a delicate situation and can cause problems.
            entry = partition_info.setdefault(
                partition,
                {"nodes": 0, "cpus": int(cpus), "sct": sct, "memory": memory, "time_limit": time_limit},
            )
            entry["nodes"] += int(nodes)  # sinfo lists one row per node state within a partition

        self["cluster_config"]["slurm"] = partition_info

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

    def setup_environment(self):
        """Configure environment variables for LAW tasks."""
        os.environ['POWHEG_RUN_DIR']     = str(self['job_settings']['run_dir'])
        os.environ['POWHEG_LOG_DIR']     = str(self['job_settings']['log_dir'])

        # Copy bootstrap.sh to run directory and save its path in self.bootstrap_script_path
        bootstrap_script_path = self['job_settings']['run_dir'] / "bootstrap.sh"
        # Adjust paths in the bootstrap script to point to the correct output and log directories
        with open(self['config_dir'] / "bootstrap.sh", 'r') as f:
            content = f.read()
            content = content.replace("{{POWHEG_RUN_DIR}}", str(self['job_settings']['run_dir']))
            content = content.replace("{{POWHEG_LOG_DIR}}", str(self['job_settings']['log_dir']))

        with open(bootstrap_script_path, 'w') as f:
            f.write(content)

    @classmethod
    def from_yaml(cls, config_file: str) -> 'POWHEGConfig':
        """Create config instance from YAML file."""
        config_file = Path(config_file).resolve()
        try:
            with open(config_file, 'r') as f:
                config_dict = yaml.safe_load(f)
            config_dict['config_file'] = str(config_file)

            if config_dict["powheg_parameters"] == None:
                config_dict["powheg_parameters"] = {}
            if config_dict["resources"] == None:
                config_dict["resources"] = {}

            return cls(**config_dict)

        except yaml.YAMLError as e:
            print(f"ERROR: Failed to parse {config_file}: {e}")
            sys.exit(1)

    def to_dict(self):
        """Return the configuration dictionary."""
        output_dict = self.config.copy()
        # Convert Path objects to strings for serialization
        for key in ["powheg_executable", "powheg_input_template", "powheg_seeds_template", "script_dir", "config_dir", "cwd", "python"]:
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
        - Stage 2 → Stage 3 (or Stage 3 GridCombine)
        - Stage 3/GridCombine → Stage 4
        - Stage 4 → Analysis/AddWeights (optional)

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

        # Check Stage 3 GridCombine requires Stage 2
        if stages.get('stage3', {}).get('grid_combination', False) and not is_enabled('stage2'):
            warnings.append("WARNING: Stage 3 grid combination is enabled but Stage 2 is disabled")

        # Check Stage 4 requires Stage 3 (or GridCombine)
        if is_enabled('stage4'):
            has_stage3 = is_enabled('stage3')
            has_gridcombine = stages.get('stage3', {}).get('grid_combination', False)
            if not (has_stage3 or has_gridcombine):
                warnings.append("WARNING: Stage 4 is enabled but Stage 3 is disabled and grid_combination is not enabled")

        # Check Analysis requires Stage 4
        if stages.get('analysis', {}).get('enabled', False) and not is_enabled('stage4'):
            warnings.append("WARNING: Analysis is enabled but Stage 4 is disabled")

        # Check AddWeights requires Stage 4
        if stages.get('addweights', {}).get('enabled', False) and not is_enabled('stage4'):
            warnings.append("WARNING: AddWeights is enabled but Stage 4 is disabled")

        # Check Stage 1 grid iterations configuration
        stage1_config = stages.get('stage1', {})
        if isinstance(stage1_config, dict) and stage1_config.get('enabled', False):
            grid_iters = stage1_config.get('grid_iterations', 3)
            if grid_iters < 1:
                warnings.append(f"WARNING: Stage 1 has {grid_iters} grid iterations (should be >= 1)")

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

            if stage_config['resources']['batch_system'] == 'slurm':
                partition = stage_config['resources']['partition']
                if partition not in self['cluster_config']['slurm']:
                    available = ", ".join(sorted(self['cluster_config']['slurm']))
                    raise ValueError(
                        f"ERROR: Stage {stage_name} uses partition '{partition}', which sinfo does not "
                        f"report on this cluster. Available partitions: {available}"
                    )
                partition_time_limit = self['cluster_config']['slurm'][partition]['time_limit']
                partition_seconds = self._parse_slurm_time(partition_time_limit)

                if partition_seconds is not None and total_seconds is not None and total_seconds > partition_seconds:
                    warnings.append(
                        f"WARNING: Stage {stage_name} time limit {time_limit} exceeds partition "
                        f"'{partition}' limit {partition_time_limit}"
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
        # It None is given, the one taken within the powheg_workflow package will be used.
        # This error is only raised if a user explicitly sets a seeds template that does not exist.
        if not self['powheg_seeds_template'].exists():
            raise FileNotFoundError(f"ERROR: pwgseeds.dat-save not found: {self['powheg_seeds_template']}")

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

