#!/usr/bin/env python3
"""
POWHEG-BOX LAW Workflow configuration file manager

"""
from pathlib import Path
import os
import yaml
import sys
import subprocess
from typing import Dict, Any, List

class POWHEGConfig:
    """
    Configuration manager that loads run.yaml and cluster configs.
    """

    def __init__(self, **kwargs):
        """
         Args:

        """
        self.config = POWHEGConfig.get_defaults()
        # Override defaults with provided kwargs
        self.config.update(kwargs)

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

        # Load cluster configuration
        cluster_name = self.get('cluster', 'mpi')
        cluster_config_path = self.config['config_dir'] / f"clusters/{cluster_name}.yaml"

        if cluster_config_path.exists():
            with open(cluster_config_path, 'r') as f:
                self["cluster_config"] = yaml.safe_load(f)
        else:
            # Default cluster config if not found
            # TODO: Update to use local cluster config from local.yaml.
            self["cluster_config"] = {
                'cluster_type': cluster_name,
                'tasks_per_node': 64,
                'slurm': {'default_time': '24:00:00'},
                'partition': 'alma',
            }

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

        if not self["job_settings"]["exclude_nodes"]:
            self["job_settings"]["exclude_nodes"] = []

        for warning in self.validate_workflow_structure():
            print(warning)

        self.setup_environment()

        print(f"Configuration loaded from {self['config_file']}:")
        for key, value in self.items():
            print(f"  {key}: {value}")

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
            if stage_config['enabled']:
                time_limit = stage_config['resources']['time']
                # Validate time limit format (HH:MM:SS)
                try:
                    if '-' in time_limit:
                        days, hms = time_limit.split('-')
                        h, m, s = map(int, hms.split(':'))
                        total_seconds = int(days) * 86400 + h * 3600 + m * 60 + s
                    else:
                        h, m, s = map(int, time_limit.split(':'))
                        total_seconds = h * 3600 + m * 60 + s
                    if total_seconds <= 0:
                        warnings.append(f"WARNING: Stage {stage_name} has non-positive time limit: {time_limit}")

                    # Make sure its smaller than the cluster default time limit
                    cluster_time_limit = self['cluster_config'].get('slurm', 'local')['time_limit']
                    # cluster_time_limit is None if there is not time limit.

                    if cluster_time_limit != None:
                        if '-' in time_limit:
                            days, hms = cluster_time_limit.split('-')
                            h, m, s = map(int, hms.split(':'))
                            cluster_total_seconds = int(days) * 86400 + h * 3600 + m * 60 + s
                        else:
                            h, m, s = map(int, cluster_time_limit.split(':'))
                            cluster_total_seconds = h * 3600 + m * 60 + s

                        if total_seconds > cluster_total_seconds:
                            raise ValueError(f"WARNING: Stage {stage_name} time limit {time_limit} exceeds cluster default {cluster_time_limit}")

                except ValueError:
                    warnings.append(f"WARNING: Stage {stage_name} has invalid time limit format: {time_limit} (expected HH:MM:SS)")

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

