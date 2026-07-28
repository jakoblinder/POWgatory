#!/usr/bin/env python3
"""
POWHEG-BOX b2luigi Workflow Manager

This CLI provides workflow orchestration for POWHEG using b2luigi.

Usage:
    powheg-workflow -c config.yaml                          # Run full workflow
    powheg-workflow -c config1.yaml config2.yaml            # Run multiple workflows
    powheg-workflow -c config.yaml --dry-run                # Show what would be done
"""

import argparse
import sys
import os
from pathlib import Path

import b2luigi

# Import validation function
from .config import POWHEGConfig
from . import __version__


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        prog='powheg-workflow',
        description='POWHEG-BOX Workflow Manager using b2luigi',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  powheg-workflow -c config.yaml                      Run full workflow
  powheg-workflow -c config.yaml --dry-run            Show what would be done

'''
    )

    parser.add_argument(
        '-c', '--config_file',
        metavar='config.yaml',
        nargs='+',
        default=[],
        help='Path to one or more config.yaml configuration files'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without executing'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )

    parser.add_argument(
        '--version',
        help='Output version',
        action='store_true'
    )

    return parser


def set_b2luigi_settings(cwd: Path, run_dir: Path, python: Path):
    """
    Set b2luigi settings based on the configuration.

    Args:
        cwd (Path): Directory where the workflow is started.
        run_dir (Path): Directory where POWHEG run will run.
    """
    cwd, run_dir, python = Path(cwd), Path(run_dir), Path(python)
    b2luigi.set_setting("result_dir", str(run_dir))
    b2luigi.set_setting("env_script", str(run_dir / "bootstrap.sh"))
    # Directory where slurm scripts will be stored.
    b2luigi.set_setting("task_file_dir", str(run_dir / "task_files"))

    # Directory from which to run the workflow; this should be absolute to avoid issues with relative paths in batch systems
    b2luigi.set_setting("working_dir", str(cwd))

    # Make sure the python executable is used for tasks which was used to run the workflow.
    # This is important if virtual environments in combination with batch jobs are used.
    # Alternatively the virtual environment could be activated in the bootstrap.sh script.
    b2luigi.set_setting("executable_prefix", [str(python),])

    this_file_path = Path(__file__).resolve()
    main_file      = this_file_path.parent.parent / "please_work.py"
    b2luigi.set_setting("executable", [str(main_file),])

    b2luigi.set_setting("add_filename_to_cmd", False)
    # Give the config file to each task as an argument (Done now in the task_cmd_additional_args property of POWHEGStage.)
    # b2luigi.set_setting("task_cmd_additional_args", ["--config_file", str(config_file)])  # No additional args for tasks


def run_workflow():
    """Main entry point for the CLI - parses arguments and runs the workflow."""
    from .tasks import POWHEGWorkflow, POWHEGWorkflow_multiple_configs

    # Parse custom arguments while ignoring b2luigi batch-runner arguments.
    parser = create_parser()
    # Parse known arguments and separate unknown ones for b2luigi
    args, b2luigi_args = parser.parse_known_args()
    # Include program name in b2luigi_args to mimic sys.argv structure
    b2luigi_args = [sys.argv[0], *b2luigi_args]

    if args.version:
        print(f"powheg-workflow version {__version__}")
        return 0
    else:
        print(f"Start powheg-workflow ({__version__})")

    config_files = [Path(config_file).resolve() for config_file in args.config_file]
    if not config_files:
        parser.error("the following arguments are required: -c/--config_file")

    configs = [POWHEGConfig.from_yaml(config_file) for config_file in config_files]
    for config in configs:
        config.validate_run_directory()

    primary_config   = configs[0]
    multiple_configs = len(configs) > 1

    if args.verbose:
        if multiple_configs:
            print("Configurations:")
            for config in configs:
                print(f"  - {config['config_file']}")
        else:
            print(f"Configuration: {primary_config['config_file']}")

    if args.dry_run:
        print(f"\nDry run - would execute:")
        for config in configs:
            print(f"  Config:   {config['config_file']}")
            print(f"  Cluster:  {config.get('cluster', 'mpi')}")
            print(f"  Enabled stages:")
            for stage, settings in config.get('stages', {}).items():
                if isinstance(settings, dict) and settings.get('enabled', False):
                    print(f"    - {stage}")
                elif settings is True:
                    print(f"    - {stage}")
            print()
        b2luigi_args.append('--dry-run')  # Pass dry-run to b2luigi

    # Update sys.argv for b2luigi, removing our custom arguments
    sys.argv = b2luigi_args


    # number of workers == number of parallel tasks to run.
    # TODO: Make this a command line argument again with different default values depending on wether slurm or local is used.
    def get_max_workers(config):
        try:
            return config["cluster_config"]["slurm"]["max_parallel_jobs"]
        except KeyError:
            return config["cluster_config"]["local"]["max_parallel_jobs"]

    max_workers = max(get_max_workers(config) for config in configs)

    program_version = __version__.replace('.', '-')  # Replace . with hyphen for environment variable compatibility
    config_dict     = primary_config.to_dict()  # Convert to dictionary for serialization

    if multiple_configs:
        workflow = POWHEGWorkflow_multiple_configs(
                        version             = program_version,
                        configuration_files = [str(config_file) for config_file in config_files],
                    )

    else:
        set_b2luigi_settings(cwd        = primary_config["cwd"],
                             run_dir    = primary_config["job_settings"]["run_dir"],
                             python     = primary_config["python"])
        workflow = POWHEGWorkflow(version=program_version, config=config_dict)

    # Run tasks using b2luigi with ignore_additional_command_line_args=True
    b2luigi.process(workflow, workers=max_workers, ignore_additional_command_line_args=True, dry_run=args.dry_run)

    return 0
