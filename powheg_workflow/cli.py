#!/usr/bin/env python3
"""
POWHEG-BOX b2luigi Workflow Manager

This CLI provides workflow orchestration for POWHEG using b2luigi.

Usage:
    powheg-workflow -c config.yaml                          # Run full workflow
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
        default=None,
        help='Path to config.yaml configuration file'
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


def set_b2luigi_settings(cwd: Path, run_dir: Path, config_file: Path):
    """
    Set b2luigi settings based on the configuration.

    Args:
        cwd (Path): Directory where the workflow is started.
        run_dir (Path): Directory where POWHEG run will run.
        config_file (Path): Path to the configuration file for this POWHEG run.
    """
    cwd, run_dir, config_file = Path(cwd), Path(run_dir), Path(config_file)
    b2luigi.set_setting("result_dir", str(run_dir))
    b2luigi.set_setting("env_script", str(run_dir / "bootstrap.sh"))
    # Directory where slurm scripts will be stored.
    b2luigi.set_setting("task_file_dir", str(run_dir / "task_files"))

    # Directory from which to run the workflow; this should be absolute to avoid issues with relative paths in batch systems
    b2luigi.set_setting("working_dir", str(cwd))

    # FIXME:
    b2luigi.set_setting("executable", ["/u/jlinder/utilities/powheg-luigi-workflow/.venv/bin/powheg-workflow"])  # Use our CLI as the executable for tasks
    b2luigi.set_setting("add_filename_to_cmd", False)
    # Give the config file to each task as an argument
    b2luigi.set_setting("task_cmd_additional_args", ["--config_file", str(config_file)])  # No additional args for tasks

# TODO: Try out how the scanning over different config files is working.

def run_workflow():
    """Main entry point for the CLI - parses arguments and runs the workflow."""
    from .tasks import POWHEGWorkflow

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


    config = POWHEGConfig.from_yaml(args.config_file)
    # Validate run directory
    config.validate_run_directory()

    if args.verbose:
        print(f"Configuration: {config['config_file']}")

    if args.dry_run:
        print(f"\nDry run - would execute:")
        print(f"  Config:   {config['config_file']}")
        print(f"  Cluster:  {config.get('cluster', 'mpi')}")
        print(f"\nEnabled stages:")
        for stage, settings in config.get('stages', {}).items():
            if isinstance(settings, dict) and settings.get('enabled', False):
                print(f"  - {stage}")
            elif settings is True:
                print(f"  - {stage}")
        # return 0
        b2luigi_args.append('--dry-run')  # Pass dry-run to b2luigi


    set_b2luigi_settings(cwd = config["cwd"],
                         run_dir = config["job_settings"]["run_dir"],
                         config_file = config["config_file"])

    # Update sys.argv for b2luigi, removing our custom arguments
    sys.argv = b2luigi_args


    # number of workers == number of parallel tasks to run.
    try:
        max_workers = config["cluster_config"]["slurm"]["max_parallel_jobs"]
    except KeyError:
        max_workers = config["cluster_config"]["local"]["max_parallel_jobs"]

    program_version = __version__.replace('.', '-')  # Replace . with hyphen for environment variable compatibility
    config_dict = config.to_dict()  # Convert to dictionary for serialization
    # Run tasks using b2luigi with ignore_additional_command_line_args=True
    b2luigi.process(POWHEGWorkflow(version=program_version, config=config_dict), workers=max_workers, ignore_additional_command_line_args=True, dry_run=args.dry_run)

    return 0