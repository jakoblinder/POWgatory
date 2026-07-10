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

# Import validation function
from .config import POWHEGConfig
from . import __version__


# Available tasks
# FIXME: Import proper tasks.
TASK_MAP = {
    'POWHEGWorkflow':          'POWHEGWorkflow',
    'POWHEGStage1':            'POWHEGStage1',
    'POWHEGStage2':            'POWHEGStage2',
    'POWHEGStage3GridCombine': 'POWHEGStage3GridCombine',
    'POWHEGStage3':            'POWHEGStage3',
    'POWHEGStage4':            'POWHEGStage4',
    'POWHEGAnalysis':          'POWHEGAnalysis',
    'POWHEGAddWeights':        'POWHEGAddWeights',
}


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

# TODO: Try out how the scanning over different config files is working.

def run_workflow():
    """Main entry point for the CLI - parses arguments and runs the workflow."""
    import b2luigi
    from .tasks import POWHEGWorkflow

    # Parse custom arguments while ignoring b2luigi batch-runner arguments.
    parser = create_parser()

    # config_path = resolve_config_path(our_args)
    # if not config_path:
    #     config_path = 'config.yaml'

    args, b2luigi_args = parser.parse_known_args()
    # Include program name in b2luigi_args to mimic sys.argv structure
    b2luigi_args = [sys.argv[0], *b2luigi_args]

    config = POWHEGConfig.from_yaml(args.config_file)

    # Validate run directory
    config.validate_run_directory()

    if args.version:
        print(f"powheg-workflow version {__version__}")
        return 0
    else:
        print(f"Start powheg-workflow ({__version__})")

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


    # Configure b2luigi settings
    b2luigi.set_setting("result_dir", str(config["job_settings"]["run_dir"]))
    b2luigi.set_setting("env_script", str(config["job_settings"]["run_dir"] / "bootstrap.sh"))
    # Directory where slurm scripts will be stored.
    b2luigi.set_setting("task_file_dir", str(config["job_settings"]["run_dir"] / "task_files"))

    # Directory from which to run the workflow; this should be absolute to avoid issues with relative paths in batch systems
    b2luigi.set_setting("working_dir", str(config["cwd"]))

    b2luigi.set_setting("executable", ["/u/jlinder/utilities/powheg-luigi-workflow/.venv/bin/powheg-workflow"])  # Use our CLI as the executable for tasks
    b2luigi.set_setting("add_filename_to_cmd", False)
    # Give the config file to each task as an argument
    b2luigi.set_setting("task_cmd_additional_args", ["--config_file", str(config["config_file"])])  # No additional args for tasks

    # Update sys.argv for b2luigi, removing our custom arguments
    sys.argv = b2luigi_args

    program_version = __version__.replace('.', '-')  # Replace . with hyphen for environment variable compatibility
    # number of workers == number of parallel tasks to run.
    try:
        max_workers = config["cluster_config"]["slurm"]["max_parallel_jobs"]
    except KeyError:
        max_workers = config["cluster_config"]["local"]["max_parallel_jobs"]


    config_dict = config.to_dict()  # Convert to dictionary for serialization
    # Run tasks using b2luigi with ignore_additional_command_line_args=True
    b2luigi.process(POWHEGWorkflow(version=program_version, config=config_dict), workers=max_workers, ignore_additional_command_line_args=True, dry_run=args.dry_run)

    return 0