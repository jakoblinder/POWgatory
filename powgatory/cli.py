#!/usr/bin/env python3
"""
POWHEG-BOX b2luigi Workflow Manager

This CLI provides workflow orchestration for POWHEG using b2luigi.

Usage:
    powgatory -c config.yaml                          # Run full workflow
    powgatory -c config1.yaml config2.yaml            # Run multiple workflows
    powgatory -c config.yaml --dry-run                # Show what would be done
"""

import argparse
import sys
import os
from pathlib import Path

import b2luigi

# Import validation function
from .config import POWHEGConfig
from . import __version__

# This block makes sure that jobs are scheduled with a 2 second wait.
# It monkey patches the b2luigi SlurmProcess class
import threading, time
from b2luigi.batch.processes.slurm import SlurmProcess


_original_start_job = SlurmProcess.start_job

def _throttled_start_job(self):
    _throttled_start_job.__dict__["_submit_lock"]      = _throttled_start_job.__dict__.get("_submit_lock", threading.Lock())
    _throttled_start_job.__dict__["_last_submit_time"] = _throttled_start_job.__dict__.get("_last_submit_time", 0.0)
    _throttled_start_job.__dict__["_submit_count"]     = _throttled_start_job.__dict__.get("_submit_count", 0)
    _throttled_start_job.__dict__["_start_time"]       = _throttled_start_job.__dict__.get("_start_time", time.time())

    MIN_SBATCH_INTERVAL = 2.0  # May be increased/decreased in the future. 2s is probably too conservative
    with _throttled_start_job._submit_lock:
        now  = time.time()
        wait = MIN_SBATCH_INTERVAL - (now - _throttled_start_job._last_submit_time)

        if wait > 0:
            print(
                f"[submit throttle] Waiting {wait:.2f}s "
                f"(submission #{_throttled_start_job._submit_count + 1}, "
                f"{_throttled_start_job._submit_count / max(now - _throttled_start_job._start_time, 1e-6):.2f} jobs/s so far)"
            )
            time.sleep(wait)
        else:
            print(
                f"[submit throttle] wait < 0.00s "
                f"(submission #{_throttled_start_job._submit_count + 1}, "
                f"{_throttled_start_job._submit_count / max(now - _throttled_start_job._start_time, 1e-6):.2f} jobs/s so far)"
            )

        result = _original_start_job(self)

        _throttled_start_job._submit_count += 1
        _throttled_start_job._last_submit_time = time.time()

    return result


SlurmProcess.start_job = _throttled_start_job
# End of patch!

def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        prog='powgatory',
        description='POWHEG-BOX Workflow Manager using b2luigi',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  powgatory -c config.yaml                      Run full workflow
  powgatory -c config.yaml --dry-run            Show what would be done

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

    parser.add_argument(
        '--workers',
        type=int,
        default=250,
        help='Number of parallel b2luigi workers (concurrently submitted/running jobs) to use.'
    )

    return parser


def run_workflow():
    """Main entry point for the CLI - parses arguments and runs the workflow."""
    from .tasks import POWHEGWorkflow, POWHEGMultiConfigWorkflow

    # Parse custom arguments while ignoring b2luigi batch-runner arguments.
    parser = create_parser()
    # Parse known arguments and separate unknown ones for b2luigi
    args, b2luigi_args = parser.parse_known_args()
    # Include program name in b2luigi_args to mimic sys.argv structure
    b2luigi_args = [sys.argv[0], *b2luigi_args]

    if args.version:
        print(f"powgatory version {__version__}")
        return 0
    else:
        print(f"Start powgatory ({__version__})")

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
            print(f"  Enabled stages:")
            for stage, settings in config.get('stages', {}).items():
                if isinstance(settings, dict) and settings.get('enabled', False):
                    resources = settings['resources']
                    print(f"    - {stage} (batch_system={resources['batch_system']}, submission_type={resources['submission_type']})")
                elif settings is True:
                    print(f"    - {stage}")
            print()
        b2luigi_args.append('--dry-run')  # Pass dry-run to b2luigi

    # Update sys.argv for b2luigi, removing our custom arguments
    sys.argv = b2luigi_args


    program_version = __version__.replace('.', '-')  # Replace . with hyphen for environment variable compatibility
    config_dict     = primary_config.to_dict()  # Convert to dictionary for serialization

    if multiple_configs:
        workflow = POWHEGMultiConfigWorkflow(
                        version             = program_version,
                        configuration_files = [str(config_file) for config_file in config_files],
                    )

    else:
        workflow = POWHEGWorkflow(version=program_version, config=config_dict)

    # Run tasks using b2luigi with ignore_additional_command_line_args=True
    b2luigi.process(workflow, workers=args.workers, ignore_additional_command_line_args=True, dry_run=args.dry_run)

    return 0
