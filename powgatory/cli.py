#!/usr/bin/env python3
"""
POWHEG-BOX b2luigi Workflow Manager

This CLI provides workflow orchestration for POWHEG using b2luigi.

Usage:
    powgatory -c config.yaml                          # Run full workflow
    powgatory -c config1.yaml config2.yaml            # Run multiple workflows
    powgatory -c config.yaml --dry-run                # Show what would be done
    powgatory --init                                  # Write a run.yaml to start from
    powgatory --save-template my.yaml                 # Make my.yaml the template for --init
    powgatory --show-defaults                         # Print all options with their defaults
"""

import argparse
import fcntl
import os
import shutil
import sys
import threading
import time
from pathlib import Path

import b2luigi
import luigi.configuration
from b2luigi.batch.processes.slurm import SlurmProcess

from .config import POWHEGConfig, slurm_partitions
from . import __version__

# This block makes sure that jobs are scheduled with a 2 second wait.
# It monkey patches the b2luigi SlurmProcess class


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


def lock_run_directories(run_dirs):
    """
    Take an exclusive lock on every run directory, so that a second POWgatory on the same run
    directory stops instead of running every branch a second time next to the first one.

    The kernel releases the locks when this process ends, also after a crash. On GPFS (/ptmp) a
    lock holds across nodes. The returned lock files must stay referenced to keep the locks.
    """
    lock_files = []
    for run_dir in sorted({Path(run_dir).resolve() for run_dir in run_dirs}):
        lock_path = run_dir / ".powgatory.lock"
        lock_file = open(lock_path, "w")
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            sys.exit(f"Another POWgatory is already running in {run_dir} (it holds {lock_path}).")
        except OSError as e:
            print(f"Warning: could not lock {lock_path} ({e}); a second POWgatory in {run_dir} would not be noticed.")
        lock_files.append(lock_file)
    return lock_files


def free_cores() -> int:
    """
    The cores of this node that are free now and that this process may use, at least 1.

    The cores this process may use (a Slurm allocation, taskset, a cpuset) are capped by the node's
    cores minus its load, since other users' processes on a shared node don't show in the former.
    The 5-minute load average is used because the result is fixed for the whole run: a short spike
    at startup (a compile, a script) hardly shows in it, while load that has lasted a few minutes does.
    """
    if hasattr(os, "sched_getaffinity"):
        usable_cores = len(os.sched_getaffinity(0))
    else:
        usable_cores = os.cpu_count()  # macOS
    idle_cores = os.cpu_count() - round(os.getloadavg()[1])
    return max(1, min(usable_cores, idle_cores))


def local_parallel_tasks(configs) -> int:
    """
    How many local branches run at the same time. All configs share one scheduler and one node, so
    the smallest number any of them sets applies, and the free cores if all of them say auto.
    """
    numbers = [config["job_settings"]["local_parallel_tasks"] for config in configs
               if config["job_settings"]["local_parallel_tasks"] != "auto"]
    if numbers:
        return min(numbers)
    else:
        return free_cores()


def user_template_paths() -> list:
    """
    Where a user's own run.yaml template is looked up, in this order: $POWGATORY_TEMPLATE, if
    set, then ~/.config/powgatory/template.yaml.
    """
    paths = [Path.home() / ".config" / "powgatory" / "template.yaml"]
    if os.environ.get("POWGATORY_TEMPLATE"):
        paths.insert(0, Path(os.environ["POWGATORY_TEMPLATE"]).expanduser())
    return paths


def write_run_config(destination: Path):
    """Copy the user's template, or else the packaged example.yaml, to destination."""
    if destination.exists():
        sys.exit(f"{destination} already exists, not overwritten.")

    user_templates = [path for path in user_template_paths() if path.exists()]
    if user_templates:
        template = user_templates[0]
    else:
        template = Path(__file__).parent / "config" / "example.yaml"

    shutil.copyfile(template, destination)
    print(f"Wrote {destination} from {template}")


def save_template(source: Path):
    """Copy source to where write_run_config looks first, asking before overwriting a template."""
    if not source.exists():
        sys.exit(f"{source} does not exist.")

    template = user_template_paths()[0]
    if template.exists() and input(f"{template} already exists. Overwrite it? [y/N] ").strip().lower() != "y":
        sys.exit("Template not saved.")

    template.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, template)
    print(f"Saved {source} as the template for powgatory --init: {template}")


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
  powgatory --init                              Write run.yaml to start a new run from
  powgatory --save-template my.yaml             Use my.yaml as the template for --init
  powgatory --show-defaults                     Print all options with their defaults

--init copies the first template that exists: $POWGATORY_TEMPLATE,
~/.config/powgatory/template.yaml, or the packaged example.yaml.
--save-template writes to the first of the two user locations.
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
        '--init',
        nargs='?',
        const='run.yaml',
        metavar='FILE',
        help='Write a run config to start from into FILE (default: run.yaml) and exit'
    )

    parser.add_argument(
        '--save-template',
        metavar='FILE',
        help='Save FILE as your own template for --init and exit'
    )

    parser.add_argument(
        '--show-defaults',
        action='store_true',
        help='Print the default configuration, with every option and its explanation, and exit'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='Number of parallel b2luigi workers (concurrently submitted/running jobs); overrides job_settings.workers.'
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
    elif args.init:
        write_run_config(Path(args.init))
        return 0
    elif args.save_template:
        save_template(Path(args.save_template))
        return 0
    elif args.show_defaults:
        # The defaults that every run.yaml is merged over, with their comments.
        print((Path(__file__).parent / "config" / "config_default.yaml").read_text(), end="")
        return 0
    else:
        print(f"Start powgatory ({__version__})")

    config_files = [Path(config_file).resolve() for config_file in args.config_file]
    if not config_files:
        parser.error("the following arguments are required: -c/--config_file")

    configs = [POWHEGConfig.from_yaml(config_file) for config_file in config_files]
    for config in configs:
        config.validate_run_directory()

    # Batch jobs skip this, so that they don't query sinfo (the controller limits requests per user).
    if "--batch-runner" not in b2luigi_args:
        slurm_configs = [config for config in configs if config.slurm_stages()]
        warnings      = [warning for config in slurm_configs for warning in config.validate_partitions(slurm_partitions())]
        for warning in warnings:
            print(warning)

    # Batch jobs run this entry point too (with --batch-runner) and must not take the lock.
    if not args.dry_run and "--batch-runner" not in b2luigi_args:
        run_dir_locks = lock_run_directories(config["job_settings"]["run_dir"] for config in configs)

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
        print("\nDry run - would execute:")
        for config in configs:
            print(f"  Config:   {config['config_file']}")
            print("  Enabled stages:")
            for stage, settings in config.get('stages', {}).items():
                if isinstance(settings, dict) and settings.get('enabled', False):
                    resources = settings['resources']
                    print(f"    - {stage} (batch_system={resources['batch_system']}, submission_type={resources['submission_type']})")
                elif settings is True:
                    print(f"    - {stage}")
            print()
        b2luigi_args.append('--dry-run')  # Pass dry-run to b2luigi

    # The configs share one b2luigi worker, so the smallest number applies.
    if args.workers is not None:
        workers = args.workers
    else:
        workers = min(config["job_settings"]["workers"] for config in configs)

    # Each local branch takes one of the local_cores (POWHEGStage.resources); luigi's scheduler
    # starts a local branch only while one is free. The number is kept out of the config dict,
    # whose hash is part of every task ID, because it differs from node to node.
    if "--batch-runner" not in b2luigi_args:
        local_cores = local_parallel_tasks(configs)
        print(f"Local branches: at most {local_cores} at a time, workers: {workers}")
        luigi.configuration.get_config().set("resources", "local_cores", str(local_cores))

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
    b2luigi.process(workflow, workers=workers, ignore_additional_command_line_args=True, dry_run=args.dry_run)

    return 0
