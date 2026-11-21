#!/usr/bin/env python3
"""
b2luigi Task Definitions for POWHEG-BOX Workflow

This module implements the POWHEG workflow using b2luigi (BELLE2 Luigi):
- Workflow coordination via Luigi
- Local and batch execution support
- Container execution via b2luigi
"""

import b2luigi
import os
import re
import time
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, Any, List

import yaml

# Import our framework classes
from .framework import POWHEGBaseTask, POWHEGWrapperTask
from .config import POWHEGConfig


def seed_strings(seed: int) -> List[str]:
    """
    The seed as POWHEG writes it into file names: with four digits, or with five digits in runs
    with more than 9999 seeds.
    """
    return [f"{seed:04d}", f"{seed:05d}"]


class POWHEGPresubmit(POWHEGBaseTask):
    """
    Presubmit task to run a user-defined script before the workflow starts.
    This can be used for environment setup, data preparation, or any other pre-processing steps.

    It is run in the run_dir specified in the configuration and is executed before any other tasks in the workflow.
    """

    batch_system = "local"

    @property
    def stage_name(self) -> str:
        return "presubmit"

    def output(self):
        """Task completion marker."""
        return self.local_target("presubmit.done")

    def run(self):
        """Set up the container image in the run_dir, then execute the presubmit script if defined."""
        # Copied or pulled here, once, before any stage runs: the parallel jobs of a stage would
        # each copy a local image, and a job could start apptainer on another job's half-written
        # copy. POWHEGStage still calls setup_container, which then finds the image in place.
        self.setup_container()

        presubmit_script = self.config["job_settings"]["presubmit"]
        if presubmit_script:
            self.publish_message(f"Running presubmit script: {presubmit_script}")
            try:
                subprocess.run(
                    presubmit_script,
                    shell=True,
                    cwd=self.config['job_settings']['run_dir'],
                    check=True
                )

                # Mark complete
                with self.output().open('w') as f:
                    f.write(f"{self.stage_name}: Completed.\n")

            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"Presubmit script failed: {e}")
        else:
            self.publish_message("No presubmit script defined. Skipping.")
            # Mark complete
            with self.output().open('w') as f:
                f.write(f"{self.stage_name}: No presubmit script defined. Skipping..\n")

    def requires(self):
        # Depend on setup task for this grid iteration.
        yield POWHEGCreateSymlinks(
            stage          = self.stage,
            grid_iteration = self.grid_iteration,
            version        = self.version,
            config         = self.config
        )


class POWHEGStageSetup(POWHEGBaseTask):
    """
    Setup task for a stage (and grid iteration): creates its powheg.input file.
    All parallel tasks of that stage depend on this completing first.
    """
    batch_system = "local"

    @property
    def stage_name(self) -> str:
        if self.grid_iteration > 0:
            return f"{self.stage}-grid{self.grid_iteration}_setup"
        else:
            return f"{self.stage}_setup"

    def output(self):
        if self.grid_iteration > 0:
            input_file = self.local_target(f"p{self.stage_number}-x{self.grid_iteration}-powheg.input")
        else:
            input_file = self.local_target(f"p{self.stage_number}-powheg.input")

        return input_file

    def run(self):
        """Create the input file."""
        self.create_powheg_input(stage=self.stage, grid=self.grid_iteration)

    def create_powheg_input(self, stage:str, grid: int = 1):
        """
        Create POWHEG input file for a job.

        Args:
            stage (str): POWHEG stage number (stage1, stage2, stage3, stage4,
                                              stage3_gridcombine, analysis, addweights)
            grid (int): Grid iteration number (1, 2, 3, ...)
        Returns:
            Path to the created input file.
        """
        input_template = self.config['powheg_input_template']

        input_file = self.local_path(f"{self.stage_code(stage, grid)}-powheg.input")

        if grid <= 0 :
            grid = 1  # Default to 1 if not specified

        input_file = Path(input_file)

        powheg_params = {
            'parallelstage':  self.stage_number,
            'xgriditeration': grid,
        }

        # Update powheg_params with any additional parameters from the config
        powheg_params.update(self.config["stages"][stage]["powheg_parameters"])

        self.change_param(input_template, input_file, powheg_params)

        return input_file

    @staticmethod
    def change_param(src_file: Path, dest_file: Path, replacements: Dict[str, Any]):
        """Modify parameters in POWHEG input file."""
        new_lines = []
        found = {key: False for key in replacements.keys()}

        with open(src_file, "r") as file:
            for line in file:
                new_line = ""
                match = re.match(
                    r"(?P<name>\w+)(?P<space>\s+)(?P<value>[\d\.\-d]+)(\s*\!\s*(?P<comment>.+))?",
                    line
                )
                if match:
                    name = match.group("name")
                    value = match.group("value")
                    total_length = len(match.group("space")) + len(value)
                    comment = match.group("comment")

                    for param_name, param_value in replacements.items():
                        if name in [param_name, param_name[1:]] and not found[param_name]:
                            param_value = re.sub(
                                r"(?P<bdot>\d)+p(?P<adot>\d*)",
                                r"\g<bdot>.\g<adot>",
                                str(param_value)
                            )
                            new_line = f"{name}{param_value:>{total_length}}"
                            if comment:
                                new_line += f"  ! {comment}"
                            new_line += "\n"
                            found[param_name] = True

                if new_line:
                    new_lines.append(new_line)
                else:
                    new_lines.append(line)

            for param_name, was_found in found.items():
                if not was_found:
                    print(f"Warning: Parameter '{param_name}' not found in {src_file}. It is added at the end of the file.")
                    new_line = f"{param_name} {replacements[param_name]}  ! Added by POWHEG workflow\n"
                    new_lines.append(new_line)

        with open(dest_file, "w") as file:
            file.write("".join(new_lines))


class POWHEGCreateSymlinks(POWHEGBaseTask):
    """
    Create symlinks for POWHEG input and seeds.
    This task is used to ensure that the correct input files are linked for POWHEG execution.
    """

    batch_system = "local"

    @property
    def stage_name(self) -> str:
        if self.grid_iteration > 0:
            return f"{self.stage}-grid{self.grid_iteration}_symlink"
        else:
            return f"{self.stage}_symlink"

    def output(self):
        """Task completion marker."""
        return self.local_target(f"symlinks_{self.stage}_grid{self.grid_iteration}.done")

    def run(self):
        """Create symlinks for POWHEG input and seeds."""
        self.create_symlinks(stage=self.stage, grid=self.grid_iteration)

    def requires(self):
        """
        Depend on setup task for this grid iteration.
        AND
        Depend on previous stage for stage > 1, so that the correct input files are linked.
        """
        yield POWHEGStageSetup(
            stage          = self.stage,
            grid_iteration = self.grid_iteration,
            version        = self.version,
            config         = self.config
        )
        if self.stage_number == 1 and self.grid_iteration > 1:
            # Depend on previous grid iteration for Stage 1
            yield POWHEGStageTimings(stage=self.get_stage_str(1), grid_iteration=self.grid_iteration - 1, version=self.version, config=self.config)
        elif self.stage_number == 2:
            # Depend on Stage 1 for Stage 2
            stages_config       = self.config['stages']
            max_grid_iterations = stages_config['stage1'].get('grid_iterations', 3)
            yield POWHEGStageTimings(stage=self.get_stage_str(1), grid_iteration=max_grid_iterations, version=self.version, config=self.config)
        elif self.stage_number > 2:
            # Depend on previous stage for stage > 2
            yield POWHEGStageTimings(stage=self.get_stage_str(self.stage_number - 1), version=self.version, config=self.config)


    def create_symlinks(self, stage: str, grid: int = -1):
        """Create symlinks for POWHEG input and seeds."""

        if grid > 0:
            self.publish_message(f"Creating symlinks for {stage}, grid {grid}")
        else:
            self.publish_message(f"Creating symlinks for {stage}")

        input_file = Path(self.local_path(f"{self.stage_code(stage, grid)}-powheg.input"))

        self.publish_message(f"Creating symlink for powheg.input: {input_file}")

        powheg_input_link = Path(self.local_path("powheg.input"))  # The file that is needed.
        if powheg_input_link.exists() or powheg_input_link.is_symlink():
            powheg_input_link.unlink()
        # Create symlink to the input file
        powheg_input_link.symlink_to(input_file)


        # Log if source doesn't exist
        if not input_file.exists():
            self.publish_message(f"WARNING: {input_file} not found, symlink points to non-existent file")

        # Create symlink to pwgseeds.dat for POWHEG to find
        seeds_template = Path(self.config['powheg_seeds_template'])
        seeds_link     = Path(self.local_path('pwgseeds.dat'))  # Where the file is needed.

        # Always create symlink, even if source doesn't exist yet
        # (POWHEG might create it, or it might already be in place)
        if seeds_link.exists() or seeds_link.is_symlink():
            seeds_link.unlink()
        # Create symlink to the template
        seeds_link.symlink_to(seeds_template)

        # Log if source doesn't exist
        if not seeds_template.exists():
            self.publish_message(f"WARNING: {seeds_template} not found, symlink points to non-existent file")

        with self.output().open('w') as f:
            f.write(f"Symlinks created for {stage}, grid {grid}\n")


class POWHEGStage(POWHEGBaseTask):
    """
    One POWHEG run (branch/seed) of a stage, and for stage 1 of a grid iteration.

    Each branch represents a parallel task within a stage. Branches of the same stage are
    grouped into a single batch submission (see branch_id, max_grouping_size, submission_type).
    Stage 1 grid iterations are sequential (iteration 2 depends on iteration 1).
    """

    branch_id = b2luigi.BatchIntParameter(
        default=0,
        description="Branch ID (seed index) for this parallel POWHEG execution (1 ... ntasks)",
        grouping=True
    )

    @property
    def max_grouping_size(self) -> int:
        """
        Maximum number of tasks to group together in a single batch job,
        as configured per stage (resources.max_grouping_size).

        Returns:
            The maximum number of tasks to group together.
        """
        return int(self.config["stages"][self.stage]["resources"]["max_grouping_size"])

    @property
    def submission_type(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("submission_type", <value>) for this task.

        Returns:
            Submission type for this task, as configured per stage: "single", "array", or "mpi".
        """
        return self.config["stages"][self.stage]["resources"]["submission_type"]

    @property
    def partition_info(self) -> Dict[str, Any]:
        """
        sinfo-derived info (node count, CPUs, memory, time limit, ...) for this stage's partition.
        Needed once submission_type == "mpi" is selected, to size the multi-node job.
        """
        partition = self.config["stages"][self.stage]["resources"]["partition"]
        return self.config["cluster_config"]["slurm"][partition]

    @property
    def task_cmd_additional_args(self) -> List[str]:
        """
        Returns:
            Additional command-line arguments for the task.
            Necessary for the batch submission.
        """
        return ["--config_file", str(self.config["config_file"])]  # Pass config file to each task

    @property
    def result_dir(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("result_dir", <value>) for this task.

        Returns:
            Directory where the results of this task are stored.
        """
        run_dir = str(self.config['job_settings']['run_dir'])
        return run_dir

    @property
    def env_script(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("env_script", <value>) for this task.

        Returns:
            Script to set up the environment for this task.
        """
        run_dir = str(self.config['job_settings']['run_dir'])
        return f"{run_dir}/bootstrap.sh"

    @property
    def task_file_dir(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("task_file_dir", <value>) for this task.

        Returns:
            Directory where task files 'executable_wrapper.sh' and 'slurm_parameters.sh' will be stored.
        """
        run_dir = str(self.config['job_settings']['run_dir'])
        return f"{run_dir}/task_files"

    def get_log_file_dir(self):
        """
        Overwrite of frameworks log file directory for this task, to have it seed specific for the slurm output and error files.
        """
        log_dir  = str(super().get_log_file_dir())
        # Use formatted seed for log file names:
        job_code = self.job_code(stage=self.stage, grid=self.grid_iteration, seed=self.branch_id, format_seed=True)

        return f"{log_dir}/{job_code}"

    @property
    def working_dir(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("working_dir", <value>) for this task.

        Returns:
            Directory from which to run the workflow; this should be absolute to avoid issues with relative paths in batch systems
        """
        return str(self.config['cwd'])

    @property
    def executable_prefix(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("executable_prefix", <value>) for this task.

        Make sure the python executable is used for tasks which was used to run the workflow.
        This is important if virtual environments in combination with batch jobs are used.
        Alternatively the virtual environment could be activated in the bootstrap.sh script.

        Returns:
            Python executable to use for this task, formatted as a list for easy concatenation with other commands.
        """
        python_path = str(self.config['python'])
        return [python_path,]

    @property
    def executable(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("executable", <value>) for this task.

        Returns:
            Arguments that make the python executable run this package's entry point (powgatory/__main__.py),
            formatted as a list for easy concatenation with other commands.
            This replaces the relative setting of the python script which 'add_filename_to_cmd == True' would use.
        """
        return ["-m", "powgatory"]

    @property
    def add_filename_to_cmd(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("add_filename_to_cmd", <value>) for this task.

        We don't want to add the filename to the command, because we already set it explicitly.
        (See 'executable' property.)
        """
        return False

    @property
    def job_name(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("job_name", <value>) for this task.

        Returns the name of the job.
        """
        return f"{self.config['job_settings']['job_name']}_{self.stage_name}_{self.format_branch_id(self.branch_id)}"

    @property
    def batch_system(self) -> str:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("batch_system", <value>) for this task.

        Returns:
            Batch system to use for this task, as configured per stage ("local" or "slurm").
        """
        return self.config["stages"][self.stage]["resources"]["batch_system"]

    @property
    def slurm_settings(self) -> Dict[str, Any]:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("slurm_settings", <value>) for this task.

        SLURM-specific settings for the batch submission.
        If batch_system is set to 'local', this returns an empty dictionary.
        """
        if self.batch_system == "local":
            return {}
        else:
            job_time      = self.config["stages"][self.stage]["resources"]["time"]
            partition     = self.config["stages"][self.stage]["resources"]["partition"]
            mem           = self.config["stages"][self.stage]["resources"]["mem"]
            cpus_per_task = 1
            exclude_nodes = self.config["job_settings"]["exclude_nodes"]

            slurm_settings = {
                "export": "NONE",
                "partition": partition,
                "ntasks": cpus_per_task,
                "mem": mem,
                "time": job_time,
                "job-name": self.job_name,
            }
            if exclude_nodes:
                slurm_settings["exclude"] = ",".join(exclude_nodes)

            return slurm_settings

    @property
    def stage_name(self) -> str:
        if self.grid_iteration > 0:
            return f"{self.stage}-grid{self.grid_iteration}"
        else:
            return f"{self.stage}"

    @property
    def __name__(self):
        return f"{self.config['job_settings']['job_name']}_{self.stage_name}"

    def requires(self):
        """Depend on setup task for this grid iteration."""
        yield POWHEGCreateSymlinks(
            stage          = self.stage,
            grid_iteration = self.grid_iteration,
            version        = self.version,
            config         = self.config
        )
        # Depend on presubmit task.
        yield POWHEGPresubmit(version=self.version, config=self.config)

        if self.stage_number == 1 and self.grid_iteration > 1:
            # Depend on previous grid iteration for Stage 1
            yield POWHEGStageTimings(stage=self.get_stage_str(1), grid_iteration=self.grid_iteration - 1, version=self.version, config=self.config)
        elif self.stage_number == 2:
            # Depend on Stage 1 for Stage 2
            stages_config       = self.config['stages']
            max_grid_iterations = stages_config['stage1'].get('grid_iterations', 3)
            yield POWHEGStageTimings(stage=self.get_stage_str(1), grid_iteration=max_grid_iterations, version=self.version, config=self.config)
        elif self.stage_number > 2:
            # Depend on previous stage for stage > 2
            yield POWHEGStageTimings(stage=self.get_stage_str(self.stage_number - 1), version=self.version, config=self.config)

    def output(self):
        """
        Output targets for POWHEG execution:
            For stage 1, there are two possible outputs:
            The xgrid 'pwg-xg?-xgrid-btl-????.dat' file and the statistics 'pwg-????-xg?-stat.dat' file.
            Depending on the implementation (ggHH does weird stuff here) 'pwggridinfo-[btl|rmn]-xg?-?.dat'
            files are produced instead.

            # FIXME: Do we always have a rmn (remnant) file for which we should check?

            For stage 2 to 4, we use the single 'pwgcounters-st?-????.dat' file, which is always produced
            and especially produced after having a finished event file.
            We check, however, for the event file 'pwgevents-????.lhe' as well, which is produced in stage 4.

            Each file set exists twice: with a 4-digit seed suffix, and with a 5-digit one, which
            POWHEG uses for seed indices above 9999 (manyseeds with more than 9999 seeds).

        Returns:
            List[List]: Output targets for this task. Each inner list represents a possible set of files that are considered as
                        outputs for the task.

        """
        grid = self.grid_iteration
        if self.stage_number == 1:
            return [
                target_set
                for seed in seed_strings(self.branch_id)
                for target_set in (
                    [self.local_target(f"pwg-xg{grid}-xgrid-btl-{seed}.dat"),
                     self.local_target(f"pwg-{seed}-xg{grid}-stat.dat"),],
                    [self.local_target(f"pwggridinfo-btl-xg{grid}-{seed}.dat"),
                     self.local_target(f"pwggridinfo-rmn-xg{grid}-{seed}.dat"),],
                )
            ]
        elif self.stage_number == 4:
            return [
                [self.local_target(f"pwgcounters-st{self.stage_number}-{seed}.dat"),
                 self.local_target(f"pwgevents-{seed}.lhe"),]
                for seed in seed_strings(self.branch_id)
            ]
        else:
            return [
                [self.local_target(f"pwgcounters-st{self.stage_number}-{seed}.dat"),]
                for seed in seed_strings(self.branch_id)
            ]

    def complete(self):
        """
        Treat stage as complete when the files of one of the possible sets of output targets exist
        and are non-empty, the same test _wait_for_stage_output applies after POWHEG ran.
        """
        return self._complete_output_set() is not None

    def _complete_output_set(self):
        """Return the first set of output targets whose files all exist and are non-empty, or None."""
        for target_set in self.output():
            try:
                sizes = [os.path.getsize(target.path) for target in target_set]
            except OSError:
                continue

            if all(size > 0 for size in sizes):
                return target_set

        return None

    @property
    def slurm_max_resubmissions(self) -> int:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("slurm_max_resubmissions", <value>) for this task.

        How often b2luigi resubmits this branch after Slurm lost its job (node failure, boot failure,
        preemption), after its job was stuck in COMPLETING, or after batch_job_is_hung reported it
        as hung. Other failures are never resubmitted.
        """
        return int(self.config["job_settings"]["retries"])

    @property
    def slurm_completing_grace(self) -> int:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("slurm_completing_grace", <value>) for this task.

        Seconds that a job without this branch's outputs may stay in COMPLETING, before b2luigi
        resubmits the branch (or the branch counts as failed, when no retries are left).
        """
        return int(self.config["job_settings"]["completing_grace"])

    @property
    def slurm_hung_check_after(self) -> int:
        """
        b2luigi setting, i.e. overwrite of b2luigi.set_setting("slurm_hung_check_after", <value>) for this task.

        Seconds after b2luigi first saw a job of this branch running at which it asks
        batch_job_is_hung, once (0 = never).
        """
        return 60 * int(self.config["job_settings"]["hung_start_minutes"])

    def batch_job_is_hung(self) -> bool:
        """
        Called by b2luigi once, slurm_hung_check_after seconds after it first saw this branch's job
        running. The job is hung if POWHEG has written nothing into its log, typically because the
        container mount hangs: a running POWHEG writes into its log within about a minute.
        run() moves an earlier attempt's log aside, so a job that hangs before that is not detected.
        """
        try:
            with open(self._powheg_log_file(), errors="replace") as log:
                head = log.read(4096)
        except OSError:
            return True
        else:
            return not any(line.strip() and not line.startswith("<Started-POWHEG") for line in head.splitlines())

    def _powheg_log_file(self) -> Path:
        """POWHEG's own log of this branch, e.g. <log_dir>/p1-x2-s7.log (not b2luigi's stdout/stderr)."""
        job_code = self.job_code(stage=self.stage, grid=self.grid_iteration, seed=self.branch_id)
        return Path(super().get_log_file_dir()).resolve() / f"{job_code}.log"

    def run(self):
        """Execute POWHEG for this branch and wait until its output files are written."""
        self._quarantine_stale_outputs()

        container = self.setup_container()

        job_code = self.job_code(stage=self.stage, grid=self.grid_iteration, seed=self.branch_id)

        if self.grid_iteration > 0:
            self.publish_message(f"Running {self.stage}, grid {self.grid_iteration}, branch {self.branch_id}")
        else:
            self.publish_message(f"Running {self.stage}, branch {self.branch_id}")

        self._run_program(job_code        = job_code,
                          seed            = self.branch_id,
                          program         = self.config['powheg_executable'],
                          apptainer_image = container,
                          working_dir     = self.config['job_settings']['run_dir'])

        self._wait_for_stage_output()

    def _wait_for_stage_output(self, retries=10, delay=5):
        """
        Poll until one of the possible output target sets is fully written.

        On cluster filesystems, output files can take a moment to become visible
        on this node after POWHEG has written them on another, so a non-zero size
        is required, not just existence.
        """
        for _ in range(retries):
            target_set = self._complete_output_set()
            if target_set is not None:
                self.publish_message(f"Output files ready: {[target.path for target in target_set]}")
                return

            self.publish_message("Expected output files not created yet. Retrying...")
            time.sleep(delay)

        raise RuntimeError(
            f"Output files for stage {self.stage_name}, branch {self.branch_id} "
            f"never appeared after {retries} retries."
        )

    def _quarantine_stale_outputs(self):
        """
        Move any leftover output/ log files from a previous, incomplete run of
        this branch out of the way, so POWHEG does not refuse to run because a
        (partial) file with the same name already exists, and so we keep a
        record of what happened in the failed attempt.
        """
        # Also grab the log file from the previous attempt, so we don't lose it.
        return self._move_to_backup(self._output_paths() | {self._powheg_log_file()})

    def _output_paths(self) -> set:
        """
        Every file that any of the possible output-target sets of this branch refers to.

        For stage 4 this includes the event file pwgevents-????.lhe. It has to be moved
        away before POWHEG runs this branch again, because POWHEG refuses to run if the
        event file already exists, even if it is left over from an incomplete run.
        """
        candidate_paths = set()
        for target_set in self.output():
            for target in target_set:
                candidate_paths.add(Path(target.path))

        return candidate_paths

    def _move_to_backup(self, candidate_paths: set) -> bool:
        """
        Move those of candidate_paths that exist into a new directory in incomplete_run_backups/.

        Returns:
            True if any file was moved.
        """
        run_dir = Path(self.config['job_settings']['run_dir'])
        backup_root = run_dir / "incomplete_run_backups"

        backup_dir = None
        def _get_backup_dir():
            nonlocal backup_dir
            if backup_dir is None:
                timestamp  = datetime.now(ZoneInfo('Europe/Berlin')).strftime("%Y%m%dT%H%M%SZ")
                backup_dir = backup_root / f"{self.stage_code(self.stage, self.grid_iteration)}-s{self.branch_id}_{timestamp}"
                backup_dir.mkdir(parents=True, exist_ok=True)
            return backup_dir

        for stale_path in candidate_paths:
            if stale_path.exists():
                destination = _get_backup_dir() / stale_path.name
                self.publish_message(f"Found stale/ incomplete file {stale_path}, moving to {destination}")
                stale_path.rename(destination)

        return backup_dir is not None

    def _run_program(self, job_code: str, seed: int, program: str, apptainer_image: str, working_dir: str, event_file: str = ""):
        """
        Run program (pwhg_main, or an analysis program, which also gets an event file) for this
        branch in working_dir, via scripts/run_program.sh.
        """
        if apptainer_image:
            self.publish_message(f"Running in container: {apptainer_image}")
            cmd = ["apptainer", "exec", "--bind", f"{self.config['cwd']}:{self.config['cwd']}", str(apptainer_image)]
        else:
            # No prefix: the job script is run directly. Process replacement is the
            # script's own job (run_program.sh execs the program), since "exec" is a shell
            # builtin and subprocess.run() does not go through a shell.
            cmd = []

        log_file = self._powheg_log_file()

        def call_job_script(script, job_code, seed, program, log_file="", event_file=""):
            """
            Call the run_program.sh script with the correct arguments.

            script, job_code, seed, program, log_file="", event_file=""
            """
            inputs     = [script, job_code, seed, program, log_file, event_file]
            cmd_script = [str(i) for i in inputs if i]  # Convert to strings and filter out empty log_file/ event_file

            return cmd_script

        cmd += call_job_script(Path(self.config["script_dir"]) / "run_program.sh", job_code, seed, program, log_file, event_file)

        self.publish_message(f"Executing command: {' '.join(cmd)} in {working_dir}")
        self.publish_message(f"Logging output to: {log_file}")
        with open(log_file, 'w') as logf:
            # Write start time to log file. E.g. <Started-POWHEG:p1-x1-s1=2026-07-20T08:10:05.858527+00:00>.
            logf.write(f"<Started-POWHEG:{job_code}={datetime.now(ZoneInfo('Europe/Berlin')).isoformat()}>\n")

        try:
            subprocess.run(
                cmd,
                cwd=working_dir,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            # POWHEG's pwhg_exit writes the counters file also when POWHEG fails, so the branch
            # would count as complete. The log stays in place; the next attempt moves it.
            self._move_to_backup(self._output_paths())
            raise RuntimeError(f"{Path(program).name} failed: {e}")
        finally:
            with open(log_file, 'a') as logf:
                logf.write(f"<Finished-POWHEG:{job_code}={datetime.now(ZoneInfo('Europe/Berlin')).isoformat()}>\n")


class POWHEGStageTimings(POWHEGBaseTask):
    """
    Summary task that scans existing POWHEG log files for a stage and writes a timing report.
    """

    stage = b2luigi.Parameter(
        default="stage1",
        description="POWHEG stage number (stage1, stage2, stage3, stage4)"
    )
    grid_iteration = b2luigi.IntParameter(
        default=-1,
        description="Grid iteration number (1, 2, 3, ...)"
    )

    batch_system = "local"

    @property
    def stage_name(self) -> str:
        if self.grid_iteration > 0:
            return f"{self.stage}-grid{self.grid_iteration}"
        return self.stage

    def requires(self):
        yield POWHEGStageWrapper(
            stage=self.stage,
            grid_iteration=self.grid_iteration,
            version=self.version,
            config=self.config,
        )

    def output(self):
        return self.local_target("timings.yaml")

    def complete(self):
        output_target = self.output()
        if not output_target.exists():
            return False

        try:
            with output_target.open("r") as handle:
                timings_data = yaml.safe_load(handle) or {}
        except yaml.YAMLError:
            return False

        if not (self.stage_name in timings_data):
            # If timings data for this stage is not present, it cannot be complete.
            return False
        else:
            # If the timings data for this stage is present, check if the dependencies are complete
            # or if the timings entry is coming from an old run.
            for dependency in self.requires():
                if not dependency.complete():
                    return False

        return True

    def _log_files(self) -> List[Path]:
        log_dir = Path(self.get_log_file_dir()).resolve()
        log_prefix = f"{self.stage_code(self.stage, self.grid_iteration)}-"
        return sorted(log_dir.glob(f"{log_prefix}*.log"))

    @staticmethod
    def _parse_log_timing(log_file: Path):
        start_time = None
        finish_time = None
        job_code = None

        with log_file.open("r") as handle:
            for line in handle:
                if line.startswith("<Started-POWHEG:"):
                    match = re.match(r"<Started-POWHEG:(?P<job_code>[^=]+)=(?P<timestamp>[^>]+)>", line.strip())
                    if match:
                        job_code = match.group("job_code")
                        start_time = datetime.fromisoformat(match.group("timestamp"))
                elif line.startswith("<Finished-POWHEG:"):
                    match = re.match(r"<Finished-POWHEG:(?P<job_code>[^=]+)=(?P<timestamp>[^>]+)>", line.strip())
                    if match:
                        finish_time = datetime.fromisoformat(match.group("timestamp"))

        if start_time is None or finish_time is None:
            return None

        if job_code is None:
            job_code = log_file.stem

        seed_match = re.search(r"-s(?P<seed>\d+)$", job_code)
        seed = int(seed_match.group("seed")) if seed_match else -1

        return {
            "log_file": log_file,
            "job_code": job_code,
            "seed": seed,
            "started_at": start_time,
            "finished_at": finish_time,
            "elapsed_seconds": (finish_time - start_time).total_seconds(),
        }

    def run(self):
        timings = []
        skipped = []

        for log_file in self._log_files():
            parsed = self._parse_log_timing(log_file)
            if parsed is None:
                skipped.append(log_file)
                continue
            timings.append(parsed)

        if not timings:
            raise RuntimeError(f"No readable POWHEG timing entries found for {self.stage_name}")

        shortest = min(timings, key=lambda entry: entry["elapsed_seconds"])
        longest  = max(timings, key=lambda entry: entry["elapsed_seconds"])
        average_seconds = sum(entry["elapsed_seconds"] for entry in timings) / len(timings)
        total_absolute_seconds = sum(entry["elapsed_seconds"] for entry in timings)

        output_target = self.output()
        timings_data = {}
        if output_target.exists():
            try:
                with output_target.open("r") as handle:
                    timings_data = yaml.safe_load(handle) or {}
            except yaml.YAMLError:
                timings_data = {}

        timings_data[self.stage_name] = {
            "Log directory": self.get_log_file_dir(),
            "Runs parsed": len(timings),
            "Skipped logs": len(skipped),
            "Total absolute time": f"{total_absolute_seconds:.3f}s",
            "Shortest": f"{shortest['elapsed_seconds']:.3f}s (seed {shortest['seed']}, log {shortest['log_file'].name})",
            "Longest": f"{longest['elapsed_seconds']:.3f}s (seed {longest['seed']}, log {longest['log_file'].name})",
            "Average": f"{average_seconds:.3f}s",
        }

        all_elapsed_seconds = []
        longest_per_stage_seconds = []
        n_stages = 0
        for key, entry in timings_data.items():
            if key in {"total", "Total"} or not isinstance(entry, dict):
                continue

            n_stages += 1
            total_absolute = entry.get("Total absolute time")
            longest_per_stage = entry.get("Longest")
            if isinstance(total_absolute, str) and total_absolute.endswith("s"):
                try:
                    all_elapsed_seconds.append(float(total_absolute[:-1]))
                except ValueError:
                    continue
            if isinstance(longest_per_stage, str):
                time_string = longest_per_stage.split()[0]
                if time_string.endswith("s"):
                    try:
                        longest_per_stage_seconds.append(float(time_string[:-1]))
                    except ValueError:
                        continue

        total_stage_absolute_seconds = sum(all_elapsed_seconds)
        run_time = sum(longest_per_stage_seconds)

        timings_data["total"] = {
            "Total absolute time": f"{total_stage_absolute_seconds:.3f}s",
            "CPU hours": f"{total_stage_absolute_seconds / 3600.0:.3f}",
            "Total run time": f"{run_time/ 3600.0:.3f}h",
            "Stages": n_stages,
        }

        with output_target.open("w") as handle:
            yaml.safe_dump(timings_data, handle, sort_keys=False, default_flow_style=False)

        self.publish_message(f"Wrote timing summary to {output_target}")


class POWHEGStageWrapper(POWHEGWrapperTask):
    """
    Wrapper task for a POWHEG stage, yielding multiple parallel tasks.
    """
    stage          = b2luigi.Parameter(
                         default="stage1",
                         description="POWHEG stage number (stage1, stage2, stage3, stage4)"
                     )
    grid_iteration = b2luigi.IntParameter(
                         default=-1,
                         description="Grid iteration number (1, 2, 3, ...)"
                     )

    def requires(self):
        ntasks = self.config["stages"][f"{self.stage}"]["resources"]['ntasks']

        for branch_id in range(1, ntasks + 1):
            yield POWHEGStage(stage=self.stage,
                              grid_iteration=self.grid_iteration,
                              version=self.version,
                              branch_id=branch_id,
                              config=self.config)


######################
# Main Workflow Task #
######################
class POWHEGWorkflow(POWHEGWrapperTask):
    """
    Define the main workflow task that coordinates all stages.
    Depending on the configuration, it will yield the appropriate stage tasks.
    """
    def requires(self):
        stages_config = self.config['stages']

        # if stages_config['addweights']['enabled']:
        #     return POWHEGAddWeights(config_file=self.config_file, version=self.version)
        # elif stages_config['analysis']['enabled']:
        #     return POWHEGAnalysis(config_file=self.config_file, version=self.version)

        if stages_config['stage4']['enabled']:
            highest_stage = 4
        elif stages_config['stage3']['enabled']:
            highest_stage = 3
        elif stages_config['stage2']['enabled']:
            highest_stage = 2
        elif stages_config['stage1']['enabled']:
            highest_stage = 1
        else:
            highest_stage = 0

        if highest_stage == 1:
            max_grid_iterations = stages_config["stage1"]["grid_iterations"]
            yield POWHEGStageTimings(stage=self.get_stage_str(1), grid_iteration=max_grid_iterations, version=self.version, config=self.config)
        else:
            yield POWHEGStageTimings(stage=self.get_stage_str(highest_stage), version=self.version, config=self.config)


class POWHEGMultiConfigWorkflow(POWHEGWrapperTask):
    configuration_files = b2luigi.ListParameter(hashed=True, description="List of POWHEG configuration files to run.")

    def requires(self):
        for config_file in self.configuration_files:
            config = POWHEGConfig.from_yaml(config_file)

            config_dict = config.to_dict()  # Convert to dictionary for serialization

            yield POWHEGWorkflow(version=self.version, config=config_dict)
