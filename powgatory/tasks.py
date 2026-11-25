#!/usr/bin/env python3
"""
b2luigi Task Definitions for POWHEG-BOX Workflow

This module implements the POWHEG workflow using b2luigi (BELLE2 Luigi):
- Workflow coordination via Luigi
- Local and batch execution support
- Container execution via b2luigi
"""

import b2luigi
import glob
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
from .framework import POWHEGBaseTask, POWHEGWrapperTask, get_stage_str, short_repr, stage_code
from .config import (
    POWHEGConfig, ANALYSIS_STAGES, SLURM_EXPORT, auto_grouping_size, slurm_partitions, slurm_submissions,
    slurm_tasks_per_node,
)
from .merge import METHOD_NAMES, merge_histograms
from .ub_correction import correction_factors


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
        # The name create_powheg_input writes: e.g. p1-x2-powheg.input, or stage3_fullgrid-powheg.input.
        return self.local_target(f"{stage_code(self.stage, self.grid_iteration)}-powheg.input")

    def run(self):
        """Create the input file."""
        self.create_powheg_input(stage=self.stage, grid=self.grid_iteration)

    def create_powheg_input(self, stage:str, grid: int = 1):
        """
        Create POWHEG input file for a job.

        Args:
            stage (str): POWHEG stage number (stage1, stage2, stage3, stage4,
                                              stage3_fullgrid, analysis, addweights)
            grid (int): Grid iteration number (1, 2, 3, ...)
        Returns:
            Path to the created input file.
        """
        input_template = self.config['powheg_input_template']

        input_file = self.local_path(f"{stage_code(stage, grid)}-powheg.input")

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
                # A line "<name> <value> ! <comment>": the keyword, the spacing (kept, so that the
                # new value stays aligned), the value and an optional "!" comment. The value is a
                # Fortran number like 1d0 or a Python %-format placeholder like %(parallelstage)s,
                # which some templates have from scripts that filled them with "template % dict".
                match = re.match(
                    r"(?P<name>\w+)(?P<space>\s+)(?P<value>[\d\.\-d]+|%\(\w+\)\w)(\s*\!\s*(?P<comment>.+))?",
                    line
                )
                if match:
                    name = match.group("name")
                    value = match.group("value")
                    total_length = len(match.group("space")) + len(value)
                    comment = match.group("comment")

                    for param_name, param_value in replacements.items():
                        if name in [param_name, param_name[1:]] and not found[param_name]:
                            new_line = f"{name} {str(param_value):>{total_length - 1}}"
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
            yield POWHEGStageTimings(stage=get_stage_str(1), grid_iteration=self.grid_iteration - 1, version=self.version, config=self.config)
        elif self.stage_number == 2:
            # Depend on Stage 1 for Stage 2
            stages_config       = self.config['stages']
            max_grid_iterations = stages_config['stage1'].get('grid_iterations', 3)
            yield POWHEGStageTimings(stage=get_stage_str(1), grid_iteration=max_grid_iterations, version=self.version, config=self.config)
        elif self.stage == "stage3" and self.config["stages"]["stage3_fullgrid"]["enabled"]:
            # The grid combination has run the first seeds of stage 3 and written the fullgrid files.
            yield POWHEGStageTimings(stage="stage3_fullgrid", version=self.version, config=self.config)
        elif self.stage_number > 2:
            # Depend on previous stage for stage > 2
            yield POWHEGStageTimings(stage=get_stage_str(self.stage_number - 1), version=self.version, config=self.config)


    def create_symlinks(self, stage: str, grid: int = -1):
        """Create symlinks for POWHEG input and seeds."""

        if grid > 0:
            self.publish_message(f"Creating symlinks for {stage}, grid {grid}")
        else:
            self.publish_message(f"Creating symlinks for {stage}")

        input_file = Path(self.local_path(f"{stage_code(stage, grid)}-powheg.input"))

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
        grouping=True,
        # A group holds its seeds in the order luigi's scheduler took them; sorted, the names
        # show them as ranges (e.g. 1-10), and element k of a job array runs the k-th seed.
        grouping_function=sorted,
    )

    def task_label(self) -> str:
        """The label of the task ID: stage, grid iteration and seed(s) as in log names, e.g. p1-x2-s7."""
        return self.job_code(stage=self.stage, grid=self.grid_iteration, seed=self.branch_id)

    @property
    def max_grouping_size(self) -> int:
        """
        Maximum number of tasks to group together in a single batch job,
        as configured per stage (resources.max_grouping_size).

        auto stays in the config and is resolved here: only luigi's worker in the main process reads
        this, once per task family, so batch jobs never query Slurm for it. luigi uses the value of the
        first task of a family for all of them, so auto is the same for every stage of a config: the
        largest job array or MPI job that all its Slurm stages can submit (auto_grouping_size).

        Returns:
            The maximum number of tasks to group together.
        """
        max_grouping_size = self.config["stages"][self.stage]["resources"]["max_grouping_size"]
        if max_grouping_size == "auto":
            return auto_grouping_size(slurm_submissions(self.config))
        else:
            return int(max_grouping_size)

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
        Slurm's info (node count, CPUs, cores and memory per node, time limit, ...) for this stage's
        partition, used to size MPI jobs. Slurm is queried once per process, by the process that
        reads this, i.e. the one that submits; it is not part of the config (see slurm_partitions).
        """
        partition = self.config["stages"][self.stage]["resources"]["partition"]
        return slurm_partitions()[partition]

    @property
    def tasks_per_node(self) -> int:
        """
        b2luigi setting for submission_type mpi: how many seeds an MPI job runs per node, the cores or
        CPUs per node of the stage's partition (slurm_tasks_per_node). b2luigi requests
        ceil(seeds / tasks_per_node) nodes.
        """
        return slurm_tasks_per_node(self.partition_info)

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
        job_code = self.job_code(stage=self.stage, grid=self.grid_iteration, seed=self.branch_id)

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
    def resources(self) -> Dict[str, int]:
        """
        luigi resources of this branch. A local branch runs one program on the node powgatory runs
        on and takes one of the local_cores there (job_settings.local_parallel_tasks, see cli.py).
        A Slurm branch takes none.
        """
        if self.batch_system == "local":
            return {"local_cores": 1}
        else:
            return {}

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

            if self.submission_type == "mpi":
                # mem is per seed, but --mem is per node and the cluster has no --mem-per-cpu, so an
                # MPI job requests mem for each of its seeds on a node. branch_id is the tuple of the
                # group's seeds.
                group_size     = len(self.branch_id) if isinstance(self.branch_id, tuple) else 1
                seeds_per_node = min(group_size, self.tasks_per_node)
                mem            = mem * seeds_per_node

            slurm_settings = {
                "export": SLURM_EXPORT,
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
            yield POWHEGStageTimings(stage=get_stage_str(1), grid_iteration=self.grid_iteration - 1, version=self.version, config=self.config)
        elif self.stage_number == 2:
            # Depend on Stage 1 for Stage 2
            stages_config       = self.config['stages']
            max_grid_iterations = stages_config['stage1'].get('grid_iterations', 3)
            yield POWHEGStageTimings(stage=get_stage_str(1), grid_iteration=max_grid_iterations, version=self.version, config=self.config)
        elif self.stage == "stage3" and self.config["stages"]["stage3_fullgrid"]["enabled"]:
            # The grid combination has run the first seeds of stage 3 and written the fullgrid files.
            yield POWHEGStageTimings(stage="stage3_fullgrid", version=self.version, config=self.config)
        elif self.stage_number > 2:
            # Depend on previous stage for stage > 2
            yield POWHEGStageTimings(stage=get_stage_str(self.stage_number - 1), version=self.version, config=self.config)

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
        return self._move_to_backup(self._output_paths() | {self._powheg_log_file()} | self._own_fullgrid_locks())

    def _own_fullgrid_locks(self) -> set:
        """
        The fullgrid locks (pwg-<contribution>-fullgrid.lock) left by an earlier attempt of this
        stage-3 seed. In stage 3, POWHEG writes the seed into the lock of the contribution whose
        grids it combines, and pwhg_exit deletes the lock. A lock is only left behind when the
        process was killed (time limit, scancel, node failure), and every other seed would then wait
        for it forever. A lock with this seed's number can only come from such an attempt, since a
        seed never runs twice at the same time; locks of other seeds are left alone.
        """
        if self.stage in ("stage3_fullgrid", "stage3"):
            run_dir = Path(self.config['job_settings']['run_dir'])
            return {lock for lock in run_dir.glob("*-fullgrid.lock") if self._lock_seed(lock) in seed_strings(self.branch_id)}
        else:
            return set()

    @staticmethod
    def _lock_seed(lock: Path) -> str:
        """The seed POWHEG wrote into a lock file, or "" if it is empty or already deleted by its seed."""
        try:
            return lock.read_text().strip()
        except OSError:
            return ""

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
                backup_dir = backup_root / f"{stage_code(self.stage, self.grid_iteration)}-s{self.branch_id}_{timestamp}"
                backup_dir.mkdir(parents=True, exist_ok=True)
            return backup_dir

        for stale_path in candidate_paths:
            if stale_path.exists():
                destination = _get_backup_dir() / stale_path.name
                self.publish_message(f"Found stale/ incomplete file {stale_path}, moving to {destination}")
                stale_path.rename(destination)

        return backup_dir is not None

    def _container_binds(self, program: str, working_dir: str) -> List[Path]:
        """
        The directories the container needs, each bound at its own path, since apptainer only
        mounts the home directory: the directory powgatory was started from, the run and working
        directory, POWgatory's package (scripts/run_program.sh, the default seeds template), the
        program's directory, the directory of the seeds template (<run_dir>/pwgseeds.dat points at
        it) and, for the analysis stages, those of the link_files. A directory inside another one of
        them is left out.
        """
        directories = [
            Path(self.config["cwd"]),
            Path(self.config["job_settings"]["run_dir"]),
            Path(working_dir),
            Path(self.config["script_dir"]).parent,
            Path(program).parent,
            Path(self.config["powheg_seeds_template"]).parent,
        ]
        if self.stage in ANALYSIS_STAGES:
            directories += [target.parent for target in link_file_targets(self.config, self.stage)]

        binds = []
        for directory in sorted(set(directories), key=lambda directory: len(directory.parts)):
            if not any(directory.is_relative_to(bound) for bound in binds):
                binds.append(directory)
        return binds

    def _run_program(self, job_code: str, seed: int, program: str, apptainer_image: str, working_dir: str, event_file: str = ""):
        """
        Run program (pwhg_main, or an analysis program, which also gets an event file) for this
        branch in working_dir, via scripts/run_program.sh.
        """
        if apptainer_image:
            self.publish_message(f"Running in container: {apptainer_image}")
            binds = [arg for directory in self._container_binds(program, working_dir) for arg in ("--bind", f"{directory}:{directory}")]
            cmd   = ["apptainer", "exec", *binds, str(apptainer_image)]
        else:
            # No prefix: the job script is run directly. Process replacement is the
            # script's own job (run_program.sh execs the program), since "exec" is a shell
            # builtin and subprocess.run() does not go through a shell.
            cmd = []

        if self.batch_system == "local":
            # Inherited by apptainer and by the program that run_program.sh execs.
            cmd = ["nice", "-n", str(self.config["job_settings"]["local_niceness"])] + cmd

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
        description="POWHEG stage number (stage1, stage2, stage3, stage4, lhef, pythia)"
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
        log_prefix = f"{stage_code(self.stage, self.grid_iteration)}-"
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


##########################################################
# Histogram files of stage 2 and of the analysis stages #
##########################################################
# Name patterns of a stage's histogram files, without ".top"; {seed} is the seed with four or
# five digits, and with several event weights pwhgtopout appends -W1, -W2, ... The names depend
# on the process's programs (pwhg_analysis, lhef_analysis.f, main-PYTHIA8.f), so a stage has a
# list of them: an analysis branch is complete when the file of any of them is there, and the
# merge uses the first one of which seed 1 has a file. "pwg" is the run prefix, for the analyses
# the part of the event file name pwgevents-<seed>.lhe before "events".
HISTOGRAM_NAMES = {
    "stage2": ["pwg-{seed}-NLO"],
    "lhef":   ["pwgLHEF_analysis-{seed}"],
    "pythia": ["pwgoutput_py8_histos-{seed}", "pwgevents-{seed}.lheoutput_py8_histos"],
}


def histogram_weights(directory: Path, name: str) -> List[str]:
    """
    The weight suffixes of the histogram files <name>[-W<n>].top in directory: "-W1", "-W2", ...
    with several weights, [""] for a single file <name>.top, and [] if there is none.
    """
    prefix  = f"{name}-W"
    weights = sorted(
        int(path.stem.removeprefix(prefix))
        for path in directory.glob(f"{prefix}*.top")
        if path.stem.removeprefix(prefix).isdigit()
    )
    if weights:
        return [f"-W{weight}" for weight in weights]
    elif (directory / f"{name}.top").exists():
        return [""]
    else:
        return []


##########################################################
# Analysis stages (lhef, pythia) of the stage-4 events #
##########################################################


def link_file_targets(config, stage: str) -> List[Path]:
    """The files matched by a stage's link_files, relative to cwd or absolute, glob patterns allowed."""
    targets = []
    for pattern in config["stages"][stage]["link_files"]:
        matches = sorted(glob.glob(str(Path(config["cwd"]) / pattern)))
        if not matches:
            raise FileNotFoundError(f"link_files of stage {stage}: {pattern} matches no file.")
        targets += [Path(match) for match in matches]

    names = [target.name for target in targets]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f"link_files of stage {stage}: several files are named {', '.join(duplicates)}.")

    return targets


def stage4_seeds(config) -> range:
    """The seeds of stage 4, i.e. of the event files the analysis stages read."""
    return range(1, config["stages"]["stage4"]["resources"]["ntasks"] + 1)


def grid_combination_seeds(config) -> range:
    """
    The seeds of the grid combination (stage3_fullgrid): the first stage-3 seeds, one per
    contribution. In bbinit.f, seed k starts with contribution k (btilde, remn, reg), so with
    btilde switched on, these seeds combine the grids of all contributions in parallel.
    """
    return range(1, config["stages"]["stage3_fullgrid"]["contributions"] + 1)


def stage_seeds(config, stage: str) -> range:
    """
    The seeds of stage 1-4 or of the grid combination. With the grid combination, stage 3 runs
    only the seeds after it: luigi runs a task it scheduled as incomplete without checking
    complete() again, so stage 3 would otherwise run the grid combination's seeds a second time.
    """
    if stage == "stage3_fullgrid":
        return grid_combination_seeds(config)
    elif stage == "stage3" and config["stages"]["stage3_fullgrid"]["enabled"]:
        return range(len(grid_combination_seeds(config)) + 1, config["stages"]["stage3"]["resources"]["ntasks"] + 1)
    else:
        return range(1, config["stages"][stage]["resources"]["ntasks"] + 1)


def seed_file(directory: Path, pattern: str, seed: int) -> Path:
    """
    The file named by pattern (e.g. "pwgevents-{seed}.lhe") in directory, with the seed in four
    digits, or else in five digits (see seed_strings).
    """
    four_digits, five_digits = (directory / pattern.format(seed=seed_string) for seed_string in seed_strings(seed))
    if four_digits.exists():
        return four_digits
    else:
        return five_digits


class POWHEGWeightCorrection(POWHEGBaseTask):
    """
    Upper bound correction factors (ub_btilde_corr, ub_remn_corr) of every stage-4 seed, from its
    own pwgcounters-st4-<seed>.dat, written once into ub_corrections.yaml for all analysis stages.
    """
    batch_system = "local"

    # Inherited from POWHEGBaseTask, but unused: the task belongs to no stage.
    stage          = b2luigi.Parameter(default="stage1", significant=False)
    grid_iteration = b2luigi.IntParameter(default=1, significant=False)

    @property
    def stage_name(self) -> str:
        return "weight_correction"

    def task_label(self) -> str:
        """The label of the task ID: none, since the task has no stage of its own."""
        return ""

    def requires(self):
        yield POWHEGStageTimings(stage=get_stage_str(4), version=self.version, config=self.config)

    def output(self):
        return self.local_target("ub_corrections.yaml")

    def complete(self):
        """Complete when ub_corrections.yaml lists every stage-4 seed (stage 4 may have got more)."""
        if not self.output().exists():
            return False

        with self.output().open("r") as handle:
            factors = yaml.safe_load(handle) or {}

        return set(stage4_seeds(self.config)) <= set(factors)

    def run(self):
        run_dir = Path(self.config["job_settings"]["run_dir"])

        factors = {}
        for seed in stage4_seeds(self.config):
            counters_file = seed_file(run_dir, "pwgcounters-st4-{seed}.dat", seed)
            factors[seed] = correction_factors([counters_file])
            if factors[seed]:
                self.publish_message(f"Seed {seed}: {factors[seed]}")

        with self.output().open("w") as handle:
            yaml.safe_dump(factors, handle, sort_keys=False)


class POWHEGAnalysisSetup(POWHEGStageSetup):
    """
    Setup of an analysis stage (lhef or pythia) in its own directory <run_dir>/<stage>/: one input
    file <seed>-powheg.input per stage-4 seed, the event files and the stage's link_files.

    The analysis programs read, without a powheg.input in their directory, first the prefix of
    their input file (the seed) and then the event file (see scripts/run_program.sh). The input
    files are the template with the stage's powheg_parameters, as for stages 1-4, plus the seed's
    upper bound correction factors if the stage has weight_correction.
    """

    def requires(self):
        yield POWHEGStageTimings(stage=get_stage_str(4), version=self.version, config=self.config)
        if self.config["stages"][self.stage]["weight_correction"]:
            yield POWHEGWeightCorrection(version=self.version, config=self.config)

    def output(self):
        return [self.local_target(self.stage, f"{seed}-powheg.input") for seed in stage4_seeds(self.config)]

    def run(self):
        # Resolved first, so that a pattern without a match stops the setup before it writes its
        # outputs, which would otherwise count as complete in the next run.
        link_targets = link_file_targets(self.config, self.stage)

        run_dir   = Path(self.config["job_settings"]["run_dir"])
        stage_dir = run_dir / self.stage
        stage_dir.mkdir(exist_ok=True)

        if self.config["stages"][self.stage]["weight_correction"]:
            with POWHEGWeightCorrection(version=self.version, config=self.config).output().open("r") as handle:
                factors = yaml.safe_load(handle)
        else:
            factors = {}

        for seed in stage4_seeds(self.config):
            # The values of stage 4, whose events the stage analyses: the analysis programs read
            # their input with powheginput, which stops on a value it cannot parse, such as a
            # placeholder %(parallelstage)s left in the template.
            powheg_params = {'parallelstage': 4, 'xgriditeration': 1}
            powheg_params.update(self.config["stages"][self.stage]["powheg_parameters"])
            powheg_params.update(factors.get(seed, {}))
            self.change_param(self.config["powheg_input_template"], stage_dir / f"{seed}-powheg.input", powheg_params)

            # A relative link: the programs take everything before "events" in the name as prefix
            # of their output files, so "../pwgevents-0001.lhe" would put the outputs into run_dir.
            event_file = seed_file(run_dir, "pwgevents-{seed}.lhe", seed)
            self._link(stage_dir / event_file.name, Path("..") / event_file.name)

        for link_target in link_targets:
            self._link(stage_dir / link_target.name, link_target)

    @staticmethod
    def _link(link: Path, target: Path):
        """Point link at target, replacing an earlier link, but never a real file of that name."""
        if link.is_symlink():
            link.unlink()
        link.symlink_to(target)


class POWHEGAnalysis(POWHEGStage):
    """
    One run of an analysis stage (lhef or pythia) on the events of one stage-4 seed, in
    <run_dir>/<stage>/ with the program <stage>_executable.
    """

    def requires(self):
        yield POWHEGAnalysisSetup(stage=self.stage, grid_iteration=-1, version=self.version, config=self.config)
        # Depend on presubmit task, which sets up the container.
        yield POWHEGPresubmit(version=self.version, config=self.config)

    def output(self):
        """
        The histogram file the stage's program writes for this seed, for every name pattern in
        HISTOGRAM_NAMES and the seed in four or in five digits. With several event weights
        (rwl_num_weights > 1), pwhgtopout writes one file per weight instead, <name>-W1.top,
        -W2.top, ..., in this order; -W1 stands for all of them, since their number is only known
        from the event file.
        """
        return [
            [self.local_target(self.stage, f"{pattern.format(seed=seed)}{weight}.top")]
            for pattern in HISTOGRAM_NAMES[self.stage]
            for seed in seed_strings(self.branch_id)
            for weight in ("", "-W1")
        ]

    def run(self):
        """Run the stage's program on this seed's event file and wait until its output files are written."""
        self._quarantine_stale_outputs()

        container = self.setup_container()

        job_code   = self.job_code(stage=self.stage, grid=self.grid_iteration, seed=self.branch_id)
        stage_dir  = Path(self.config["job_settings"]["run_dir"]) / self.stage
        event_file = seed_file(stage_dir, "pwgevents-{seed}.lhe", self.branch_id)

        self.publish_message(f"Running {self.stage}, branch {self.branch_id}")

        self._run_program(job_code        = job_code,
                          seed            = self.branch_id,
                          program         = self.config[f"{self.stage}_executable"],
                          apptainer_image = container,
                          working_dir     = stage_dir,
                          event_file      = event_file.name)

        self._wait_for_stage_output()


class POWHEGMerge(POWHEGStage):
    """
    Merge of the histogram files of a stage (stage2, lhef or pythia) over all its seeds, one merged
    file per weight, with the stage's merge_method (see merge.py): e.g. pwg-mean-NLO.top or
    lhef/pwgLHEF_analysis-mean-W1.top. One branch with the stage's resources, so a Slurm job, or a
    local process with batch_system local.
    """

    @property
    def job_name(self) -> str:
        return f"{self.config['job_settings']['job_name']}_{self.stage}_merge"

    def task_label(self) -> str:
        """The label of the task ID: the stage, without the branch_id, which is always 1."""
        return f"{self.stage}-merge"

    def get_log_file_dir(self):
        """b2luigi's stdout and stderr of the merge, in <log_dir>/<stage>-merge/."""
        return f"{POWHEGBaseTask.get_log_file_dir(self)}/{self.stage}-merge"

    def batch_job_is_hung(self) -> bool:
        """
        The merge never counts as hung. It has to override the method it inherits from
        POWHEGStage, which b2luigi would otherwise call: that one reads the POWHEG log of the
        stage's seed 1, which says nothing about the merge.
        """
        return False

    def requires(self):
        yield POWHEGStageTimings(stage=self.stage, version=self.version, config=self.config)

    def output(self):
        return [[b2luigi.LocalTarget(str(merged_file))] for merged_file in self._merged_files()]

    def complete(self):
        """
        Not before the stage's timings task is complete; then when every merged file exists and is
        non-empty, or when there is nothing to merge (stage 2 writes its NLO files only with
        testplots 1).
        """
        if not POWHEGStageTimings(stage=self.stage, version=self.version, config=self.config).complete():
            return False
        else:
            return all(merged_file.exists() and merged_file.stat().st_size > 0 for merged_file in self._merged_files())

    def run(self):
        """Merge the histogram files of all seeds, one merged file per weight and merge method."""
        pattern, weights = self._pattern_and_weights()

        if pattern is None:
            # E.g. stage 2 writes its NLO histograms only with testplots 1.
            self.publish_message(f"No histogram files of {self.stage} to merge: seed 1 has none of {HISTOGRAM_NAMES[self.stage]}[-W<n>].top.")
        else:
            for weight in weights:
                input_files, missing_seeds = [], []
                for seed in self._seeds():
                    histogram_file = seed_file(self._histogram_dir(), f"{pattern}{weight}.top", seed)
                    if histogram_file.exists():
                        input_files.append(histogram_file)
                    else:
                        missing_seeds.append(seed)

                if missing_seeds:
                    self.publish_message(f"WARNING: {len(missing_seeds)} seeds have no {pattern}{weight}.top and are left out: {missing_seeds[:20]}")

                for method in self.config["stages"][self.stage]["merge_method"]:
                    merged_file = self._merged_file(pattern, weight, method)
                    self.publish_message(f"Merging {len(input_files)} files into {merged_file} with method {method} ({METHOD_NAMES[method]})")
                    merge_histograms(input_files, method, merged_file)

    def _histogram_dir(self) -> Path:
        run_dir = Path(self.config["job_settings"]["run_dir"])
        if self.stage in ANALYSIS_STAGES:
            return run_dir / self.stage
        else:
            return run_dir

    def _seeds(self) -> range:
        if self.stage in ANALYSIS_STAGES:
            return stage4_seeds(self.config)
        else:
            return range(1, self.config["stages"][self.stage]["resources"]["ntasks"] + 1)

    def _pattern_and_weights(self):
        """
        The first name pattern of the stage in HISTOGRAM_NAMES of which seed 1 has a histogram
        file (a run keeps one naming convention), and the weight suffixes of seed 1's files; or
        (None, []) if seed 1 has none.
        """
        for pattern in HISTOGRAM_NAMES[self.stage]:
            for seed_string in seed_strings(self._seeds()[0]):
                weights = histogram_weights(self._histogram_dir(), pattern.format(seed=seed_string))
                if weights:
                    return pattern, weights
        return None, []

    def _merged_files(self) -> List[Path]:
        """The merged files of every weight and merge method."""
        pattern, weights = self._pattern_and_weights()
        return [
            self._merged_file(pattern, weight, method)
            for weight in weights
            for method in self.config["stages"][self.stage]["merge_method"]
        ]

    def _merged_file(self, pattern: str, weight: str, method: int) -> Path:
        """The merged file of a weight and merge method: the pattern with the method's name in place of the seed."""
        return self._histogram_dir() / f"{pattern.format(seed=METHOD_NAMES[method])}{weight}.top"


class POWHEGStageWrapper(POWHEGWrapperTask):
    """
    Wrapper task for a POWHEG stage, yielding multiple parallel tasks.
    """
    stage          = b2luigi.Parameter(
                         default="stage1",
                         description="POWHEG stage number (stage1, stage2, stage3, stage4, lhef, pythia)"
                     )
    grid_iteration = b2luigi.IntParameter(
                         default=-1,
                         description="Grid iteration number (1, 2, 3, ...)"
                     )

    def task_label(self) -> str:
        """The label of the task ID: the stage and grid iteration as in file names, e.g. p1-x2."""
        return stage_code(self.stage, self.grid_iteration)

    def requires(self):
        if self.stage in ANALYSIS_STAGES:
            # One analysis run per stage-4 event file.
            branch_ids = stage4_seeds(self.config)
            stage_task = POWHEGAnalysis
        else:
            branch_ids = stage_seeds(self.config, self.stage)
            stage_task = POWHEGStage

        for branch_id in branch_ids:
            yield stage_task(stage=self.stage,
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
            yield POWHEGStageTimings(stage=get_stage_str(1), grid_iteration=max_grid_iterations, version=self.version, config=self.config)
        elif highest_stage == 2 and stages_config['stage3_fullgrid']['enabled']:
            # The grid combination comes between stage 2 and stage 3, so a run can stop after it.
            yield POWHEGStageTimings(stage="stage3_fullgrid", version=self.version, config=self.config)
        else:
            yield POWHEGStageTimings(stage=get_stage_str(highest_stage), version=self.version, config=self.config)

        # The merges of the histogram files; the one of stage 2 runs while stage 3 runs, since
        # stage 3 requires stage 2's timings task, not the merge. Without a merge, an analysis
        # stage is required through its timings task (stage 2 already is, through stage 3 or 4).
        for stage in ("stage2",) + ANALYSIS_STAGES:
            enabled = stages_config[stage]['enabled']
            merged  = bool(stages_config[stage]['merge_method'])
            if enabled and merged:
                yield POWHEGMerge(stage=stage, grid_iteration=-1, branch_id=1, version=self.version, config=self.config)
            elif enabled and stage in ANALYSIS_STAGES:
                yield POWHEGStageTimings(stage=stage, version=self.version, config=self.config)


class POWHEGMultiConfigWorkflow(POWHEGWrapperTask):
    configuration_files = b2luigi.ListParameter(hashed=True, description="List of POWHEG configuration files to run.")

    def __repr__(self):
        """Without the list of files, which can have hundreds; each POWHEGWorkflow names its own."""
        return short_repr(self, hidden=("configuration_files",))

    def requires(self):
        for config_file in self.configuration_files:
            config = POWHEGConfig.from_yaml(config_file)

            config_dict = config.to_dict()  # Convert to dictionary for serialization

            yield POWHEGWorkflow(version=self.version, config=config_dict)
