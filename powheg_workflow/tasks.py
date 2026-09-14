#!/usr/bin/env python3
"""
b2luigi Task Definitions for POWHEG-BOX Workflow

This module implements the POWHEG workflow using b2luigi (BELLE2 Luigi):
- Workflow coordination via Luigi
- Local and batch execution support
- Container execution via b2luigi
"""

from sys import path

import b2luigi
import os
import re
import time
import subprocess
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, Any, Optional, List

import yaml

# from .config import POWHEGConfig

# Import our framework classes
from .framework import POWHEGBaseTask, POWHEGWrapperTask
from .config import POWHEGConfig


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
        """Execute the presubmit script if defined."""
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
    Setup task for Stage 1: Creates input file and container.
    All Stage 1 parallel tasks depend on this completing first.
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
        """Setup container and create input file."""
        # self.setup_container()
        # job_code = self.job_code(stage=self.stage, grid=self.grid_iteration, seed=1)
        self.create_powheg_input(stage=self.stage, grid=self.grid_iteration)

        # self.get_scripts()

    # def requires(self):
    #     """Depend on presubmit task."""
    #     yield POWHEGPresubmit(version=self.version, config=self.config)

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
    Stage 1: Grid generation with iterations.

    Each branch represents a parallel task within a grid iteration.
    Grid iterations are sequential (iteration 2 depends on iteration 1).
    """

    branch_id = b2luigi.BatchIntParameter(
        default=0,
        description="Branch ID for this parallel POWHEG execution (0-indexed)",
        grouping=True
    )

    @property
    def max_grouping_size(self) -> int:
        """
        Maximum number of tasks to group together in a single batch job.
        # TODO: Make it a parameter of the config file.

        Returns:
            The maximum number of tasks to group together.
        """
        # return int(self.config["cluster_config"]["slurm"]["max_grouping_size"])
        # return int(self.config["cluster_config"]["slurm"]["max_parallel_jobs"])
        return 100

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
            Python script to execute for this task, formatted as a list for easy concatenation with other commands.
            Path to the main entry point of this program.
            This replaces the relative setting of the python script which 'add_filename_to_cmd == True' would use.
        """
        this_file_path = Path(__file__).resolve()
        main_file      = this_file_path.parent.parent / "run_workflow.py"
        return [str(main_file),]

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
        return f"{self.config['job_settings']['job_name']}_{self.stage_name}_s{self.format_branch_id(self.branch_id)}"

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

        Returns:
            List[List]: Output targets for this task. Each inner list represents a possible set of files that are considered as
                        outputs for the task.

        """
        if self.stage_number == 1:
            return [
                [self.local_target(f"pwg-xg{self.grid_iteration}-xgrid-btl-{self.branch_id:04d}.dat"),
                 self.local_target(f"pwg-{self.branch_id:04d}-xg{self.grid_iteration}-stat.dat"),],
                #
                [self.local_target(f"pwggridinfo-btl-xg{self.grid_iteration}-{self.branch_id:04d}.dat"),
                 self.local_target(f"pwggridinfo-rmn-xg{self.grid_iteration}-{self.branch_id:04d}.dat"),],
            ]
        elif self.stage_number == 4:
            return [
                [self.local_target(f"pwgcounters-st{self.stage_number}-{self.branch_id:04d}.dat"),
                 self.local_target(f"pwgevents-{self.branch_id:04d}.lhe"),],
            ]
        else:
            return [
                [self.local_target(f"pwgcounters-st{self.stage_number}-{self.branch_id:04d}.dat"),],
            ]

    def complete(self):
        """Treat stage as complete when one of the possible sets of output targets exists."""
        output = self.output()

        for target_set in output:
            if all(target.exists() for target in target_set):
                return True

        return False

    def run(self):
        """Execute one task of Stage 1."""
        self._quarantine_stale_outputs()

        container = self.setup_container()

        job_code = self.job_code(stage=self.stage, grid=self.grid_iteration, seed=self.branch_id)

        if self.grid_iteration > 0:
            self.publish_message(f"Running {self.stage}, grid {self.grid_iteration}, branch {self.branch_id}")
        else:
            self.publish_message(f"Running {self.stage}, branch {self.branch_id}")

        # Execute POWHEG
        self._run_powheg(job_code=job_code, task_id=self.branch_id, apptainer_image=container)

        if self.stage_number == 1:
            output = self.local_path(f"pwg-xg{self.grid_iteration}-xgrid-btl-{self.branch_id:04d}.dat")
        else:
            output = self.local_path(f"pwgcounters-st{self.stage_number}-{self.branch_id:04d}.dat")

        for _ in range(10):
            try:
                filesize = os.path.getsize(output)
            except OSError as err:
                filesize = 0
                print("OS error: {0}".format(err))

            if filesize > 0:
                self.publish_message(f"Output file {output} created successfully with size {filesize} bytes.")
                break
            else:
                self.publish_message(f"Output file {output} is empty or not created yet. Retrying...")
                time.sleep(5)

        #     # Mark complete
        #     with self.output().open('w') as f:
        #         f.write(f"{self.stage} completed for branch {self.branch_id}\n")

    def _quarantine_stale_outputs(self):
        """
        Move any leftover output/ log files from a previous, incomplete run of
        this branch out of the way, so POWHEG does not refuse to run because a
        (partial) file with the same name already exists, and so we keep a
        record of what happened in the failed attempt.
        """
        run_dir = Path(self.config['job_settings']['run_dir'])
        backup_root = run_dir / "incomplete_run_backups"

        # Collect every filename that any of the possible output-target sets for
        # this stage/branch could refer to.
        candidate_paths = set()
        for target_set in self.output():
            for target in target_set:
                candidate_paths.add(Path(target.path))

        # Stage 4's event file is the known culprit: POWHEG refuses to run if it
        # already exists, even if the run was previously incomplete.
        move_incomplete_event_files = False # FIXME: Make this configurable in the config file.
        if self.stage_number == 4 and move_incomplete_event_files:
            candidate_paths.add(run_dir / f"pwgevents-{self.branch_id:04d}.lhe")

        # Also grab the log file from the previous attempt, so we don't lose it.
        job_code = self.job_code(stage=self.stage, grid=self.grid_iteration, seed=self.branch_id)
        log_dir  = Path(super().get_log_file_dir()).resolve()
        log_file = log_dir / f"{job_code}.log"
        candidate_paths.add(log_file)

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

    def _run_powheg(self, job_code: str, task_id: int, apptainer_image: str=None):
        """Execute POWHEG for this task."""
        if apptainer_image:
            self.publish_message(f"Running POWHEG in container: {apptainer_image}")
            cmd = ["apptainer", "exec", str(apptainer_image)]
        else:
            cmd = ["exec", ]

        pwhg_main = self.config['powheg_executable']

        log_dir = Path(super().get_log_file_dir()).resolve()
        log_file = log_dir / f"{job_code}.log"

        def call_job_script(script, job_code, task_id, program, log_file=""):
            """
            Call the pwhg_run.sh script with the correct arguments.

            script, job_code, task_id, program, log_file=""
            """
            inputs     = [script, job_code, task_id, program, log_file]
            cmd_script = [str(i) for i in inputs if i]  # Convert to strings and filter out empty log_file

            return cmd_script

        cmd += call_job_script(Path(self.config["script_dir"]) / "pwhg_run.sh", job_code, task_id, pwhg_main, log_file)

        self.publish_message(f"Executing command: {' '.join(cmd)} in {str(self.config['job_settings']['run_dir'])}")
        self.publish_message(f"Logging output to: {log_file}")
        with open(log_file, 'w') as logf:
            # Write start time to log file. E.g. <Started-POWHEG:p1-x1-s1=2026-07-20T08:10:05.858527+00:00>.
            logf.write(f"<Started-POWHEG:{job_code}={datetime.now(ZoneInfo('Europe/Berlin')).isoformat()}>\n")

        try:
            subprocess.run(
                cmd,
                cwd=self.config['job_settings']['run_dir'],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"POWHEG failed: {e}")
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
        log_prefix = f"{self.stage_code(self.stage, self.grid_iteration)}-s"
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
        total_cpu_hours        = total_absolute_seconds / 3600.0

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
