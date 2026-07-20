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
from pathlib import Path
from typing import Dict, Any, Optional, List

# from .config import POWHEGConfig

# Import our framework classes
from .framework import POWHEGBaseTask, POWHEGWrapper_template
from .config import POWHEGConfig
from .cli import set_b2luigi_settings


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

    def requires(self):
        """Depend on presubmit task."""
        yield POWHEGPresubmit(version=self.version, config=self.config)

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

        with open(dest_file, "w") as file:
            file.write("".join(new_lines))

    @staticmethod
    def parse_template_file(src: Path, dest: Path, replacements: Dict[str, Any]):
        """Parse template file with Python % formatting."""
        # FIXME: Do we still need this function? It isn't used anywhere in the current codebase.
        with open(src, 'r') as f:
            content = f.read()
        content = content % replacements
        with open(dest, 'w') as f:
            f.write(content)

    # def get_scripts(self):
    #     """Copy scripts to the output directory."""
    #     for script in ["pwhg_run.sh"]:
    #         src  = self.config["script_dir"] / script
    #         dest = self.local_path(script)
    #         shutil.copy2(src, dest)


class POWHEGcreateSymlinks(POWHEGBaseTask):
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
            yield POWHEGStageWrapper(stage=self.get_stage_str(1), grid_iteration=self.grid_iteration - 1, version=self.version, config=self.config)
        elif self.stage_number == 2:
            # Depend on Stage 1 for Stage 2
            stages_config    = self.config['stages']
            max_grid_iterations = stages_config['stage1'].get('grid_iterations', 3)
            yield POWHEGStageWrapper(stage=self.get_stage_str(1), grid_iteration=max_grid_iterations, version=self.version, config=self.config)
        elif self.stage_number > 2:
            # Depend on previous stage for stage > 2
            yield POWHEGStageWrapper(stage=self.get_stage_str(self.stage_number - 1), version=self.version, config=self.config)


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

    branch_id = b2luigi.IntParameter(
        default=0,
        description="Branch ID for this parallel POWHEG execution (0-indexed)"
    )

    @property
    def task_cmd_additional_args(self) -> List[str]:
        """
        Additional command-line arguments for the task.
        Necessary for the batch submission.
        """
        return ["--config_file", str(self.config["config_file"])]  # Pass config file to each task

    @property
    def batch_system(self) -> str:
        if self.config["stages"][self.stage]["resources"]["cluster"] == "local":
            return "local"
        return "slurm"

    @property
    def job_name(self) -> str:
        return f"{self.config['job_settings']['job_name']}_{self.stage_name}_s{self.branch_id}"

    @property
    def slurm_settings(self) -> Dict[str, Any]:
        if self.config["stages"][self.stage]["resources"]["cluster"] == "local":
            return {}

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
        yield POWHEGcreateSymlinks(
            stage          = self.stage,
            grid_iteration = self.grid_iteration,
            version        = self.version,
            config         = self.config
        )
        if self.stage_number == 1 and self.grid_iteration > 1:
            # Depend on previous grid iteration for Stage 1
            yield POWHEGStageWrapper(stage=self.get_stage_str(1), grid_iteration=self.grid_iteration - 1, version=self.version, config=self.config)
        elif self.stage_number == 2:
            # Depend on Stage 1 for Stage 2
            stages_config       = self.config['stages']
            max_grid_iterations = stages_config['stage1'].get('grid_iterations', 3)
            yield POWHEGStageWrapper(stage=self.get_stage_str(1), grid_iteration=max_grid_iterations, version=self.version, config=self.config)
        elif self.stage_number > 2:
            # Depend on previous stage for stage > 2
            yield POWHEGStageWrapper(stage=self.get_stage_str(self.stage_number - 1), version=self.version, config=self.config)

    def output(self):
        """
        Output targets for POWHEG execution:
            For stage 1, there are two possible outputs:
            The xgrid 'pwg-xg?-xgrid-btl-????.dat' file and the statistics 'pwg-????-xg?-stat.dat' file.
            Depending on the implementation (ggHH does weird stuff here) 'pwggridinfo-[btl|rmn]-xg?-?.dat'
            files are produced instead.

            # FIXME: Do we always have a rmn file for which we should check?

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

    def _run_powheg(self, job_code: str, task_id: int, apptainer_image: str=None):
        """Execute POWHEG for this task."""
        if apptainer_image:
            self.publish_message(f"Running POWHEG in container: {apptainer_image}")
            cmd = ["apptainer", "exec", apptainer_image]
        else:
            cmd = ["exec", ]

        pwhg_main = self.config['powheg_executable']

        log_dir = Path(self.get_log_file_dir()).resolve()
        log_file = log_dir / f"{job_code}.log"

        def call_job_script(script, job_code, task_id, program, log_file):
            """Call the pwhg_run.sh script with the correct arguments."""
            # cmd_script  = [str(script), job_code, task_id, program, str(log_file)]
            cmd_script  = [str(script), job_code, task_id, program]
            return cmd_script

        cmd += call_job_script(Path(self.config["script_dir"]) / "pwhg_run.sh", job_code, task_id, pwhg_main, log_file)
        # cmd += [pwhg_main,]
        cmd = list(map(str, cmd))  # Ensure all parts are strings

        self.publish_message(f"Executing command: {' '.join(cmd)} in {self.config['job_settings']['run_dir']}")
        self.publish_message(f"Logging output to: {log_file}")
        with open(log_file, 'w') as logf:
            # Write start time to log file. E.g. <Started-POWHEG:p1-x1-s1=2026-07-20T08:10:05.858527+00:00>.
            logf.write(f"<Started-POWHEG:{job_code}={datetime.now(timezone.utc).isoformat()}>\n")

        with open(log_file, 'a') as logf:
            try:
                subprocess.run(
                    cmd,
                    cwd=self.config['job_settings']['run_dir'],
                    stdout=logf,
                    stderr=subprocess.STDOUT,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"POWHEG failed: {e}")
            finally:
                logf.write(f"<Finished-POWHEG:{job_code}={datetime.now(timezone.utc).isoformat()}>\n")


class POWHEGStageWrapper(POWHEGWrapper_template):
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
class POWHEGWorkflow(POWHEGWrapper_template):
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
            yield POWHEGStageWrapper(stage=self.get_stage_str(4), version=self.version, config=self.config)
        elif stages_config['stage3']['enabled']:
            yield POWHEGStageWrapper(stage=self.get_stage_str(3), version=self.version, config=self.config)
        elif stages_config['stage2']['enabled']:
            yield POWHEGStageWrapper(stage=self.get_stage_str(2), version=self.version, config=self.config)
        elif stages_config['stage1']['enabled']:
            max_grid_iterations = stages_config["stage1"]["grid_iterations"]
            yield POWHEGStageWrapper(stage=self.get_stage_str(1), grid_iteration=max_grid_iterations, version=self.version, config=self.config)


class POWHEGWorkflow_multiple_configs(POWHEGWrapper_template):
    configuration_files = b2luigi.ListParameter(hashed=True, description="List of POWHEG configuration files to run.")

    def requires(self):
        for config_file in self.configuration_files:
            config = POWHEGConfig.from_yaml(config_file)

            set_b2luigi_settings(cwd         = config["cwd"],
                                 run_dir     = config["job_settings"]["run_dir"],
                                 python      = config["python"])

            config_dict = config.to_dict()  # Convert to dictionary for serialization

            yield POWHEGWorkflow(version=self.version, config=config_dict)
