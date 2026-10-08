

# class POWHEGStage3GridCombineSetup(POWHEGBaseTask):
#     """
#     Setup task for Stage 3 Grid Combination: Creates input file and container.
#     All grid combination tasks depend on this completing first.
#     """

#     @property
#     def stage_name(self) -> str:
#         return "stage3_gridcombine"

#     def requires(self):
#         """Depends on Stage 2."""
#         return POWHEGStage2(config_file=self.config_file, version=self.version)

#     def output(self):
#         return self.local_target("stage3_gridcombine_setup.done")

#     def run(self):
#         """Setup container and create input file."""
#         self.setup_container()
#         job_code = self.generate_job_code(stage=3, grid=1, seed=1)
#         self.create_powheg_input(stage=3, grid=1)
#         self.setup_seeds_file()
#         self.output().touch()


# class POWHEGStage3GridCombine(POWHEGWorkflowTask):
#     """
#     Stage 3 Grid Combination: Run 3 seeds on single node with /tmp.

#     This is a special phase that runs only 3 seeds to combine the grid
#     before the full Stage 3 event generation.
#     """

#     @property
#     def stage_name(self) -> str:
#         return "stage3_gridcombine"

#     def create_branch_map(self):
#         """Only 3 seeds for grid combination."""
#         return {i: i for i in range(3)}

#     def workflow_requires(self):
#         """Depends on setup task."""
#         return {"stage3_gridcombine_setup": POWHEGStage3GridCombineSetup(config_file=self.config_file, version=self.version)}

#     def requires(self):
#         return self.workflow_requires()

#     def output(self):
#         return self.local_target(f"seed_{self.branch}.done")

#     def run(self):
#         seed = self.branch_data
#         job_code = f"s{seed + 1}-p3-gridcombine"

#         self.publish_message(f"Running Stage 3 grid combination, seed {seed}")
#         self._run_powheg(job_code, seed)
#         self.output().touch()

#     def _run_powheg(self, job_code: str, seed: int):
#         """Execute POWHEG for this task."""
#         pwhg_main = self.config.config['powheg_executable']

#         env = os.environ.copy()
#         env['SLURM_ARRAY_TASK_ID'] = str(seed)
#         env['SLURM_PROCID'] = str(seed)

#         # Create log file for output
#         log_file = self.config.cwd / f"{job_code}-seed{seed}.log"

#         try:
#             with open(log_file, 'w') as logf:
#                 wrapped_cmd = self.container_manager.build_command([pwhg_main])
#                 result = subprocess.run(
#                     wrapped_cmd,
#                     cwd=self.config.cwd,
#                     env=env,
#                     stdout=logf,
#                     stderr=subprocess.STDOUT,
#                     check=True
#                 )
#         except subprocess.CalledProcessError as e:
#             raise RuntimeError(f"POWHEG failed: {e}")


# class POWHEGStage3Setup(POWHEGBaseTask):
#     """
#     Setup task for Stage 3: Creates input file and container.
#     All Stage 3 parallel tasks depend on this completing first.
#     """

#     @property
#     def stage_name(self) -> str:
#         return "stage3"

#     def requires(self):
#         """Depends on Stage 2 or Stage 3 Grid Combination."""
#         stage3_config = self.config.config['stages'].get('stage3', {})

#         if stage3_config.get('grid_combination', False):
#             return POWHEGStage3GridCombine(config_file=self.config_file, version=self.version)
#         else:
#             return POWHEGStage2(config_file=self.config_file, version=self.version)

#     def output(self):
#         return self.local_target("stage3_setup.done")

#     def run(self):
#         """Setup container and create input file."""
#         self.setup_container()
#         job_code = self.generate_job_code(stage=3, grid=1, seed=1)
#         self.create_powheg_input(stage=3, grid=1)
#         self.setup_seeds_file()
#         self.output().touch()


# class POWHEGStage3(POWHEGWorkflowTask):
#     """Stage 3: Event generation."""

#     @property
#     def stage_name(self) -> str:
#         return "stage3"

#     def create_branch_map(self):
#         resources = self.config.get_resources('stage3')
#         ntasks = resources.get('ntasks', 100)
#         return {i: i for i in range(ntasks)}

#     def workflow_requires(self):
#         """Depends on setup task."""
#         return {"stage3_setup": POWHEGStage3Setup(config_file=self.config_file, version=self.version)}

#     def requires(self):
#         return self.workflow_requires()

#     def output(self):
#         return self.local_target(f"task_{self.branch}.done")

#     def run(self):
#         task_id = self.branch_data
#         job_code = self.generate_job_code(stage=3, grid=1, seed=task_id)

#         self.publish_message(f"Running Stage 3, task {task_id}")
#         self._run_powheg(job_code, task_id)
#         self.output().touch()

#     def _run_powheg(self, job_code: str, task_id: int):
#         """Execute POWHEG for this task."""
#         pwhg_main = self.config.config['powheg_executable']

#         env = os.environ.copy()
#         env['SLURM_ARRAY_TASK_ID'] = str(task_id)
#         env['SLURM_PROCID'] = str(task_id)

#         # Create log file for output
#         log_file = self.config.cwd / f"{job_code}-task{task_id}.log"

#         try:
#             with open(log_file, 'w') as logf:
#                 wrapped_cmd = self.container_manager.build_command([pwhg_main])
#                 result = subprocess.run(
#                     wrapped_cmd,
#                     cwd=self.config.cwd,
#                     env=env,
#                     stdout=logf,
#                     stderr=subprocess.STDOUT,
#                     check=True
#                 )
#         except subprocess.CalledProcessError as e:
#             raise RuntimeError(f"POWHEG failed: {e}")


# class POWHEGStage4Setup(POWHEGBaseTask):
#     """
#     Setup task for Stage 4: Creates input file and container.
#     All Stage 4 parallel tasks depend on this completing first.
#     """

#     @property
#     def stage_name(self) -> str:
#         return "stage4"

#     def requires(self):
#         """Depends on Stage 3."""
#         return POWHEGStage3(config_file=self.config_file, version=self.version)

#     def output(self):
#         return self.local_target("stage4_setup.done")

#     def run(self):
#         """Setup container and create input file."""
#         self.setup_container()
#         job_code = self.generate_job_code(stage=4, grid=1, seed=1)
#         self.create_powheg_input(stage=4, grid=1)
#         self.setup_seeds_file()
#         self.output().touch()


# class POWHEGStage4(POWHEGWorkflowTask):
#     """Stage 4: Event file output."""

#     @property
#     def stage_name(self) -> str:
#         return "stage4"

#     def create_branch_map(self):
#         resources = self.config.get_resources('stage4')
#         ntasks = resources.get('ntasks', 100)
#         return {i: i for i in range(ntasks)}

#     def workflow_requires(self):
#         """Depends on setup task."""
#         return {"stage4_setup": POWHEGStage4Setup(config_file=self.config_file, version=self.version)}

#     def requires(self):
#         return self.workflow_requires()

#     def output(self):
#         return self.local_target(f"task_{self.branch}.done")

#     def run(self):
#         task_id = self.branch_data
#         job_code = self.generate_job_code(stage=4, grid=1, seed=task_id)

#         self.publish_message(f"Running Stage 4, task {task_id}")
#         self._run_powheg(job_code, task_id)
#         self.output().touch()

#     def _run_powheg(self, job_code: str, task_id: int):
#         """Execute POWHEG for this task."""
#         pwhg_main = self.config.config['powheg_executable']

#         env = os.environ.copy()
#         env['SLURM_ARRAY_TASK_ID'] = str(task_id)
#         env['SLURM_PROCID'] = str(task_id)

#         # Create log file for output
#         log_file = self.config.cwd / f"{job_code}-task{task_id}.log"

#         try:
#             with open(log_file, 'w') as logf:
#                 wrapped_cmd = self.container_manager.build_command([pwhg_main])
#                 result = subprocess.run(
#                     wrapped_cmd,
#                     cwd=self.config.cwd,
#                     env=env,
#                     stdout=logf,
#                     stderr=subprocess.STDOUT,
#                     check=True
#                 )
#         except subprocess.CalledProcessError as e:
#             raise RuntimeError(f"POWHEG failed: {e}")


# class POWHEGAnalysisSetup(POWHEGBaseTask):
#     """
#     Setup task for Analysis: Sets up container.
#     All analysis tasks depend on this completing first.
#     """

#     @property
#     def stage_name(self) -> str:
#         return "analysis"

#     def requires(self):
#         """Depends on Stage 4."""
#         return POWHEGStage4(config_file=self.config_file, version=self.version)

#     def output(self):
#         return self.local_target("analysis_setup.done")

#     def run(self):
#         """Setup container."""
#         self.setup_container()
#         self.setup_seeds_file()
#         self.output().touch()


# class POWHEGAnalysis(POWHEGWorkflowTask):
#     """
#     Analysis stage: LHEF, PYTHIA, weight correction.
#     """

#     @property
#     def stage_name(self) -> str:
#         return "analysis"

#     def create_branch_map(self):
#         resources = self.config.get_resources('analysis')
#         ntasks = resources.get('ntasks', 100)
#         return {i: i for i in range(ntasks)}

#     def workflow_requires(self):
#         """Depends on setup task."""
#         return {"analysis_setup": POWHEGAnalysisSetup(config_file=self.config_file, version=self.version)}

#     def requires(self):
#         return self.workflow_requires()

#     def output(self):
#         return self.local_target(f"task_{self.branch}.done")

#     def run(self):
#         analysis_config = self.config.config['stages'].get('analysis', {})

#         if not analysis_config.get('enabled', False):
#             self.output().touch()
#             return

#         task_id = self.branch_data

#         # Run enabled analyses
#         if analysis_config.get('weight_correction', False):
#             self._run_weight_correction(task_id)

#         if analysis_config.get('lhef', False):
#             self._run_lhef_analysis(task_id)

#         if analysis_config.get('pythia', False):
#             self._run_pythia(task_id)

#         self.output().touch()

#     def _run_weight_correction(self, task_id: int):
#         """Run finduboundcorr weight correction."""
#         self.publish_message(f"Running weight correction, task {task_id}")

#     def _run_lhef_analysis(self, task_id: int):
#         """Run LHEF analysis."""
#         self.publish_message(f"Running LHEF analysis, task {task_id}")

#     def _run_pythia(self, task_id: int):
#         """Run PYTHIA."""
#         self.publish_message(f"Running PYTHIA, task {task_id}")


# class POWHEGAddWeightsSetup(POWHEGBaseTask):
#     """
#     Setup task for AddWeights: Sets up container.
#     All addweights tasks depend on this completing first.
#     """

#     @property
#     def stage_name(self) -> str:
#         return "addweights"

#     def requires(self):
#         """Depends on Stage 4."""
#         return POWHEGStage4(config_file=self.config_file, version=self.version)

#     def output(self):
#         return self.local_target("addweights_setup.done")

#     def run(self):
#         """Setup container."""
#         self.setup_container()
#         self.setup_seeds_file()
#         self.output().touch()


# class POWHEGAddWeights(POWHEGWorkflowTask):
#     """
#     Addweights stage: Add reweighting to existing event files.
#     """

#     @property
#     def stage_name(self) -> str:
#         return "addweights"

#     def create_branch_map(self):
#         resources = self.config.get_resources('addweights')
#         ntasks = resources.get('ntasks', 100)
#         return {i: i for i in range(ntasks)}

#     def workflow_requires(self):
#         """Depends on setup task."""
#         return {"addweights_setup": POWHEGAddWeightsSetup(config_file=self.config_file, version=self.version)}

#     def requires(self):
#         return self.workflow_requires()

#     def output(self):
#         return self.local_target(f"seed_{self.branch}.done")

#     def run(self):
#         addweights_config = self.config.config['stages'].get('addweights', {})

#         if not addweights_config.get('enabled', False):
#             self.output().touch()
#             return

#         seed = self.branch_data
#         rwl_file = addweights_config.get('rwl_file', 'newweights.xml')
#         lhe_dir = addweights_config.get('lhe_dir', '.')

#         self.publish_message(f"Running addweights, seed {seed}")
#         self._run_addweights(seed, rwl_file, lhe_dir)
#         self.output().touch()

#     def _run_addweights(self, seed: int, rwl_file: str, lhe_dir: str):
#         """Execute addweights for a seed."""
#         pwhg_main = self.config.config['powheg_executable']

#         env = os.environ.copy()
#         env['SLURM_ARRAY_TASK_ID'] = str(seed)
#         env['SLURM_PROCID'] = str(seed)

#         self.container_manager.run(
#             [pwhg_main],
#             cwd=self.config.cwd,
#             env=env,
#             check=True
#         )

