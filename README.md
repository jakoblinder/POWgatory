# POWgatory

POWgatory is a [b2luigi](https://b2luigi.readthedocs.io/)-based workflow manager for running multi-stage [POWHEG-BOX](https://powhegbox.mib.infn.it/) NLO event-generation campaigns on a Slurm cluster (or locally, for testing). A single YAML configuration file describes the whole run; POWgatory expands it into a dependency graph of per-seed batch jobs, submits and tracks them, and aggregates timing information as stages complete.

This document is the single reference for installing, configuring, and running POWgatory, and for understanding what is and isn't currently implemented.

## Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Pipeline architecture](#pipeline-architecture)
- [Configuration reference](#configuration-reference)
- [Batch submission details](#batch-submission-details)
- [Container support](#container-support)
- [CLI reference](#cli-reference)
- [Run directory layout](#run-directory-layout)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)
- [License](#license)

## Overview

A POWHEG-BOX calculation runs in several sequential stages (grid generation, integration, event generation, ...), each of which needs to be split into many parallel seeded jobs and re-joined before the next stage can start. POWgatory automates that:

- One `run.yaml` file per campaign describes the POWHEG executable, the stages to run, per-stage resource requests, and POWHEG parameter overrides.
- b2luigi resolves the task dependency graph and submits per-seed jobs to Slurm (or runs them as local subprocesses for testing), including grouping many seeds into a single array or multi-node job where configured.
- Each stage's completion is detected from the actual POWHEG output files it produces, and per-seed run times are parsed from job logs and recorded in a `timings.yaml` in the run directory.
- Optional Apptainer/Singularity container execution and a one-off pre-submission shell hook are supported.

## Requirements

- Python 3.10+ (the codebase and its dependencies use modern Python features; `setup.py` currently advertises `>=3.7`, which is stale — see [Known limitations](#known-limitations)).
- [`b2luigi`](https://b2luigi.readthedocs.io/) and `luigi` (installed automatically as dependencies).
- A Slurm cluster for anything beyond local testing (`sinfo`, `sbatch`, `squeue`, `scancel` must be on `PATH`).
- [Apptainer](https://apptainer.org/) if you want containerized execution (optional).
- A working POWHEG-BOX build (`pwhg_main` or equivalent) and its input/seed templates.

**Important:** `submission_type: mpi` (multi-node Slurm jobs, see [Batch submission details](#batch-submission-details)) requires a version of `b2luigi` that implements MPI-style submission. This support is not yet part of any released `b2luigi` version — if you see an `Unknown submission type` error, you need the development checkout of `b2luigi` that added it, not the PyPI package pinned in `setup.py`.

## Installation

```bash
git clone <repo-url> POWgatory
cd POWgatory

# Editable install is recommended so changes to powheg_workflow/ take effect immediately
pip install --user -e .

# Verify
powgatory --version
```

This installs the `powgatory` console script (`powheg_workflow.cli:run_workflow`) and the `powheg_workflow` Python package.

If `powgatory` isn't found afterwards, make sure your user site-packages `bin` directory is on `PATH`:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

## Quick start

```bash
# Set up a run directory next to your POWHEG build
mkdir my_run && cd my_run
cp /path/to/powheg-box/pwhg_main .
cp /path/to/powheg-box/powheg.input-save .
cp /path/to/powheg-box/pwgseeds.dat-save .   # optional: falls back to a bundled default

# Start from the packaged default configuration and edit it
cp <repo>/config/config_default.yaml run.yaml
$EDITOR run.yaml

# Submit the workflow
powgatory -c run.yaml
```

`powgatory` loads `run.yaml`, resolves it against the packaged defaults, validates the stage/resource configuration (printing warnings for inconsistent setups), and hands the resulting task graph to b2luigi/Slurm.

## Pipeline architecture

Each stage is executed by a fixed sequence of internal b2luigi tasks (`powheg_workflow/tasks.py`):

1. **`POWHEGPresubmit`** — runs `job_settings.presubmit` once, locally, before anything else (skipped if empty).
2. **`POWHEGStageSetup`** — writes that stage's `powheg.input` by copying `powheg_input_template` and patching in the stage's `powheg_parameters` (values are rewritten in place; unrecognized parameters are appended with a comment).
3. **`POWHEGcreateSymlinks`** — symlinks `powheg.input` and `pwgseeds.dat` (from `powheg_seeds_template`) into the run directory for that stage.
4. **`POWHEGStage`** — the actual POWHEG execution, one instance per parallel branch (seed). This is the task that carries all Slurm/container/local-execution settings and is fanned out by `POWHEGStageWrapper` across `resources.ntasks` branches.
5. **`POWHEGStageTimings`** — once all branches for a stage/grid-iteration are done, scans their job logs for start/finish markers, records per-seed timing statistics into `timings.yaml`, and is what the next stage actually depends on (i.e. it is the join point of the DAG, not an optional side report).

Stage 1 (grid generation) runs `grid_iterations` times sequentially; stage 2 waits for all of stage 1's iterations; stages 3 and 4 each wait for the previous stage. `POWHEGWorkflow` requires only the highest enabled stage's `POWHEGStageTimings`, since everything before it is pulled in transitively.

```
POWHEGStage1(grid=1) → POWHEGStage1(grid=2) → POWHEGStage1(grid=3)
                                                      │
                                                      ▼
                                               POWHEGStage2
                                                      │
                                                      ▼
                                               POWHEGStage3
                                                      │
                                                      ▼
                                               POWHEGStage4
```

`POWHEGWorkflow_multiple_configs` lets a single CLI invocation submit several independent `run.yaml`s at once.

A failed POWHEG run inside `POWHEGStage` does not currently fail the b2luigi task — the subprocess error is logged via `publish_message()` and swallowed, so `POWHEGStage.complete()` (which checks for the presence of the expected output files, not an exit code) is the actual source of truth for whether a branch succeeded. Stale output files from a previous failed/killed attempt are moved into `incomplete_run_backups/` before each run so POWHEG (which refuses to overwrite an existing event file) doesn't choke on them.

## Configuration reference

`run.yaml` is merged against `config/config_default.yaml`; any key you omit falls back to the packaged default. The default file, reproduced here as the schema reference:

```yaml
powheg_executable: "./pwhg_main"
powheg_input_template: "powheg.input-save"
powheg_seeds_template: "pwgseeds.dat-save"   # optional; falls back to a bundled seeds file

job_settings:
  job_name: "powheg"
  mail_type: "None"
  mail_user: ""
  run_dir: "powheg_output"          # where POWHEG runs and writes output
  log_dir: "powheg_output/logs"     # where job logs are written
  exclude_nodes: ""                 # comma-separated list of nodes to exclude
  container_image: ""               # registry URL (oras://...), local .sif path, or empty for no container
  presubmit: ""                     # shell command run once before the workflow starts

stages:
  stage1:
    enabled: true
    grid_iterations: 3
    resources: {batch_system: slurm, submission_type: array, ntasks: 1, time: "24:00:00", partition: alma}
    powheg_parameters: {ncall1: 500}
  stage2:
    enabled: true
    resources: {ntasks: 3, time: "24:00:00", partition: alma}
  stage3:
    enabled: true
  stage4:
    enabled: true

resources:                          # global fallback; any key missing from a stage's own `resources` is taken from here
  batch_system: slurm                # "local" (GNU-parallel-style subprocess execution) or "slurm"
  submission_type: array             # Slurm only: "single", "array", or "mpi"
  partition: alma
  ntasks: 1
  time: "24:00:00"
  mem: 4000

powheg_parameters:                  # global fallback for per-stage powheg_parameters
  # ncall1: 20000
  # ncall2: 20000
  # nubound: 10000
  # numevts: 1000
```

Notes:

- `resources` fields are merged **key by key**, not block-by-block: a stage that only overrides `ntasks` still inherits `batch_system`, `submission_type`, `partition`, `time`, and `mem` from the global block.
- `batch_system: local` ignores `partition` and runs each branch as a local subprocess instead of submitting to Slurm (useful for testing without cluster access).
- `container_image` accepts a registry URL (pulled once into `<run_dir>/powheg.sif` via `apptainer pull`), a local file path (copied into the run directory), or an empty string to run without a container.
- The config file also defines `stage3.grid_combination`, a `stage3_gridcombine` stage, and `analysis`/`addweights` stages. **These are configuration placeholders only** — see [Known limitations](#known-limitations).

## Batch submission details

Per-stage Slurm behavior is controlled by `resources.submission_type`:

- **`single`** — one independent `sbatch` submission per branch.
- **`array`** — all branches for a stage submitted as one Slurm job array.
- **`mpi`** — all branches submitted as a single multi-node Slurm job, dispatched via `srun`/`$SLURM_PROCID`. Sizing information (nodes, CPUs, memory, time limit) for the stage's partition is queried live from `sinfo` at config-load time (not hand-maintained per-cluster config files) and exposed via `POWHEGStage.partition_info`.

`POWHEGConfig` also uses this live `sinfo` data to warn (not fail) if a stage's requested `resources.time` exceeds its partition's actual Slurm time limit.

Job submission is deliberately throttled: `cli.py` monkey-patches b2luigi's `SlurmProcess.start_job` to enforce a minimum interval between `sbatch` calls, to avoid overwhelming the scheduler when submitting large numbers of jobs.

Use `--workers` to control how many jobs b2luigi keeps submitted/running concurrently (default 250) — this replaced an earlier per-cluster `max_parallel_jobs` setting.

## Container support

If `job_settings.container_image` is set, `POWHEGStage` runs POWHEG inside Apptainer:

- A registry reference (anything containing `://`, e.g. `oras://...`) is pulled once into `<run_dir>/powheg.sif`.
- A local path is copied into `<run_dir>/powheg.sif`.
- An existing `<run_dir>/powheg.sif` is reused as-is.
- An empty value runs POWHEG directly, with no container.

Only the actual POWHEG-execution task (`POWHEGStage`) runs inside the container; setup/symlink/timing tasks always run locally as plain Python.

## CLI reference

```
powgatory -c run.yaml [run2.yaml ...] [OPTIONS]

Options:
  -c, --config_file FILE [FILE ...]   Path to one or more run.yaml files (required)
  --workers N                         Number of parallel b2luigi workers (default: 250)
  --dry-run                           Print what would be submitted, without submitting
  -v, --verbose                       Print the resolved configuration file path(s)
  --version                           Print the installed version and exit
```

Any additional arguments are passed through to b2luigi/luigi.

## Run directory layout

Everything happens inside `job_settings.run_dir` (`powheg_output/` by default):

- `bootstrap.sh` — generated per-run from `config/bootstrap.sh`; sourced at the start of every batch job. Prints diagnostic job info and sources an optional `<run_dir>/.env` for user environment setup (module loads, library paths, etc.).
- `task_files/` — b2luigi's generated submission scripts.
- `logs/<job_code>/` — POWHEG stdout/stderr per branch, bracketed with `<Started-POWHEG:...>`/`<Finished-POWHEG:...>` markers that `POWHEGStageTimings` parses.
- `timings.yaml` — per-stage timing statistics (shortest/longest/average/total), updated as each stage completes.
- `incomplete_run_backups/` — output files quarantined from a previous failed or killed attempt.
- POWHEG's own output files (`pwg*.dat`, `pwgevents-*.lhe`, ...) and the `powheg.input`/`pwgseeds.dat` symlinks, written directly into the run directory.

## Troubleshooting

- **`powgatory: command not found`** — check `pip show powheg-workflow` and that your Python user-bin directory is on `PATH`.
- **A stage silently doesn't run** — check the validation warnings printed at startup; they catch the common cases (e.g. a later stage enabled while its prerequisite is disabled).
- **`Unknown submission type: mpi`** — your installed `b2luigi` doesn't yet support MPI-style submission; see [Requirements](#requirements).
- **Container pull fails** — `setup_container()` shells out to `apptainer pull`; check that `apptainer` is on `PATH` on the submission host and that the registry URL is reachable.
- **A branch seems stuck / never completes** — `POWHEGStage.complete()` is based on POWHEG's own output files, not an exit code, so check the branch's log under `logs/<job_code>/` for the actual failure; a failed `run()` currently does not surface as a b2luigi task failure.

## Known limitations

This reflects the current state of the code, so gaps are documented rather than hidden:

- `stage3.grid_combination`, the `stage3_gridcombine` stage, and the `analysis`/`addweights` stages are all present in the configuration schema but have **no corresponding task implementation** — enabling them has no effect. `powheg_workflow/scrap.py` contains an old, unused draft of this work and is not imported anywhere.
- `setup.py` has a few stale details left over from earlier iterations of the project: it registers the `powgatory` console script but the CLI's own `--help` text still says `powheg-workflow`; its `classifiers` claim an Apache license while the repository ships GPLv3 (see below); and its `package_data` references `config/clusters/` and `config/scripts/`, neither of which exist in the current layout.
- `submission_type: mpi` depends on unreleased `b2luigi` functionality (see [Requirements](#requirements)).

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE).
