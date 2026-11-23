# POWgatory

POWgatory is a [b2luigi](https://b2luigi.readthedocs.io/)-based workflow manager for running multi-stage [POWHEG-BOX](https://powhegbox.mib.infn.it/) NLO event-generation campaigns on a Slurm cluster (or locally, for testing). A single YAML configuration file describes the whole run; POWgatory expands it into a dependency graph of per-seed batch jobs, submits and tracks them, and aggregates timing information as stages complete.

This document is the single reference for installing, configuring, and running POWgatory, and for understanding what is and isn't currently implemented.

> [!TIP]
> Using Claude Code, e.g. in VS Code? Install the POWgatory skill once, and Claude knows how to set up and run POWgatory: see [Using POWgatory with Claude Code](#using-powgatory-with-claude-code).

## Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Pipeline architecture](#pipeline-architecture)
- [Grid combination](#grid-combination)
- [Analysis stages](#analysis-stages)
- [Merging histograms](#merging-histograms)
- [Configuration reference](#configuration-reference)
- [Batch submission details](#batch-submission-details)
- [Container support](#container-support)
- [CLI reference](#cli-reference)
- [Using POWgatory with Claude Code](#using-powgatory-with-claude-code)
- [Run directory layout](#run-directory-layout)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)
- [License](#license)

## Overview

A POWHEG-BOX calculation runs in several sequential stages (grid generation, integration, event generation, ...), each of which needs to be split into many parallel seeded jobs and re-joined before the next stage can start. POWgatory automates that:

- One `run.yaml` file per campaign describes the POWHEG executable, the stages to run, per-stage resource requests, and POWHEG parameter overrides.
- b2luigi resolves the task dependency graph and submits per-seed jobs to Slurm (or runs them as local subprocesses for testing), including grouping many seeds into a single array or multi-node job where configured.
- Each stage's completion is detected from the actual POWHEG output files it produces, and per-seed run times are parsed from job logs and recorded in a `timings.yaml` in the run directory.
- Optional analysis stages run the LHEF analysis and the PYTHIA shower on every stage-4 event file, with POWHEG's upper bound weight correction.
- Optional Apptainer/Singularity container execution and a one-off pre-submission shell hook are supported.

## Requirements

- Python 3.11 to 3.14 (3.11+ is required by the `b2luigi` version below).
- [`b2luigi`](https://b2luigi.readthedocs.io/) with Slurm array/MPI submission support (see below) and `luigi`, both installed automatically.
- `git` on the installing machine, since `b2luigi` and `luigi` are installed directly from GitHub.
- A Slurm cluster for anything beyond local testing (`sinfo`, `sbatch`, `squeue`, `scancel` must be on `PATH`).
- [Apptainer](https://apptainer.org/) if you want containerized execution (optional).
- A working POWHEG-BOX build (`pwhg_main` or equivalent) and its input/seed templates.

**About `b2luigi`:** `submission_type: array` (the default) and `submission_type: mpi` (see [Batch submission details](#batch-submission-details)) need the `submission_type` setting for Slurm. No released `b2luigi` version has it yet; it is proposed upstream in [belle2/b2luigi#6](https://github.com/belle2/b2luigi/pull/6). `pyproject.toml` installs `b2luigi` from the `powgatory` branch of [jakoblinder/b2luigi](https://github.com/jakoblinder/b2luigi/tree/powgatory), which combines that pull request with a second unreleased change: allowing `tenacity` 9, which the pinned `luigi` needs. With a PyPI `b2luigi`, `submission_type` would be silently ignored and grouped branches would not be submitted as arrays.

## Installation

Install into a virtual environment on a filesystem that the batch nodes can see. Batch jobs run with the same Python interpreter as the `powgatory` command that submitted them.

```bash
python3.14 -m venv /path/to/venv    # any Python 3.11-3.14; the system python3 may be older
source /path/to/venv/bin/activate

pip install git+https://github.com/jakoblinder/POWgatory.git
# or, from a clone:  pip install /path/to/POWgatory

# Verify
powgatory --version
```

This installs:
- the `powgatory` command;
- the `powgatory` Python package, including its default configuration (`powgatory/config/`) and job script (`powgatory/scripts/`);
- `b2luigi` from the fork's `powgatory` branch;
- `luigi` from a pinned commit on GitHub, because no `luigi` release supports Python 3.14 yet.

### Updating `b2luigi`

`pyproject.toml` points at the fork's `powgatory` branch on GitHub. pip does not re-fetch a branch by itself, so after new commits are pushed to the fork, reinstall POWgatory together with all its dependencies:

```bash
pip install --force-reinstall git+https://github.com/jakoblinder/POWgatory.git
# or, from a clone:  pip install --force-reinstall /path/to/POWgatory
```

Reinstall POWgatory, not `b2luigi` alone. pip only applies POWgatory's pinned `luigi` when POWgatory is part of the command. On its own, `b2luigi` gets `luigi` from PyPI, which on Python 3.14 is the old 3.6.0 that fails on import. Do not add `--no-deps` either: new commits of the fork can bring new dependencies (e.g. `xrootd`, which upstream `b2luigi` now requires), and `import b2luigi` fails without them.

### Development setup

To develop POWgatory, install it and the `b2luigi` fork from local checkouts in editable mode, POWgatory first:

```bash
pip install -e /path/to/POWgatory
pip install -e /path/to/b2luigi    # checkout of the powgatory branch
```

Use the fork's `powgatory` branch for this. It combines the Slurm array/MPI submission support from the `slurm_array_submission` branch with the change from the `allow_tenacity_9` branch, which allows `tenacity` 9, as required by the pinned `luigi`.

## Quick start

```bash
# Set up a run directory next to your POWHEG build
mkdir my_run && cd my_run
cp /path/to/powheg-box/pwhg_main .
cp /path/to/powheg-box/powheg.input-save .
cp /path/to/powheg-box/pwgseeds.dat-save .   # optional: falls back to a bundled default

# Write run.yaml from your template (or the packaged example) and edit it
powgatory --init
$EDITOR run.yaml

# Submit the workflow
powgatory -c run.yaml
```

`powgatory` loads `run.yaml`, resolves it against the packaged defaults, validates the stage/resource configuration (printing warnings for inconsistent setups), and hands the resulting task graph to b2luigi/Slurm.

## Pipeline architecture

Each stage is executed by a fixed sequence of internal b2luigi tasks (`powgatory/tasks.py`):

1. **`POWHEGPresubmit`** — runs `job_settings.presubmit` once, locally, before anything else (skipped if empty).
2. **`POWHEGStageSetup`** — writes that stage's `powheg.input` by copying `powheg_input_template` and patching in the stage's `powheg_parameters` (values are rewritten in place; unrecognized parameters are appended with a comment).
3. **`POWHEGCreateSymlinks`** — symlinks `powheg.input` and `pwgseeds.dat` (from `powheg_seeds_template`) into the run directory for that stage.
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
                                   (stage3_fullgrid, optional, see below)
                                                      │
                                                      ▼
                                               POWHEGStage3
                                                      │
                                                      ▼
                                               POWHEGStage4
```

The analysis stages `lhef` and `pythia` (see [Analysis stages](#analysis-stages)) both wait for stage 4. At the end, the histogram files of stage 2, `lhef` and `pythia` are merged over all seeds (see [Merging histograms](#merging-histograms)); `POWHEGWorkflow` requires these merges for every enabled stage among them.

`POWHEGMultiConfigWorkflow` lets a single CLI invocation submit several independent `run.yaml`s at once.

A failed POWHEG run inside `POWHEGStage` (non-zero exit code) raises a `RuntimeError`, which fails that branch's b2luigi task. A branch that exits cleanly but whose output files never appear also fails, after a short polling window that allows for slow cluster filesystems. `POWHEGStage.complete()` checks for the expected POWHEG output files, not an exit code; both 4-digit (`-0042`) and 5-digit (`-12345`) seed suffixes are accepted, so runs with more than 9999 seeds work. Stale output files from a previous failed/killed attempt are moved into `incomplete_run_backups/` before each run so POWHEG (which refuses to overwrite an existing event file) doesn't choke on them.

## Grid combination

At the start of stage 3, POWHEG combines the stage-2 grids of each contribution (btilde, remn, reg) into a fullgrid file. One seed does this per contribution, while all other seeds wait for the fullgrid files. In POWHEG-BOX-RES, seed k starts with contribution k, so the first seeds combine the grids in parallel. This can take hours, and every stage-3 job then needs a time limit that covers it.

With the stage `stage3_fullgrid`, these first seeds run as a stage of their own between stage 2 and stage 3. Stage 3 runs its other seeds only afterwards, when the fullgrid files exist:

```yaml
stages:
  stage3_fullgrid:
    enabled: true
    contributions: auto        # or 1, 2, 3
    resources:
      batch_system: local      # e.g. on the login node, without a Slurm time limit
```

- The stage has its own `resources`. With `batch_system: local`, its seeds run as local processes on the node `powgatory` runs on, while all other stages stay on Slurm. Alternatively, keep Slurm and give only this stage a longer `time`.
- Its seeds are stage-3 seeds 1 to `contributions`. They run POWHEG's complete stage 3, with stage 3's `powheg_parameters`. The stage cannot have `powheg_parameters` of its own. Stage 3 runs the seeds after them, so it needs more seeds (`ntasks`) than `contributions`. The stage's own `ntasks` is not used.
- `contributions: auto` reads the number when the config is loaded, from `powheg_input_template` with stage 3's `powheg_parameters`, by POWHEG-BOX-RES's rules (`bbinit.f`):
  - btilde: unless `withbtilde 0`.
  - remn: with `hdamp > 0`, `hfact > 0`, `withdamp 1` or `bornzerodamp 1`, unless `withremnants 0`.
  - reg: unless `withregular 0`.
  - remn and reg: never with `bornonly 1`, `LOevents 1` or `virtonly 1`.

  Whether a process has regular contributions at all depends on the process, not on the input. A process without them gets one seed more than needed, which waits for the others and then does its normal stage-3 work. For other POWHEG versions, set `contributions` to 1, 2 or 3.
- Its logs are `logs/stage3_fullgrid-s<seed>.log`, and `timings.yaml` has an entry `stage3_fullgrid`.
- POWHEG writes the seed into a lock `pwg-<contribution>-fullgrid.lock` while that seed combines the contribution's grids, and deletes the lock when it exits. A seed that is killed instead (time limit, `scancel`, node failure) leaves its lock behind, and all other seeds would wait for it forever. When the killed seed runs again, POWgatory moves its own lock into `incomplete_run_backups/`. This holds for the grid combination and for stage 3. Locks of other seeds are left alone.

## Analysis stages

The stages `lhef` and `pythia` run `lhef_executable` (e.g. `lhef_analysis`) and `pythia_executable` (e.g. `main-PYTHIA82-lhef`) once per stage-4 seed, on that seed's event file. Their number of branches is stage 4's `ntasks`; all other `resources` and the `powheg_parameters` can be set per stage, as for stages 1-4. Each stage works in its own directory, `<run_dir>/lhef/` or `<run_dir>/pythia/`, so the outputs of the two analyses stay apart, and the `powheg.input` of stages 1-4 stays untouched:

```
POWHEGStageTimings(stage4) → POWHEGWeightCorrection → POWHEGAnalysisSetup(lhef) → POWHEGAnalysis(lhef) × seeds → POWHEGStageTimings(lhef)
                                                    → POWHEGAnalysisSetup(pythia) → POWHEGAnalysis(pythia) × seeds → POWHEGStageTimings(pythia)
```

- **`POWHEGWeightCorrection`** runs once, locally, if a stage has `weight_correction: true`. For every seed it computes the upper bound correction factors `ub_btilde_corr` and `ub_remn_corr` from that seed's `pwgcounters-st4-<seed>.dat`, and writes them into `<run_dir>/ub_corrections.yaml`. A factor is the cross section estimated during the event generation divided by the one used for it and by the bound violation correction factor. It exists only for an event type whose weights were corrected for upper bound violations, i.e. whose bound violation correction factor is not 1; without such corrections, the estimate differs from the cross section used only by its statistical error. This is the logic of POWHEG-BOX's `FindReweightFromCounters.py`, which is also available as `python -m powgatory.ub_correction pwgcounters-st4-*.dat`. The counters contain these numbers only with `ubexcess_correct 1` in the POWHEG input.
- **`POWHEGAnalysisSetup`** writes `<seed>-powheg.input` per seed: the template with the stage's `powheg_parameters`, like the input files of stages 1-4, plus the seed's correction factors if the stage has `weight_correction: true`. It links the event files in (`pwgevents-0001.lhe -> ../pwgevents-0001.lhe`), and the stage's `link_files`, for programs that need further files.
- **`POWHEGAnalysis`** runs the program with the seed and then the event file on stdin (`scripts/run_program.sh`). Without a `powheg.input` in their directory, the POWHEG programs take the first line as prefix of their input file, read `<seed>-powheg.input`, and multiply the event weights by the factors in it. A branch is complete when its histogram file exists and is non-empty: `pwgLHEF_analysis-<seed>.top` or `pwgoutput_py8_histos-<seed>.top`, the names in `lhef_analysis.f` and `main-PYTHIA8.f`. With several event weights (e.g. scale variations via `rwl_file`), the programs write one file per weight instead, `...-<seed>-W1.top`, `...-W2.top`, ..., and `-W1` is checked. A program that names its histograms differently needs its name pattern in `HISTOGRAM_NAMES` in `powgatory/tasks.py`, a list per stage; any of the names counts.

```yaml
lhef_executable: "./lhef_analysis"
pythia_executable: "./main-PYTHIA82-lhef"

stages:
  lhef:
    enabled: true
    weight_correction: true                     # Apply ub_btilde_corr/ub_remn_corr
    link_files: ["powheg_output/FlavRegList"]   # Relative to the start directory, or absolute; globs allowed
  pythia:
    enabled: true
    resources:
      time: "48:00:00"
```

## Merging histograms

At the end of a run, POWgatory merges the histogram files (`.top`) of all seeds, one merged file per weight, with the stage's `merge_method`:

| Stage | Files | Merged file |
|---|---|---|
| `stage2` | `pwg-<seed>-NLO.top` (only with `testplots 1` in the POWHEG input) | `pwg-mean-NLO.top` |
| `lhef` | `lhef/pwgLHEF_analysis-<seed>[-W<n>].top` | `lhef/pwgLHEF_analysis-mean[-W<n>].top` |
| `pythia` | `pythia/pwgoutput_py8_histos-<seed>[-W<n>].top` | `pythia/pwgoutput_py8_histos-mean[-W<n>].top` |

The merged file has the method's name in place of the seed. The file names come from `HISTOGRAM_NAMES` in `powgatory/tasks.py`, a list of name patterns per stage (e.g. also `pwgevents-<seed>.lheoutput_py8_histos` for PYTHIA); the merge uses the first one of which seed 1 has a file, and finds the weights from seed 1's files. Seeds without a file are left out with a warning. Without any file (e.g. stage 2 without `testplots 1`) there is nothing to merge, and the run still ends successfully.

Each merge (`POWHEGMerge`) is one job with its stage's `resources`, a Slurm job or, with `batch_system: local`, a local process. The merge of stage 2 runs while stage 3 runs.

`merge_method` (per stage, default 1):

| Method | Combination |
|---|---|
| 1 `mean` | Mean of sets with equal statistics; errors added in quadrature and divided by the number of files |
| 2 `uneven` | Mean weighted with 1/error², for sets with different statistics |
| 3 `sum` | Sum (e.g. born + virtual + real) |
| 4 `max`, 5 `min` | Maximum or minimum, with its error |
| 6 `sum-last` | Sum of all but the last set, minus the last set |
| -1 | No merge for this stage |

The merge is `powgatory/merge.py`, the logic of `mergedata` (POWHEG's Fortran program and its C++ rewrite) with numpy: it reads one file at a time, so memory grows with the lines of one file, not with the number of files, and it writes the same output as `mergedata`, numbers in Fortran D notation (`0.12345678D+00`). It can also be used by hand:

```bash
python -m powgatory.merge 1 pwgLHEF_analysis-*-W1.top             # → pwgLHEF_analysis-mean-W1.top
python -m powgatory.merge 2 files... -out merged.top -modern       # error-weighted, E notation
```

All files must have the same layout (the same number of lines and the same bin edges); data lines are lines with four numbers, all other lines are taken from the first file.

## Configuration reference

`run.yaml` is merged against `powgatory/config/config_default.yaml`, which ships only the minimal set of keys needed to run and is reproduced below as the schema reference. `powgatory/config/example.yaml` holds only the settings most runs change, including a per-stage `resources`/`powheg_parameters` override, and is what `powgatory --init` writes when you have no template of your own (see [Quick start](#quick-start)).

```yaml
powheg_executable: "./pwhg_main"
powheg_input_template: "powheg.input-save"
powheg_seeds_template: "pwgseeds.dat-save"   # optional; falls back to a bundled seeds file
lhef_executable: "./lhef_analysis"          # program of the stage lhef
pythia_executable: "./main-PYTHIA82-lhef"   # program of the stage pythia

job_settings:
  job_name: "powheg"
  mail_type: "None"
  mail_user: ""
  run_dir: "powheg_output"          # where POWHEG runs and writes output
  log_dir: "powheg_output/logs"     # where job logs are written
  exclude_nodes: ""                 # nodes to exclude, as for sbatch --exclude, e.g. "node[01-09],node[15-16]"
  container_image: ""               # registry URL (oras://...), local .sif path, or empty for no container
  copy_container_image: true        # copy a local image into the run_dir (false: use it in place)
  presubmit: ""                     # shell command run once before the workflow starts
  retries: 3                        # resubmissions of a branch whose Slurm job was lost, stuck in COMPLETING, or hung
  completing_grace: 900             # seconds a job may stay COMPLETING without the branch's outputs
  hung_start_minutes: 15            # a running job without POWHEG output after this long is hung (0 = never)
  workers: 250                      # jobs b2luigi keeps running at once (see Batch submission details); --workers overrides it
  local_parallel_tasks: auto        # local branches running at once; auto: cores free at startup (5-minute load average)
  local_niceness: 0                 # niceness (0-19) of the programs of local branches, as for nice -n

stages:
  stage1:
    enabled: true
    grid_iterations: 3
  stage2:
    enabled: true
    merge_method: 1                 # see Merging histograms
  stage3:
    enabled: true
  stage4:
    enabled: true
  lhef:                             # see Analysis stages
    enabled: false
    weight_correction: true
    link_files: []
    merge_method: 1
  pythia:
    enabled: false
    weight_correction: true
    link_files: []
    merge_method: 1

resources:                          # applies to every stage unless overridden per stage (see below)
  batch_system: slurm                # "local" (processes on the node powgatory runs on) or "slurm"
  submission_type: array             # Slurm only: "single", "array", or "mpi"
  max_grouping_size: auto            # seeds per submission; auto: Slurm's MaxArraySize (see Batch submission details)
  partition: alma
  ntasks: 1                          # seeds run in parallel (array jobs) or MPI tasks (mpi jobs)
  time: "24:00:00"
  mem: 4000

powheg_parameters:                  # applies to every stage unless overridden per stage (see below)
  # ncall1: 20000
  # ncall2: 20000
  # nubound: 10000
  # numevts: 1000
```

### Global vs. per-stage settings

Both `resources` and `powheg_parameters` can be set once at the top level, applying to every stage, **and/or** overridden for an individual stage under `stages.<stage>.resources` / `stages.<stage>.powheg_parameters`. Overrides are merged **key by key**, not block-by-block: a stage that only sets one key still inherits every other key from the top-level block. `powgatory/config/example.yaml` demonstrates this — a larger global `ntasks` for most stages, with `stage1` cut down to a single grid-generation task and its own `ncall1`:

```yaml
resources:
  batch_system: slurm
  partition: alma
  ntasks: 10                        # used by stage2/stage3/stage4, which don't override it
  time: "24:00:00"
  mem: 4000

stages:
  stage1:
    enabled: true
    grid_iterations: 3
    resources:
      ntasks: 1                     # overrides ntasks just for stage1; batch_system/partition/time/mem still inherited
    powheg_parameters:
      ncall1: 500                   # overrides ncall1 just for stage1
```

Further notes:

- `batch_system: local` ignores `partition` and runs each branch as a local subprocess instead of submitting to Slurm (useful for testing without cluster access, or for the grid combination `stage3_fullgrid`). At most `job_settings.local_parallel_tasks` local branches run at the same time, across all stages and configs of one POWgatory. With `auto`, this is the number of cores free when POWgatory starts: the cores the process may use, but at most the node's cores minus its 5-minute load average. Right after a previous local run, its processes still count in that average, so set a number then. `job_settings.local_niceness` runs the POWHEG and analysis programs of local branches with that niceness, so that other processes on a busy node go first.
- `container_image` accepts a registry URL (pulled once into `<run_dir>/powheg.sif` via `apptainer pull`), a local file path (copied into the run directory, or used in place with `copy_container_image: false`), or an empty string to run without a container.
- The config file also defines an `addweights` stage. **It is a configuration placeholder only** — see [Known limitations](#known-limitations).

## Batch submission details

Per-stage Slurm behavior is controlled by `resources.submission_type`:

- **`single`** — one independent `sbatch` submission per branch.
- **`array`** — all branches for a stage submitted as one Slurm job array.

`resources.max_grouping_size` sets how many branches go into one submission (one job array, one MPI job, or one batch of `single` submissions). `auto` takes Slurm's `MaxArraySize` from `scontrol show config`, the most elements a job array may have. A stage with `submission_type: mpi` needs a number. Set it in the top-level `resources` block: luigi takes one value for all of stages 1-4 and `stage3_fullgrid` (one task family), from the first of their tasks it sees, so a per-stage value may not take effect.
- **`mpi`** — all branches submitted as a single multi-node Slurm job, dispatched via `srun`/`$SLURM_PROCID`. Sizing information (nodes, CPUs, memory, time limit) for the stage's partition is queried live from `sinfo` at config-load time (not hand-maintained per-cluster config files) and exposed via `POWHEGStage.partition_info`.

`POWHEGConfig` also uses this live `sinfo` data to warn (not fail) if a stage's requested `resources.time` exceeds its partition's actual Slurm time limit.

Job submission is deliberately throttled: `cli.py` monkey-patches b2luigi's `SlurmProcess.start_job` to enforce a minimum interval between `sbatch` calls, to avoid overwhelming the scheduler when submitting large numbers of jobs.

`job_settings.workers` (default 250; `--workers` overrides it) sets how many jobs b2luigi keeps submitted/running concurrently — this replaced an earlier per-cluster `max_parallel_jobs` setting. A grouped Slurm submission takes one of them for all its branches, pending or running, while each local branch and each local helper task takes one. With several configs, the smallest `workers` and `local_parallel_tasks` of them apply. Local branches waiting for one of the `local_parallel_tasks` cores take no worker.

### Stuck, lost and failed jobs

For `submission_type: single` and `array`, b2luigi checks the branches' Slurm jobs once per status poll (every 2 minutes by default) and handles the cases that would otherwise block a stage for hours. These replace the external watchdog script.

| Situation | What happens |
|---|---|
| Job hangs in `COMPLETING`, the branch's outputs are complete | The branch counts as done, and the next stage can start. |
| Job `COMPLETING` for `completing_grace` seconds without the branch's outputs | The branch is resubmitted, at most `retries` times. |
| Job lost by Slurm (`NODE_FAIL`, `BOOT_FAIL`, `PREEMPTED`) | The branch is resubmitted, at most `retries` times. |
| Job `RUNNING` for `hung_start_minutes` without POWHEG output in its log | The job is cancelled and the branch resubmitted, at most `retries` times. This is typically a hanging container mount. |
| POWHEG failed, timed out or ran out of memory | The branch fails and is **not** resubmitted, because it would fail again with the same seed and input. |

When POWHEG exits with an error, POWgatory moves the branch's output files to `incomplete_run_backups/`. POWHEG writes its counters file also when it fails, and the branch would otherwise count as complete. A branch also counts as complete only if its output files are non-empty.

luigi's own retries are switched off. A run with a failed branch finishes everything that does not depend on it and ends with `:(`. After fixing the cause, rerun `powgatory`; it resubmits only the incomplete branches.

Only one POWgatory can run per `run_dir`: a second one stops at once with a message. The lock (`<run_dir>/.powgatory.lock`) is released when the first one ends, also after a crash.

## Container support

If `job_settings.container_image` is set, `POWHEGStage` runs POWHEG inside Apptainer:

- A registry reference (anything containing `://`, e.g. `oras://...`) is pulled once into `<run_dir>/powheg.sif`, by `POWHEGPresubmit`, before any stage runs.
- A local path is copied into `<run_dir>/powheg.sif` once, by `POWHEGPresubmit`, before any stage runs. With `job_settings.copy_container_image: false` it is used in place instead, which suits many runs sharing one image (no copy per run directory).
- An existing `<run_dir>/powheg.sif` is reused as-is.
- An empty value runs POWHEG directly, with no container.

Only the actual POWHEG-execution task (`POWHEGStage`) runs inside the container; setup/symlink/timing tasks always run locally as plain Python.

## CLI reference

```
powgatory -c run.yaml [run2.yaml ...] [OPTIONS]
powgatory --init [FILE]
powgatory --save-template FILE
powgatory --show-defaults

Options:
  -c, --config_file FILE [FILE ...]   Path to one or more run.yaml files (required)
  --workers N                         Number of parallel b2luigi workers (overrides job_settings.workers, default 250)
  --dry-run                           Print what would be submitted, without submitting
  -v, --verbose                       Print the resolved configuration file path(s)
  --version                           Print the installed version and exit
  --init [FILE]                       Write a run config to start from into FILE (default: run.yaml) and exit
  --save-template FILE                Save FILE as your own template for --init and exit
  --show-defaults                     Print the default configuration, with every option and its explanation, and exit
```

Any additional arguments are passed through to b2luigi/luigi.

`--init` never overwrites an existing file. It copies the first template that exists:

1. the file named by `$POWGATORY_TEMPLATE`, if set,
2. `~/.config/powgatory/template.yaml`,
3. the packaged `powgatory/config/example.yaml`.

A personal template holds the settings that stay the same from run to run, such as `container_image`, the partition or `powheg_executable`. `--save-template` copies a file to the first of the two user locations and asks before overwriting a template there. The template can also be a symlink, e.g. into a dotfiles repository.

`--show-defaults` prints `powgatory/config/config_default.yaml`, the defaults every `run.yaml` is merged over, with a comment for each option. It is the complete reference of the options, also where this README is not at hand.

## Using POWgatory with Claude Code

This repository is also a Claude Code plugin marketplace. Its plugin `powgatory` brings a skill that tells Claude how to set up, start, monitor and debug POWgatory runs, so you can ask Claude in any run directory, e.g. "start a POWHEG run with 200 seeds from this powheg.input-save". Install it once.

> [!TIP]
> **Installation in the VS Code extension** ([docs](https://code.claude.com/docs/en/vs-code#manage-plugins)):
> 1. Type `/plugins` in the Claude Code prompt box. This opens the plugin manager.
> 2. In its **Marketplaces** tab, add the marketplace `jakoblinder/POWgatory`.
> 3. In its **Plugins** tab, install `powgatory` for yourself (user scope), so that it is available in all your projects.

In Claude Code in the terminal:

```
/plugin install powgatory --marketplace jakoblinder/POWgatory
```

With Claude Code older than 2.1.275, in two steps:

```
/plugin marketplace add jakoblinder/POWgatory
/plugin install powgatory@powgatory
```

Get a newer version of the skill with `claude plugin update powgatory@powgatory` (then `/reload-plugins` in an open session); a new version is published by raising `version` in `claude-plugin/.claude-plugin/plugin.json`. The skill itself is `claude-plugin/skills/powgatory/SKILL.md`. Claude still asks before it starts a run, and the skill tells it to show what would be submitted first.

## Run directory layout

Everything happens inside `job_settings.run_dir` (`powheg_output/` by default):

- `task_files/` — b2luigi's generated submission scripts.
- `logs/<job_code>/` — POWHEG stdout/stderr per branch, bracketed with `<Started-POWHEG:...>`/`<Finished-POWHEG:...>` markers that `POWHEGStageTimings` parses. The job code is e.g. `p1-x2-s7` (stage 1, grid iteration 2, seed 7), `p3-s12`, or `stage3_fullgrid-s1` for a seed of the grid combination.
- `timings.yaml` — per-stage timing statistics (shortest/longest/average/total), updated as each stage completes.
- `incomplete_run_backups/` — output files quarantined from a previous failed or killed attempt, or from a POWHEG run that exited with an error.
- `.powgatory.lock` — held by the running POWgatory, so that a second one on the same `run_dir` stops.
- `ub_corrections.yaml` — the upper bound correction factors of every stage-4 seed, if an analysis stage has `weight_correction: true`.
- `lhef/`, `pythia/` — the analysis stages' `<seed>-powheg.input` files, links to the event files and `link_files`, the programs' outputs, and the merged histograms.
- `pwg-mean-NLO.top` — the merged NLO histograms of stage 2 (see [Merging histograms](#merging-histograms)).
- POWHEG's own output files (`pwg*.dat`, `pwgevents-*.lhe`, ...) and the `powheg.input`/`pwgseeds.dat` symlinks, written directly into the run directory.

## Troubleshooting

- **`powgatory: command not found`** — check `pip show powgatory` and that your Python user-bin directory is on `PATH`.
- **`ModuleNotFoundError: No module named 'pkg_resources'`** — `luigi` 3.6.0 got installed, usually because `b2luigi` was installed or updated on its own on Python 3.14. Reinstall POWgatory as described in [Updating `b2luigi`](#updating-b2luigi).
- **A stage silently doesn't run** — check the validation warnings printed at startup; they catch the common cases (e.g. a later stage enabled while its prerequisite is disabled).
- **`Unknown submission type: mpi`** — your installed `b2luigi` doesn't yet support MPI-style submission; see [Requirements](#requirements).
- **Container pull fails** — `setup_container()` shells out to `apptainer pull`; check that `apptainer` is on `PATH` on the submission host and that the registry URL is reachable.
- **A branch fails with `pwhg_main failed: ...` (or the name of the analysis program) or `Output files ... never appeared`** — check the branch's POWHEG log under `logs/<job_code>/` for the actual failure. Re-running `powgatory` resubmits only the branches whose output files are missing.
- **`Stage ... uses partition '...', which sinfo does not report`** — the `resources.partition` of that stage doesn't exist on the cluster you're submitting from; the message lists the partitions that do.

## Known limitations

This reflects the current state of the code, so gaps are documented rather than hidden:

- The `addweights` stage is present in the configuration schema but has **no corresponding task implementation** — enabling it has no effect. `powgatory/scrap.py` contains an old, unused draft of this work and is not imported anywhere.
- A stale `pwg-<contribution>-fullgrid.lock` (see [Grid combination](#grid-combination)) is only cleared when the seed that wrote it runs again. If the seed is no longer part of the run (fewer `contributions` or `ntasks` in the rerun), or if it was killed in the instant between POWHEG creating the lock and writing the seed into it (an empty lock), delete the lock by hand while no stage-3 job is running.
- `lhef_analysis` writes its `.top` files already every 20000 events, not only at the end. An analysis branch with more events per seed that is killed during the run can leave files that count as complete; delete them to rerun that seed.
- `submission_type: array` and `mpi` depend on unreleased `b2luigi` functionality, installed from a fork (see [Requirements](#requirements)).
- For `submission_type: mpi`, the live `sinfo` data in `POWHEGStage.partition_info` is not yet passed on to b2luigi; b2luigi packs sub-tasks onto nodes using its own `tasks_per_node` setting (default 64), which must match the partition.
- A job that hangs before POWgatory has moved an earlier attempt's log aside is not detected as hung, because that old log already shows POWHEG output. Such a job sits until its Slurm time limit and its branch fails; `scancel` it and rerun `powgatory`.

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE).
