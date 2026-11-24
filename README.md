# POWgatory

POWgatory runs multi-stage [POWHEG-BOX](https://powhegbox.mib.infn.it/) campaigns on a Slurm cluster, or locally. One YAML file describes the run; POWgatory expands it with [b2luigi](https://b2luigi.readthedocs.io/) into per-seed jobs, submits and tracks them stage by stage, and merges the histograms at the end.

> [!TIP]
> Using Claude Code, e.g. in VS Code? Install the POWgatory skill once, and Claude knows how to set up and run POWgatory: see [Using POWgatory with Claude Code](#using-powgatory-with-claude-code).

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [How a run proceeds](#how-a-run-proceeds)
- [Configuration](#configuration)
- [Slurm and local execution](#slurm-and-local-execution)
- [Grid combination](#grid-combination)
- [Analysis stages](#analysis-stages)
- [Merging histograms](#merging-histograms)
- [CLI reference](#cli-reference)
- [Run directory layout](#run-directory-layout)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)
- [How it works](#how-it-works)
- [Using POWgatory with Claude Code](#using-powgatory-with-claude-code)

## Installation

Requirements:

- Python 3.11 to 3.14.
- `git`, since `b2luigi` and `luigi` are installed from GitHub.
- For Slurm runs: `sinfo`, `scontrol`, `sbatch`, `squeue`, `sacct` and `scancel` on `PATH`.
- [Apptainer](https://apptainer.org/), if POWHEG runs in a container.
- A POWHEG-BOX build (`pwhg_main`) and its input template.

POWgatory needs a `b2luigi` that no release provides yet. It installs the `powgatory` branch of the fork [jakoblinder/b2luigi](https://github.com/jakoblinder/b2luigi/tree/powgatory), which adds Slurm array and MPI submission (`submission_type`, proposed upstream in [belle2/b2luigi#6](https://github.com/belle2/b2luigi/pull/6)) and the handling of stuck and lost Slurm jobs (see [Slurm and local execution](#slurm-and-local-execution)). With a `b2luigi` from PyPI, `submission_type` is silently ignored. `luigi` comes from a pinned commit on GitHub, because no `luigi` release supports Python 3.14 yet.

Install into a virtual environment on a file system the batch nodes can see; batch jobs run with the same Python as the `powgatory` command that submitted them:

```bash
python3.14 -m venv /path/to/venv    # any Python 3.11-3.14; the system python3 may be older
source /path/to/venv/bin/activate
pip install git+https://github.com/jakoblinder/POWgatory.git    # or: pip install /path/to/POWgatory
powgatory --version
```

**Updating.** pip does not fetch new commits of a branch by itself. Reinstall POWgatory with its dependencies:

```bash
pip install --force-reinstall git+https://github.com/jakoblinder/POWgatory.git
```

Reinstall POWgatory, not `b2luigi` alone: pip applies POWgatory's `luigi` pin only when POWgatory is part of the command, and a `luigi` from PyPI (3.6.0) fails on Python 3.14. Don't add `--no-deps` either; new commits of the fork can bring new dependencies (e.g. `xrootd`).

**Development.** Install both checkouts in editable mode, POWgatory first:

```bash
pip install -e /path/to/POWgatory
pip install -e /path/to/b2luigi    # checkout of the powgatory branch
```

## Quick start

```bash
mkdir my_run && cd my_run
cp /path/to/powheg-box/pwhg_main /path/to/powheg-box/powheg.input-save .
cp /path/to/powheg-box/pwgseeds.dat-save .    # optional; otherwise a bundled seeds file is used

powgatory --init                   # writes run.yaml from your template or the packaged example
$EDITOR run.yaml
powgatory -c run.yaml --dry-run    # validates the config and shows what would run
powgatory -c run.yaml              # runs the campaign
```

POWgatory has to keep running until the campaign is done, often for hours or days: start it in `tmux` or `screen` on a login node. Don't edit `run.yaml` or update POWgatory while a campaign runs (see [How it works](#how-it-works)).

## How a run proceeds

```
stage 1, grid 1 → … → stage 1, grid N → stage 2 → [stage3_fullgrid] → stage 3 → stage 4 → [lhef] + [pythia]
                                            └→ merge stage 2                                └→ merge lhef, pythia
```

- Each stage runs `ntasks` seeds in parallel and starts only when the previous stage is complete. Stage 1 runs `grid_iterations` times. Optional stages are in brackets: the [grid combination](#grid-combination) and the [analyses](#analysis-stages).
- POWgatory writes each stage's `powheg.input` from your template with the stage's `powheg_parameters` (parameters missing in the template are appended), and sets `parallelstage` and `xgriditeration` itself. The input needs `manyseeds 1`.
- A seed is complete when its POWHEG output files exist and are non-empty; the exit code is not used. Running `powgatory -c run.yaml` again therefore runs only what is incomplete. Seeds above 9999 (5-digit file names) work.
- Before a seed runs again, leftovers of an earlier attempt are moved to `incomplete_run_backups/`, since POWHEG refuses to overwrite e.g. an event file. When POWHEG exits with an error, its outputs are moved there too (it writes its counters file also on failure).
- A failed seed is not rerun automatically. Everything that does not depend on it still runs, and the summary at the end shows `:(`. Fix the cause and start POWgatory again.
- Only one POWgatory can run per run directory: a second one stops at once ("Another POWgatory is already running …"), also when started from another directory or node. The lock is released when the first one ends, also after a crash.
- `powgatory -c a.yaml b.yaml …` runs several independent configs in one process; hundreds work.

## Configuration

`run.yaml` is merged over the defaults. **All options, each with its explanation, are in [`powgatory/config/config_default.yaml`](powgatory/config/config_default.yaml)**, which `powgatory --show-defaults` prints. A typical run sets only a few (this is [`example.yaml`](powgatory/config/example.yaml), which `powgatory --init` writes without a template of your own):

```yaml
powheg_executable: "./pwhg_main"
powheg_input_template: "powheg.input-save"

job_settings:
  run_dir: "powheg_output"          # where POWHEG runs and writes its output
  log_dir: "powheg_output/logs"
  container_image: "oras://gitlab-registry.mpcdf.mpg.de/mpp/containers/jlinder/powheg:powheg-latest"

stages:
  stage1:
    enabled: true
    grid_iterations: 3
    resources:
      ntasks: 1                     # per-stage settings override the global ones, key by key
    powheg_parameters:
      ncall1: 500
  stage2:
    enabled: true
  stage3:
    enabled: true
  stage4:
    enabled: true

resources:                          # every stage, unless the stage overrides a key
  batch_system: slurm               # slurm or local
  partition: alma
  ntasks: 10                        # seeds per stage
  time: "24:00:00"
  mem: 4000                         # MB per seed

powheg_parameters:                  # values that replace those in powheg.input-save, every stage
  # ncall1: 20000
```

`resources` and `powheg_parameters` can be set at the top level and per stage (`stages.<stage>.resources`, `stages.<stage>.powheg_parameters`). They are merged key by key: a stage that sets only `ntasks` keeps the top-level `partition`, `time` and so on. Paths are relative to the directory POWgatory is started from, or absolute.

## Slurm and local execution

**Submission.** `resources.submission_type` selects how a stage's seeds go to Slurm:

- `array` (default): one job array per stage.
- `single`: one `sbatch` per seed.
- `mpi`: one multi-node job, one seed per CPU (`srun`). It packs as many seeds onto a node as the stage's partition has CPUs per node, and requests `mem` (per seed) times the seeds per node, since `--mem` counts per node.

`resources.max_grouping_size` is the most seeds per submission. `auto` (default) is Slurm's `MaxArraySize`; an `mpi` stage needs a number. Set it in the top-level `resources`: stages 1–4 and `stage3_fullgrid` share one value (luigi takes it from the first of their tasks), so a per-stage value may not take effect.

At startup, POWgatory checks every Slurm stage against `sinfo`: an unknown partition is an error; a `time` above the partition's limit, or an `mpi` stage whose `mem` × CPUs per node exceeds a node's memory, is a warning. `sbatch` calls are at least 2 s apart.

**Limits.** `job_settings.workers` (default 250, `--workers` overrides it) is the number of jobs b2luigi keeps submitted or running at once. A grouped Slurm submission counts once for all its seeds, pending or running; each local seed and each short local helper task counts once.

`batch_system: local` runs a stage's seeds as processes on the node POWgatory runs on. At most `job_settings.local_parallel_tasks` of them run at the same time, over all stages and configs; `auto` is the number of cores free at startup (the cores the process may use, at most the node's cores minus its 5-minute load average). Right after a previous local run, its processes still count in that average, so set a number then. `job_settings.local_niceness` (0–19) runs them with that niceness. A local seed waiting for a core takes no worker. With several configs, the smallest `workers` and `local_parallel_tasks` among them apply.

**Containers.** `job_settings.container_image` runs POWHEG and the analysis programs in Apptainer:

- a registry URL (`oras://…`) is pulled once into `<run_dir>/powheg.sif` before any stage runs;
- a local `.sif` is copied there, or used in place with `copy_container_image: false` (e.g. one image for many runs);
- an existing `<run_dir>/powheg.sif` is reused; `""` runs without a container.

**Stuck, lost and failed jobs.** For `single` and `array`, b2luigi checks the jobs at every status poll (every 2 minutes):

| Situation | What happens |
|---|---|
| Job hangs in `COMPLETING`, the seed's outputs are complete | The seed counts as done, and the next stage can start. |
| Job `COMPLETING` for `completing_grace` seconds without the seed's outputs | The seed is resubmitted, at most `retries` times. |
| Job lost by Slurm (`NODE_FAIL`, `BOOT_FAIL`, `PREEMPTED`) | The seed is resubmitted, at most `retries` times. |
| Job `RUNNING` for `hung_start_minutes` without POWHEG output in its log | The job is cancelled and the seed resubmitted, at most `retries` times. This is typically a hanging container mount. |
| POWHEG failed, timed out or ran out of memory | The seed fails and is **not** resubmitted: it would fail again with the same seed and input. |

`retries`, `completing_grace` and `hung_start_minutes` are in `job_settings`.

## Grid combination

At the start of stage 3, POWHEG combines the stage-2 grids of each contribution (btilde, remn, reg) into a fullgrid file. One seed does this per contribution, while all other seeds wait. This can take hours, and every stage-3 job then needs a time limit that covers it. With `stage3_fullgrid`, these first seeds run as a stage of their own; stage 3 runs its other seeds afterwards:

```yaml
stages:
  stage3_fullgrid:
    enabled: true
    contributions: auto        # or 1, 2, 3
    resources:
      batch_system: local      # e.g. on the login node, without a Slurm time limit
```

- The stage has its own `resources`: run it locally, or keep Slurm and give only this stage a longer `time`. Its own `ntasks` is not used.
- Its seeds are stage-3 seeds 1 to `contributions`, with stage 3's `powheg_parameters` (it can't have its own). Stage 3 therefore needs more seeds (`ntasks`) than `contributions`.
- `contributions: auto` reads the number from `powheg_input_template` and stage 3's `powheg_parameters`, by POWHEG-BOX-RES's rules (`bbinit.f`): btilde unless `withbtilde 0`; remn with `hdamp > 0`, `hfact > 0`, `withdamp 1` or `bornzerodamp 1`, unless `withremnants 0`; reg unless `withregular 0`; remn and reg never with `bornonly 1`, `LOevents 1` or `virtonly 1`. A process without regular contributions gets one seed more than needed, which waits and then does normal stage-3 work. For other POWHEG versions, set 1, 2 or 3.
- Logs: `logs/stage3_fullgrid-s<seed>.log`; `timings.yaml` has an entry `stage3_fullgrid`.
- While a seed combines a contribution, POWHEG holds `pwg-<contribution>-fullgrid.lock`. A killed seed leaves its lock behind and all other seeds wait for it; POWgatory moves the lock into `incomplete_run_backups/` when that seed runs again (also in stage 3). A lock whose seed is no longer part of the run, or an empty one (the seed was killed right after creating it), has to be deleted by hand, while no stage-3 job runs.

## Analysis stages

`lhef` and `pythia` run `lhef_executable` (e.g. `lhef_analysis`) and `pythia_executable` (e.g. `main-PYTHIA82-lhef`) once per stage-4 seed, on its event file, each in its own directory `<run_dir>/lhef/` or `<run_dir>/pythia/`. They run in parallel after stage 4, with stage 4's `ntasks`; all other `resources` and the `powheg_parameters` can be set per stage.

```yaml
lhef_executable: "./lhef_analysis"
pythia_executable: "./main-PYTHIA82-lhef"

stages:
  lhef:
    enabled: true
    weight_correction: true                     # multiply the weights by ub_btilde_corr/ub_remn_corr
    link_files: ["powheg_output/FlavRegList"]   # further files the program needs; globs allowed
  pythia:
    enabled: true
    resources:
      time: "48:00:00"
```

- `weight_correction: true` applies POWHEG's upper bound correction: the factors `ub_btilde_corr` and `ub_remn_corr` of every seed are computed once from `pwgcounters-st4-<seed>.dat` into `<run_dir>/ub_corrections.yaml`, as POWHEG-BOX's `FindReweightFromCounters.py` does (also by hand: `python -m powgatory.ub_correction pwgcounters-st4-*.dat`). The counters contain the numbers only with `ubexcess_correct 1` in the POWHEG input.
- Each seed gets `<seed>-powheg.input` (template, the stage's `powheg_parameters`, the seed's factors) and links to its event file and the `link_files`. The program reads the seed and the event file on stdin.
- A seed is complete when its histogram file is non-empty: `pwgLHEF_analysis-<seed>.top` or `pwgoutput_py8_histos-<seed>.top`, with several weights `…-<seed>-W1.top`, `-W2.top`, …. A program that names its files differently needs its pattern in `HISTOGRAM_NAMES` in `powgatory/tasks.py`.

## Merging histograms

At the end, the histogram files (`.top`) of all seeds are merged, one file per weight, with the stage's `merge_method`:

| Stage | Files | Merged file |
|---|---|---|
| `stage2` | `pwg-<seed>-NLO.top` (only with `testplots 1`) | `pwg-mean-NLO.top` |
| `lhef` | `lhef/pwgLHEF_analysis-<seed>[-W<n>].top` | `lhef/pwgLHEF_analysis-mean[-W<n>].top` |
| `pythia` | `pythia/pwgoutput_py8_histos-<seed>[-W<n>].top` | `pythia/pwgoutput_py8_histos-mean[-W<n>].top` |

| `merge_method` | Combination |
|---|---|
| 1 `mean` (default) | Mean of sets with equal statistics; errors added in quadrature and divided by the number of files |
| 2 `uneven` | Mean weighted with 1/error², for sets with different statistics |
| 3 `sum` | Sum (e.g. born + virtual + real) |
| 4 `max`, 5 `min` | Maximum or minimum, with its error |
| 6 `sum-last` | Sum of all but the last set, minus the last set |
| 0 | No merge for this stage |

The merged file has the method's name in place of the seed. Seeds without a file are left out with a warning; without any file (e.g. stage 2 without `testplots 1`) nothing is merged. Each merge is one job with its stage's `resources`; stage 2's runs while stage 3 runs.

The merge (`powgatory/merge.py`) writes the same output as POWHEG's `mergedata` and reads one file at a time. All files need the same bins. By hand:

```bash
python -m powgatory.merge 1 pwgLHEF_analysis-*-W1.top             # → pwgLHEF_analysis-mean-W1.top
python -m powgatory.merge 2 files... -out merged.top -modern       # error-weighted, E notation
```

## CLI reference

```
powgatory -c run.yaml [run2.yaml ...] [OPTIONS]
powgatory --init [FILE]
powgatory --save-template FILE
powgatory --show-defaults

Options:
  -c, --config_file FILE [FILE ...]   One or more run.yaml files
  --workers N                         Jobs b2luigi keeps running at once (overrides job_settings.workers)
  --dry-run                           Validate the config and show what would run, without running it
  -v, --verbose                       Print the configuration file path(s)
  --version                           Print the installed version and exit
  --init [FILE]                       Write a run config to start from into FILE (default: run.yaml) and exit
  --save-template FILE                Save FILE as your own template for --init and exit
  --show-defaults                     Print every option with its default and explanation, and exit
```

Further arguments are passed on to b2luigi.

`--init` never overwrites a file. It copies the first template that exists: `$POWGATORY_TEMPLATE`, `~/.config/powgatory/template.yaml`, or the packaged `example.yaml`. A personal template holds what stays the same from run to run, such as `container_image` or `partition`. `--save-template` copies a file to the first of the two user locations, and asks before overwriting; the template may also be a symlink, e.g. into a dotfiles repository.

## Run directory layout

Inside `job_settings.run_dir`:

- POWHEG's input and output files (`p<N>-powheg.input`, `pwg*.dat`, `pwgevents-*.lhe`, …) and the `powheg.input`/`pwgseeds.dat` symlinks of the current stage.
- `logs/<job_code>.log`: POWHEG's (or the analysis program's) output of one seed. The job code is e.g. `p1-x2-s7` (stage 1, grid iteration 2, seed 7), `p3-s12`, `stage3_fullgrid-s1` or `lhef-s3`.
- `logs/<job_code>/stdout`, `stderr`: the batch job's own output.
- `timings.yaml`: run times per finished stage (shortest, longest, average, total).
- `incomplete_run_backups/`: files of earlier failed or killed attempts.
- `ub_corrections.yaml`, `lhef/`, `pythia/`: see [Analysis stages](#analysis-stages); `pwg-mean-NLO.top`: see [Merging histograms](#merging-histograms).
- `task_files/`: b2luigi's generated job scripts.

The lock file `.powgatory-run-dirs.lock` is in the directory that contains the run directory.

## Troubleshooting

- **A seed fails with `pwhg_main failed: …` (or the analysis program's name) or `Output files … never appeared`**: read `logs/<job_code>.log`. Rerunning `powgatory` runs only the incomplete seeds.
- **`task id … does not exist in the locally reproduced task graph`**: `run.yaml` or POWgatory changed while jobs were queued. Stop POWgatory and start it again; finished seeds stay done.
- **`Stage … uses partition '…', which sinfo does not report`**: wrong partition name; the message lists the existing ones.
- **`Another POWgatory is already running in …`**: one is still running on that run directory.
- **`Unknown submission_type '…'`**: a typo in `submission_type`.
- **A stage silently doesn't run**: read the warnings printed at startup (e.g. a stage enabled while the stage before it is disabled).
- **`ModuleNotFoundError: No module named 'pkg_resources'`**: `luigi` 3.6.0 got installed, usually by updating `b2luigi` on its own. Reinstall POWgatory (see [Installation](#installation)).
- **Container pull fails**: check that `apptainer` is on `PATH` and the registry is reachable.

## Known limitations

- The `addweights` stage is in the configuration but has no implementation; enabling it has no effect.
- `lhef_analysis` writes its `.top` files every 20000 events, not only at the end. An analysis seed with more events that is killed during the run can leave files that count as complete; delete them to rerun that seed.
- A job that hangs before POWgatory has moved an earlier attempt's log aside is not detected as hung, because that old log already shows POWHEG output. It runs into its time limit and the seed fails; `scancel` it and rerun `powgatory`.

## How it works

Each stage is a chain of b2luigi tasks (`powgatory/tasks.py`): `POWHEGStageSetup` writes the stage's input file, `POWHEGCreateSymlinks` points `powheg.input` and `pwgseeds.dat` at it, one `POWHEGStage` per seed runs POWHEG (`scripts/run_program.sh`, optionally in the container), and `POWHEGStageTimings` writes `timings.yaml`. The timings task is the join point: the next stage depends on it. `POWHEGPresubmit` runs `job_settings.presubmit` and prepares the container once, before everything else. The analyses add `POWHEGWeightCorrection`, `POWHEGAnalysisSetup` and `POWHEGAnalysis`, the merges `POWHEGMerge`.

A batch job gets only the config file and its task ID. It rebuilds the config and the task graph and looks its task up by the ID, which contains a hash of the whole config. Editing `run.yaml` or updating POWgatory while jobs are queued therefore makes them fail ("task id … does not exist"). Nothing queried from Slurm goes into the config, so batch jobs send no request to the Slurm controller.

## Using POWgatory with Claude Code

This repository is also a Claude Code plugin marketplace. Its plugin `powgatory` brings a skill that tells Claude how to set up, start, monitor and debug POWgatory runs, so you can ask Claude in any run directory, e.g. "start a POWHEG run with 200 seeds from this powheg.input-save". Claude still asks before it starts a run, and shows what would be submitted first.

> [!TIP]
> **Installation in the VS Code extension** ([docs](https://code.claude.com/docs/en/vs-code#manage-plugins)):
> 1. Type `/plugins` in the Claude Code prompt box. This opens the plugin manager.
> 2. In its **Marketplaces** tab, add the marketplace `jakoblinder/POWgatory`.
> 3. In its **Plugins** tab, install `powgatory` for yourself (user scope), so that it is available in all your projects.

In Claude Code in the terminal: `/plugin install powgatory --marketplace jakoblinder/POWgatory` (before Claude Code 2.1.275: `/plugin marketplace add jakoblinder/POWgatory`, then `/plugin install powgatory@powgatory`).

Update with `claude plugin update powgatory@powgatory` (then `/reload-plugins` in an open session). The skill is `claude-plugin/skills/powgatory/SKILL.md`; a new version is published by raising `version` in `claude-plugin/.claude-plugin/plugin.json`.

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE).
