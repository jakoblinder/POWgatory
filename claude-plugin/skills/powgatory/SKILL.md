---
name: powgatory
description: Set up, start, monitor and debug POWHEG-BOX runs with POWgatory (stages 1-4 on Slurm or locally, LHEF analysis, PYTHIA shower, merged histograms). Use when the user wants to start, configure, check, rerun or debug a POWHEG run, or mentions powgatory, run.yaml, pwhg_main, powheg.input, POWHEG stages or seeds.
---

# POWgatory

POWgatory runs a POWHEG-BOX campaign from one `run.yaml`: it expands the stages into per-seed
jobs (b2luigi), submits them as Slurm array jobs or runs them as local processes, and waits until
each stage is done before starting the next one:

stage 1 (`grid_iterations` iterations) → stage 2 → optional grid combination `stage3_fullgrid` →
stage 3 → stage 4 → optional `lhef` and `pythia` analyses of the event files → merge of the
histograms of stage 2, `lhef` and `pythia`.

A branch counts as done when its output files exist and are non-empty, not by exit code. Running
`powgatory -c run.yaml` again therefore only (re)runs what is incomplete.

## Rules

- Do not start a run (`powgatory -c ...` without `--dry-run`) before the user has agreed to it.
  Show what will be submitted first: the dry run, plus `ntasks`, `partition`, `time` per stage.
- The POWgatory process must keep running until the campaign is finished, often for hours or
  days. Suggest starting it in `tmux` or `screen` on a login node. If you start it yourself, use
  `nohup powgatory -c run.yaml > powgatory.log 2>&1 &` in the run's start directory, not a
  foreground command, which a timeout would kill.
- Do not edit `run.yaml` or update POWgatory while a campaign is running: the batch jobs rebuild
  the task graph from the config and the installed code, and must find the same tasks.
- Never delete or overwrite a run directory, event files or results without asking.
- POWgatory locks each run directory (one byte of `.powgatory-run-dirs.lock` in its parent
  directory), so a second process on the same run directory stops at once ("Another POWgatory is
  already running ..."). Check for a running one instead.
- Each seed writes about 20 files per stage. GPFS file systems such as `/ptmp` limit the number of
  files per user; check the quota (`mmlsquota`) before campaigns with many seeds.

## Starting a run

1. The start directory holds the POWHEG executable (`pwhg_main`), the input template
   (`powheg.input-save`), and optionally `pwgseeds.dat-save`. Paths in `run.yaml` are relative to
   this directory or absolute.
2. `powgatory --init` writes `run.yaml`, from the user's template (`$POWGATORY_TEMPLATE` or
   `~/.config/powgatory/template.yaml`) or else the packaged example.
3. Adapt it. `powgatory --show-defaults` prints every option with its explanation and default:
   read it instead of guessing option names. The usual ones:
   - `powheg_executable`, `powheg_input_template`
   - `job_settings.run_dir` (all outputs go there), `job_settings.container_image`
     (registry URL, local `.sif`, or `""`)
   - `stages.<stage>.enabled`, `stages.stage1.grid_iterations`
   - `resources`: `batch_system` (`slurm` or `local`), `partition`, `ntasks` (number of seeds),
     `time`, `mem`
   - `powheg_parameters`: values that override the template, e.g. `ncall1`, `ncall2`,
     `nubound`, `numevts`
   `resources` and `powheg_parameters` can be set at the top level and per stage
   (`stages.<stage>.resources`), merged key by key.
4. The POWHEG input needs `manyseeds 1` (in the template or in `powheg_parameters`): POWgatory
   gives each branch its seed number. It sets `parallelstage` and `xgriditeration` itself.
   Stage 2 writes the NLO histograms `pwg-<seed>-NLO.top` only with `testplots 1`; the upper bound
   weight correction of the analyses needs `ubexcess_correct 1`.
5. `powgatory -c run.yaml --dry-run` validates the config (executables, partitions from `sinfo`,
   time limits) and lists what would run.
6. `powgatory -c run.yaml` starts the run. `job_settings.workers` (default 250, overridden by
   `--workers N`) limits the number of jobs submitted or running at the same time; a grouped Slurm
   submission counts once. `job_settings.local_parallel_tasks` limits the local branches running at
   the same time (`auto`: cores free at startup), `job_settings.local_niceness` their niceness (0-19). `resources.max_grouping_size: auto` (default)
   puts up to Slurm's `MaxArraySize` seeds into one job array; `mpi` stages need a number. Several
   configs: `powgatory -c a.yaml b.yaml`; they share these limits (the smallest value applies).

## Grid combination before stage 3

At the start of stage 3, one seed per contribution (btilde, remn, reg) combines the stage-2 grids
into a fullgrid file while all other seeds wait; this can take hours. Suggest
`stages.stage3_fullgrid.enabled: true` when stage-3 jobs run into their time limit, or when the
user mentions fullgrid files or grid combination:

- Its seeds are the first stage-3 seeds, one per contribution. Stage 3 runs the others only
  afterwards, and needs more seeds (`ntasks`) than `contributions`.
- `contributions: auto` (default) reads the number from the input; `1`, `2` or `3` sets it, e.g.
  for older POWHEG versions. The config output names the contributions it found.
- `stages.stage3_fullgrid.resources.batch_system: local` runs it on the node POWgatory runs on,
  without a Slurm time limit. Or give only this stage a longer `time`.
- It uses stage 3's `powheg_parameters` and cannot have its own. Its logs are
  `stage3_fullgrid-s<seed>.log`.
- A killed seed leaves its `pwg-<contribution>-fullgrid.lock` in the run directory, and the other
  seeds wait for it. POWgatory moves the lock aside when that seed runs again. If stage-3 seeds
  hang on "lock ... already there, waiting" while the seed named in the lock does not run (an
  empty lock, or a seed no longer in the run), ask before deleting the lock, and only when no
  stage-3 job is running.

## Analyses and merged histograms

- `lhef` and `pythia` run `lhef_executable` / `pythia_executable` once per stage-4 seed, in
  `<run_dir>/lhef/` and `<run_dir>/pythia/`, with `weight_correction` (the factors
  `ub_btilde_corr`, `ub_remn_corr` from `pwgcounters-st4-<seed>.dat`) and `link_files` (further
  files the program needs).
- `merge_method` per stage (`stage2`, `lhef`, `pythia`): 1 mean (default), 2 weighted with
  1/error², 3 sum, 4 max, 5 min, 6 sum minus the last, -1 no merge. Results:
  `pwg-mean-NLO.top`, `lhef/pwgLHEF_analysis-mean[-W<n>].top`,
  `pythia/pwgoutput_py8_histos-mean[-W<n>].top`.
- By hand: `python -m powgatory.merge 1 files... -out merged.top` (like `mergedata`), and
  `python -m powgatory.ub_correction pwgcounters-st4-*.dat` (like `FindReweightFromCounters.py`).

## Checking a run

- The POWgatory output ends with a summary: `:)` everything done, `:(` something failed.
- `squeue -u $USER`, `sacct -u $USER -S <time>` for the jobs.
- `<run_dir>/timings.yaml`: per finished stage, the number of runs and their run times.
- `<run_dir>/logs/<job_code>.log`: the program's output of one branch; `<job_code>` is e.g.
  `p1-x2-s7` (stage 1, grid iteration 2, seed 7), `p4-s12`, `stage3_fullgrid-s1`, `lhef-s3`.
  `<run_dir>/logs/<job_code>/stdout` and `stderr`: the batch job's output.
- `<run_dir>/incomplete_run_backups/`: files of earlier failed or interrupted attempts.

## When something fails

- Fix the cause, then run `powgatory -c run.yaml` again: only incomplete branches run.
- A branch whose program failed, timed out or ran out of memory is not resubmitted
  automatically; Slurm jobs that were lost, stuck in COMPLETING or hung are, up to
  `job_settings.retries` times.
- `pwhg_main failed: ...` (or the analysis program) or `Output files ... never appeared`: read
  `<run_dir>/logs/<job_code>.log`.
- `task id ... does not exist in the locally reproduced task graph`: the config or POWgatory
  changed while jobs were queued. Stop POWgatory and start it again; finished branches stay done.
- `Stage ... uses partition '...', which sinfo does not report`: wrong partition name; the
  message lists the existing ones.

The full documentation is the README at https://github.com/jakoblinder/POWgatory.
