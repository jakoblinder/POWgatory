# POWHEG-BOX LAW Workflow System

A professional workflow orchestration system for POWHEG-BOX using **LAW (Luigi Analysis Workflows)**. This version provides native SLURM workflow support, container sandboxing, and all the power of LAW while maintaining a simple configuration interface.

## Key Features

- **LAW-native SLURM workflows** - Proper job submission via `law.slurm.SlurmWorkflow`
- **Apptainer container sandboxes** - Container support built into LAW
- **Local parallel execution** - Test workflows without SLURM via `law.LocalWorkflow`
- **Single YAML configuration** - One `run.yaml` per simulation
- **Automatic dependency resolution** - Luigi handles task ordering
- **Branch-based parallelism** - Each seed/task runs as a workflow branch
- **Environment module loading** - Automatically load modules at job startup via `.modules` file
- **Workflow validation** - Automatic detection of configuration errors at startup
- **Clean container management** - Consolidated `ContainerManager` for consistent execution

---

## Quick Start

### Installation

```bash
# Clone the repository
cd /path/to/powheg-luigi-workflow

# Install with LAW support
pip install --user -e .

# Verify installation
powheg-workflow --help
law --help
```

### Usage

```bash
# Create your run directory
mkdir my_powheg_run && cd my_powheg_run

# Copy POWHEG files
cp ~/powheg/pwhg_main .
cp ~/powheg/powheg.input-save .
cp ~/powheg/pwgseeds.dat-save .

# (Optional) Create modules file for environment setup
cat > .modules <<EOF
gcc/11
intel/2021.4
mpi/openmpi-4.1.2
EOF

# Copy and edit configuration
cp /path/to/powheg-luigi-workflow/example_run.yaml ./run.yaml
nano run.yaml

# Run the workflow (validation warnings printed automatically)
powheg-workflow run.yaml

# Or use LAW directly
law run POWHEGWorkflow --config-file run.yaml
```

---

## LAW vs Plain Luigi

This version uses **LAW (Luigi Analysis Workflows)** which extends Luigi with:

| Feature | Plain Luigi | LAW |
|---------|-------------|-----|
| SLURM submission | Manual scripts | Built-in `SlurmWorkflow` |
| Container support | Manual setup | `apptainer::` sandbox |
| Job monitoring | Basic | Interactive status |
| Dependency inspection | Manual | `law run --print-deps` |
| Task indexing | None | `law index` |

---

## Workflow Backends

### 1. SLURM Execution (Default)

```bash
# Auto-detect from run.yaml
powheg-workflow run.yaml

# Force SLURM backend
powheg-workflow run.yaml --workflow slurm

# LAW native
law run POWHEGStage3 --config-file run.yaml --workflow slurm
```

### 2. Local Execution (Testing)

```bash
# Force local backend (GNU parallel style)
powheg-workflow run.yaml --workflow local

# LAW native
law run POWHEGStage3 --config-file run.yaml --workflow local --workers 4
```

---

## run.yaml Configuration

### Minimal Configuration

```yaml
cluster: mpi
powheg_executable: "./pwhg_main"
powheg_input_template: "powheg.input-save"

stages:
  stage1: {enabled: true, grid_iterations: 3}
  stage2: {enabled: true}
  stage3: {enabled: true, grid_combination: true}
  stage4: {enabled: true}
```

### Full Configuration

```yaml
# =============================================================================
# CLUSTER SELECTION
# =============================================================================
cluster: mpi                           # mpi, viper, raven, or local

# =============================================================================
# POWHEG FILES
# =============================================================================
powheg_executable: "./pwhg_main"
powheg_input_template: "powheg.input-save"
powheg_seeds_template: "pwgseeds.dat-save"

# =============================================================================
# STAGES TO RUN
# =============================================================================
stages:
  stage1:
    enabled: true
    grid_iterations: 3

  stage2:
    enabled: true

  stage3:
    enabled: true
    grid_combination: true             # Use dedicated grid combination phase

  stage4:
    enabled: true

  analysis:
    enabled: false
    lhef: true
    pythia: true
    weight_correction: true

  addweights:
    enabled: false
    rwl_file: "newweights.xml"
    lhe_dir: "."

# =============================================================================
# RESOURCE ALLOCATION
# =============================================================================
resources:
  stage1: {ntasks: 100, time: "24:00:00", partition: alma}
  stage2: {ntasks: 100, time: "24:00:00", partition: alma}
  stage3: {ntasks: 100, time: "24:00:00", partition: alma}
  stage3_gridcombine: {ntasks: 3, time: "4:00:00", use_tmp: true}
  stage4: {ntasks: 100, time: "24:00:00", partition: alma}
  analysis: {ntasks: 100, time: "24:00:00", partition: alma}
  addweights: {ntasks: 100, time: "24:00:00", mem: 1000}

# =============================================================================
# POWHEG PARAMETER OVERRIDES
# =============================================================================
powheg_parameters:
  # ncall1: 50000
  # ncall2: 100000
  # nevents: 100000

# =============================================================================
# JOB SETTINGS
# =============================================================================
job_settings:
  job_name: "powheg"
  mail_type: "End,Fail"
  mail_user: "you@example.com"
  output_dir: "powheg_output"
  # Container image (optional - omit for no container)
  container_image: "oras://gitlab-registry.mpcdf.mpg.de/mpp/containers/jlinder/powheg:powheg-latest"
```

---

## Environment Module Loading

You can automatically load environment modules when jobs start by creating a `.modules` file in your run directory:

```bash
# .modules (in run directory)
gcc/11
intel/2021.4
python/3.10
mpi/openmpi-4.1.2
```

The bootstrap script will:
- Load each module at job startup
- Skip empty lines and comments (lines starting with `#`)
- Print warnings if a module fails to load
- Continue loading remaining modules even if one fails

This is useful for ensuring consistent compiler versions, MPI implementations, or Python packages across all job nodes.

---

## Workflow Validation

The system automatically validates your workflow configuration at startup to catch common errors:

```bash
$ powheg-workflow run.yaml
WARNING: Stage 2 is enabled but Stage 1 is disabled
WARNING: Stage 4 is enabled but Stage 3 is disabled
```

**Validation Checks:**
- Stage 2 requires Stage 1 to be enabled
- Stage 3 requires Stage 2 to be enabled
- Stage 3 GridCombine requires Stage 2 to be enabled
- Stage 4 requires either Stage 3 or Stage 3 GridCombine to be enabled
- Analysis and AddWeights require Stage 4 to be enabled
- Stage 1 must have at least 1 grid iteration if enabled

This helps prevent misconfigured workflows that would silently skip stages or fail mid-run.

---

### Running Workflows

```bash
# Full workflow
law run POWHEGWorkflow --config-file run.yaml

# Specific stage
law run POWHEGStage3 --config-file run.yaml

# With SLURM backend
law run POWHEGStage3 --config-file run.yaml --workflow slurm

# With local backend
law run POWHEGStage3 --config-file run.yaml --workflow local --workers 4

# Single branch (for debugging)
law run POWHEGStage3 --config-file run.yaml --branch 5
```

### Inspecting Workflows

```bash
# Index tasks for completion
law index --modules powheg_workflow.tasks

# Print task status
law run POWHEGWorkflow --config-file run.yaml --print-status 0

# Print dependency tree
law run POWHEGWorkflow --config-file run.yaml --print-deps 2

# Remove task outputs (reset)
law run POWHEGStage3 --config-file run.yaml --remove-output 0
```

### LAW Configuration

Copy `law.cfg.example` to your run directory as `law.cfg` for custom settings:

```bash
cp /path/to/powheg-luigi-workflow/law.cfg.example ./law.cfg
```

Key `law.cfg` sections:
- `[modules]` - Task modules for indexing
- `[slurm]` - SLURM job settings
- `[apptainer_sandbox]` - Container configuration

---

## Workflow Architecture

### Task Dependency Graph

```
POWHEGStage1(grid=1) → POWHEGStage1(grid=2) → POWHEGStage1(grid=3)
                                                       ↓
                                               POWHEGStage2
                                                       ↓
                         ┌─────────────────────────────┴─────────────────────────────┐
                         ↓                                                           ↓
              POWHEGStage3GridCombine                              POWHEGStage3 (if grid_combination=false)
                         ↓                                                           ↓
                         └─────────────────────────────┬─────────────────────────────┘
                                                       ↓
                                               POWHEGStage4
                                                       ↓
                                    ┌──────────────────┴──────────────────┐
                                    ↓                                     ↓
                            POWHEGAnalysis                        POWHEGAddWeights
```

### Workflow Classes

Each stage inherits from both `SlurmWorkflow` and `LocalWorkflow`:

```python
class POWHEGStage3(POWHEGWorkflowTask):
    """Event generation stage."""

    def create_branch_map(self):
        # Each branch is a parallel task (seed)
        return {i: i for i in range(self.config.get_resources('stage3')['ntasks'])}

    def workflow_requires(self):
        # Depends on Stage 2 or GridCombine
        if self.config.run_config['stages']['stage3'].get('grid_combination'):
            return {"gridcombine": POWHEGStage3GridCombine.req(self)}
        return {"stage2": POWHEGStage2.req(self)}

    def run(self):
        # Execute POWHEG for this branch
        task_id = self.branch_data
        self._run_powheg(task_id)
```

---

## Container Support

LAW provides native Apptainer sandbox support. The workflow uses a consolidated `ContainerManager` class to handle all container execution logic consistently across stages:

```python
# Internal implementation (ContainerManager handles all of this)
self.container_manager.run([pwhg_main], cwd=..., env=..., check=True)
```

This approach:
- Eliminates duplicate container-wrapping code across all 7 stages
- Provides consistent error handling and environment merging
- Automatically wraps commands with `apptainer exec` when needed
- Works seamlessly with both container and non-container modes

### Container Configuration

In `run.yaml`:

```yaml
job_settings:
  # From registry (will be pulled to powheg.sif)
  container_image: "oras://registry.example.com/powheg:latest"

  # Local file
  container_image: "/path/to/powheg.sif"

  # No container (run directly)
  container_image: ""
```

---

## Available Clusters

| Cluster | Type | Tasks/Node | Use Case |
|---------|------|------------|----------|
| `mpi` | SLURM array | 64 | Standard Linux cluster |
| `viper` | MPI multi-node | 128 | Large HPC jobs |
| `raven` | MPI multi-node | 72 | Large HPC jobs |
| `local` | GNU parallel | 4 | Testing without SLURM |

Cluster configurations are in `config/clusters/`.

---

## CLI Reference

### powheg-workflow (Wrapper)

```bash
powheg-workflow CONFIG [OPTIONS]

Arguments:
  CONFIG              Path to run.yaml

Options:
  -t, --task TASK     Task to run (default: POWHEGWorkflow)
  --workflow MODE     Execution backend: slurm, local, auto
  --workers N         Number of workers for local execution
  --grid-iteration N  Grid iteration for Stage1
  --print-status      Print workflow status
  --print-deps        Print dependency tree
  --dry-run           Show what would be done
  -v, --verbose       Verbose output
  --version VER       Output version identifier
```

### Available Tasks

| Task | Description |
|------|-------------|
| `POWHEGWorkflow` | Full workflow (default) |
| `POWHEGStage1` | Grid generation |
| `POWHEGStage2` | Integration |
| `POWHEGStage3GridCombine` | Grid combination (3 seeds) |
| `POWHEGStage3` | Event generation |
| `POWHEGStage4` | Event file output |
| `POWHEGAnalysis` | Analysis pipeline |
| `POWHEGAddWeights` | Event reweighting |

---

## Monitoring & Troubleshooting

### Check Job Status

```bash
# SLURM queue
squeue -u $USER

# Job history
sacct -u $USER --format=JobID,JobName,State,Elapsed
```

### Check LAW Status

```bash
# Task completion status
law run POWHEGWorkflow --config-file run.yaml --print-status 0

# What would run
law run POWHEGStage3 --config-file run.yaml --print-deps 1
```

### Reset/Rerun Tasks

```bash
# Remove output (reset task)
law run POWHEGStage3 --config-file run.yaml --remove-output 0

# Or delete markers manually
rm powheg_output/stage3/*.done

# Re-run
law run POWHEGStage3 --config-file run.yaml
```

### View Logs

```bash
# Job logs
tail logs/*.out
grep -i error logs/*.err

# LAW job data
ls powheg_output/slurm_jobs/
```

---

## Migration from v1.x

Version 2.0 uses LAW instead of plain Luigi. Key changes:

1. **Dependencies**: Now requires `law` package
2. **CLI**: Supports both `powheg-workflow` wrapper and `law run` commands
3. **Workflows**: Tasks inherit from `law.slurm.SlurmWorkflow`
4. **Containers**: Use LAW sandbox system instead of manual handling

### Upgrade Steps

```bash
# Update installation
cd /path/to/powheg-luigi-workflow
git pull
pip install --user -e .

# Existing run.yaml files work unchanged
powheg-workflow run.yaml
```

---

## Support

For issues or questions:
1. Check the logs in `logs/` and `powheg_output/slurm_jobs/`
2. Run with `--verbose` for detailed output
3. Use `--dry-run` to test configuration
4. Check LAW documentation: https://law.readthedocs.io/
