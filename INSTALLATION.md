# Installation Guide - POWHEG-BOX LAW Workflow

## Quick Install

```bash
# 1. Navigate to the workflow directory
cd /path/to/powheg-luigi-workflow

# 2. Install with LAW support
pip install --user -e .

# 3. Verify installation
powheg-workflow --help
law --help
which powheg-workflow

# Done! You can now use `powheg-workflow` or `law run` from any directory.
```

## Full Installation Steps

### Step 1: Requirements

- **Python 3.7+** (3.8+ recommended)
- **pip** (package installer)
- **SLURM** (for cluster submission)
- **Apptainer** (for containers, optional)

### Step 2: Get the Workflow System

If you have it as a git repository:
```bash
git clone <repo-url> powheg-luigi-workflow
cd powheg-luigi-workflow
```

Or if you already have the directory:
```bash
cd /u/jlinder/utilities/powheg-luigi-workflow
```

### Step 3: Install the Workflow Tool

#### Option A: User Install (Recommended)

```bash
pip install --user -e .
# Installs:
#   - ~/.local/bin/powheg-workflow (wrapper CLI)
#   - law package with `law` command
#   - luigi package
```

#### Option B: Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e .
# Remember to activate: source venv/bin/activate
```

#### Option C: Conda Environment

```bash
conda create -n powheg python=3.10
conda activate powheg
pip install -e .
```

### Step 4: Verify Installation

```bash
# Check wrapper CLI
powheg-workflow --help

# Check LAW installation
law --help
law index --help

# Check which is used
which powheg-workflow
which law
```

If not found, add to PATH:

```bash
export PATH="~/.local/bin:$PATH"
# Add to ~/.bashrc for permanent
echo 'export PATH="~/.local/bin:$PATH"' >> ~/.bashrc
```

---

## LAW Configuration (Optional)

LAW uses a `law.cfg` configuration file. You can copy the example:

```bash
# Copy example config to your run directory
cp /path/to/powheg-luigi-workflow/law.cfg.example ./law.cfg
```

Or set LAW home directory:

```bash
export LAW_HOME=$(pwd)/.law
mkdir -p $LAW_HOME
```

### Index Tasks for Completion

LAW provides shell auto-completion:

```bash
# Index tasks (creates ~/.law/index)
law index --modules powheg_workflow.tasks

# Enable completion
source "$(law completion)"
```

---

## Using the Workflow

For each POWHEG run:

```bash
# Create run directory
mkdir my_powheg_run
cd my_powheg_run

# Copy executables and templates
cp ~/powheg/pwhg_main .
cp ~/powheg/powheg.input-save .
cp ~/powheg/pwgseeds.dat-save .

# Copy and edit configuration
cp /path/to/powheg-luigi-workflow/example_run.yaml run.yaml
nano run.yaml

# Option 1: Run using wrapper
powheg-workflow run.yaml

# Option 2: Run using LAW directly
law run POWHEGWorkflow --config-file run.yaml
```

---

## Quick Configuration

Edit `run.yaml`:

```yaml
cluster: mpi              # Change to: viper, raven, or local

powheg_executable: "./pwhg_main"
powheg_input_template: "powheg.input-save"

stages:
  stage1: {enabled: true, grid_iterations: 3}
  stage2: {enabled: true}
  stage3: {enabled: true, grid_combination: true}
  stage4: {enabled: true}

resources:
  stage1:
    ntasks: 100
    time: "24:00:00"
    partition: alma

powheg_parameters:
  ncall1: 50000         # Override defaults
```

---

## What Gets Installed?

The installation creates:

1. **`powheg-workflow` command** - Wrapper CLI
2. **`law` command** - LAW CLI for native workflow execution
3. **Python packages** - `law`, `luigi`, `pyyaml`
4. **`powheg_workflow` module** - Task and framework definitions

Additionally, the package includes:
- **Cluster configs** - In `config/clusters/` (mpi.yaml, viper.yaml, raven.yaml, local.yaml)
- **SLURM templates** - In `config/templates/` (powheg_mpi.sh-save, law_bootstrap.sh, etc.)
- **LAW config example** - `law.cfg.example`

**Per run, you only need:**
- POWHEG executables (`pwhg_main`, `lhef_analysis`, etc.)
- Input templates (`powheg.input-save`, `pwgseeds.dat-save`)
- Single `run.yaml` configuration file

---

## Troubleshooting

### "Command not found: powheg-workflow"

```bash
# Reinstall
cd /path/to/powheg-luigi-workflow
pip install --user --upgrade -e .

# Check PATH
echo $PATH
export PATH="$HOME/.local/bin:$PATH"
```

### "Command not found: law"

```bash
# LAW should be installed automatically, but if not:
pip install --user law
```

### "No module named 'law'"

```bash
pip install --user law luigi pyyaml
```

### "No module named 'powheg_workflow'"

```bash
# Reinstall in editable mode
cd /path/to/powheg-luigi-workflow
pip install --user -e .
```

### Python version issues

LAW requires Python 3.7+. If your default Python is older:

```bash
# Use specific Python version
python3.9 -m venv venv
source venv/bin/activate
pip install -e .
```

### Permission issues

```bash
pip install --user -e .   # Use --user flag
```

### Editing installed workflow

Since we use `-e` (editable install), you can modify the workflow code and it takes effect immediately:

```bash
# Edit task definitions
nano /path/to/powheg-luigi-workflow/powheg_workflow/tasks.py

# Changes take effect immediately, no reinstall needed
```

---

## Upgrading from v1.x

If you have the previous plain-Luigi version:

```bash
# Update installation
cd /path/to/powheg-luigi-workflow
git pull  # or update files
pip install --user --upgrade -e .

# Verify LAW is installed
law --help
```

Your existing `run.yaml` files should work unchanged with the new version.

---

## Uninstall

```bash
# Remove packages
pip uninstall powheg-workflow law luigi

# Optionally remove the directory
rm -rf /path/to/powheg-luigi-workflow
```

---

## Next Steps

See **README.md** for:
- Complete LAW usage guide
- LAW commands (`law run`, `law index`, etc.)
- Configuration reference
- Multi-cluster support
- Command-line options
- Troubleshooting tips
