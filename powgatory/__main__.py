#!/usr/bin/env python3
"""
Entry point for `python -m powgatory`.

Batch jobs start the workflow this way (see POWHEGStage.executable), so they do not
depend on where POWgatory is installed.
"""

import sys
from powgatory.cli import run_workflow


if __name__ == '__main__':
    sys.exit(run_workflow())

