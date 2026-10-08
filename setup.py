#!/usr/bin/env python3
"""Setup script for POWHEG-BOX b2luigi Workflow"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_path = Path(__file__).parent / 'README.md'
long_description = readme_path.read_text() if readme_path.exists() else ''

setup(
    name='powgatory',
    version='2.0.0',
    description='POWHEG-BOX workflow orchestration using b2luigi',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='POWHEG Team',
    url='https://github.com/jakoblinder/POWgatory',
    packages=find_packages(),
    include_package_data=True,
    # Files read at runtime; they must be part of the installed package, not just the checkout.
    package_data={
        'powgatory': [
            'config/*.yaml',
            'config/*.sh',
            'config/pwgseeds.dat-save',
            'scripts/*.sh',
        ],
    },
    install_requires=[
        # BELLE2 Luigi variant. The fork adds Slurm array/mpi submission ("submission_type"),
        # which is not part of any b2luigi release yet.
        'b2luigi @ git+https://github.com/jakoblinder/b2luigi.git@slurm_array_submission',
        'luigi>=3.0.0',     # Workflow orchestration
        'pyyaml>=5.4',      # YAML configuration
    ],
    extras_require={
        'dev': [
            'pytest>=6.0',
            'pytest-cov',
            'black',
            'flake8',
        ],
        'slurm': [
            # No extra deps needed - b2luigi handles SLURM natively
        ],
    },
    entry_points={
        'console_scripts': [
            'powgatory=powgatory.cli:run_workflow',
        ],
    },
    python_requires='>=3.11',   # Matches the b2luigi fork providing array/mpi submission
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Scientific/Engineering :: Physics',
    ],
    keywords='powheg physics workflow luigi law slurm hpc',
)
