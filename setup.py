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
    url='https://github.com/your-repo/powheg-workflow',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'powheg_workflow': [
            '../config/clusters/*.yaml',
            '../config/scripts/*.sh',
        ],
    },
    install_requires=[
        'b2luigi>=0.9.0',   # BELLE2 Luigi variant
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
            'powgatory=powheg_workflow.cli:run_workflow',
        ],
    },
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Scientific/Engineering :: Physics',
    ],
    keywords='powheg physics workflow luigi law slurm hpc',
)
