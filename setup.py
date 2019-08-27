#!/usr/bin/env python3

"""Setup script for autocensus."""

import os
from setuptools import setup, find_packages
import sys

# Enforce Python version
if sys.version_info < (3, 7):
    raise SystemExit('autocensus requires Python 3.7 or higher')

# Get long description from readme
local_path = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(local_path, 'readme.md')) as f:
    long_description = f.read()

setup(
    name='autocensus',
    version='1.0.1',
    description='A tool for collecting ACS and geospatial data from the Census API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/socrata/autocensus',
    author='Christopher Setzer',
    author_email='chris.setzer@socrata.com',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.7'
    ],
    keywords='census acs api open data socrata',
    packages=find_packages(),
    python_requires='>=3.7',
    install_requires=[
        'aiohttp>=3.0.0',
        'appdirs',
        'Fiona',
        'geopandas',
        'nest-asyncio',
        'pandas>=0.24.1',
        'Shapely',
        'socrata-py',
        'tenacity',
        'titlecase',
        'yarl'
    ],
    include_package_data=True
)
