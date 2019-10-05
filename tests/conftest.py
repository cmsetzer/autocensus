from pathlib import Path

import pytest


@pytest.fixture(scope='session')
def fixtures_path():
    return Path(__file__).parent / 'fixtures'


@pytest.fixture(scope='session')
def shapefile_path(fixtures_path):
    return fixtures_path / 'cb_2018_us_state_20m.zip'
