from pathlib import Path

import fiona  # noqa
from geopandas import GeoDataFrame
import pandas as pd
from pandas import DataFrame
import pytest
from shapely import wkt


@pytest.fixture(scope='session')
def fixtures_path():
    return Path(__file__).parent / 'fixtures'


@pytest.fixture(scope='session')
def counties(fixtures_path: Path) -> DataFrame:
    dataframe = pd.read_csv(fixtures_path / 'counties.csv', parse_dates=['date'])
    dataframe['annotation'] = dataframe['annotation'].astype('object')
    return dataframe


@pytest.fixture(scope='session')
def counties_points(fixtures_path: Path) -> DataFrame:
    dataframe = pd.read_csv(fixtures_path / 'counties_points.csv', parse_dates=['date'])
    dataframe['annotation'] = dataframe['annotation'].astype('object')
    dataframe['geometry'] = dataframe['geometry'].map(wkt.loads)
    geodataframe = GeoDataFrame(dataframe, geometry='geometry')
    dataframe = DataFrame(geodataframe)
    return dataframe


@pytest.fixture(scope='session')
def counties_polygons(fixtures_path: Path) -> DataFrame:
    dataframe = pd.read_csv(fixtures_path / 'counties_polygons.csv', parse_dates=['date'])
    dataframe['annotation'] = dataframe['annotation'].astype('object')
    # dataframe['geometry'] = dataframe['geometry'].map(wkt.loads)
    # geodataframe = GeoDataFrame(dataframe, geometry='geometry')
    # dataframe = DataFrame(geodataframe)
    return dataframe
