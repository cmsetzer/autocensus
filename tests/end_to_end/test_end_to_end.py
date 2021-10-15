import os
from typing import Optional

from autocensus import Query
import fiona  # noqa
from pandas import DataFrame
import pytest
from shapely import wkt


@pytest.fixture(scope='function')
def query_params() -> dict:
    return {
        'estimate': 5,
        'years': [2014, 2019],
        'variables': [
            # Commuting
            'DP03_0025E',  # Mean commute time
            # Internet access
            'S2801_C01_012E',  # Having internet service
        ],
        'for_geo': ['county:*'],
        'in_geo': ['state:08'],
    }


@pytest.fixture(scope='session')
def domain() -> str:
    return os.environ['AUTOCENSUS_TEST_DOMAIN']


def test_query_instantiation(query_params: dict):
    query = Query(**query_params)
    for key in query_params:
        assert getattr(query, key) is not None


def test_query_instantiation_with_invalid_estimate(query_params: dict):
    query_params['estimate'] = 7
    with pytest.raises(ValueError, match='Please specify a valid estimate value'):
        Query(**query_params)


def test_query_instantiation_with_invalid_geometry(query_params: dict):
    query_params['geometry'] = 'invalid value'
    with pytest.raises(ValueError, match='Please specify a valid geometry value'):
        Query(**query_params)


def test_query_instantiation_with_resolution_but_not_polygons(query_params: dict, caplog):
    query_params['geometry'] = 'points'
    query_params['resolution'] = '500k'
    Query(**query_params)
    assert 'Warning: Specifying a resolution is only supported for polygons' in caplog.text


def test_query_instantiation_with_invalid_resolution(query_params: dict):
    query_params['geometry'] = 'polygons'
    query_params['resolution'] = 'invalid value'
    with pytest.raises(ValueError, match='Please specify a valid resolution value'):
        Query(**query_params)


def test_query_run(query_params: dict, counties: DataFrame):
    query = Query(**query_params)
    dataframe: DataFrame = query.run()
    assert dataframe.equals(counties)


def test_query_run_with_points_geometry(query_params: dict, counties_points: DataFrame):
    query_params['geometry'] = 'points'
    query = Query(**query_params)
    dataframe: DataFrame = query.run()
    assert dataframe.equals(counties_points)


def test_query_run_with_polygons_geometry(query_params: dict, counties_polygons: DataFrame):
    query_params['geometry'] = 'polygons'
    query = Query(**query_params)
    dataframe: DataFrame = query.run()
    dataframe['geometry'] = dataframe['geometry'].map(
        lambda geo: wkt.dumps(geo, rounding_precision=1)
    )
    assert dataframe.equals(counties_polygons)


@pytest.mark.parametrize('geometry', [None, 'points', 'polygons'])
def test_query_to_socrata(query_params: dict, domain: str, geometry: Optional[str]):
    query_params['variables'] = ['DP03_0025E']
    query_params['years'] = [2019]
    query_params['geometry'] = geometry
    query = Query(**query_params)
    revision_url = query.to_socrata(domain, open_in_browser=False)
    assert revision_url


def test_query_to_socrata_update_existing_dataset(query_params: dict, domain: str):
    query = Query(**query_params)
    revision_url = query.to_socrata(domain, open_in_browser=False, wait_for_finish=True)

    dataset_id: str = revision_url.parts[2]
    query_params['years'] = [2017]
    query_2 = Query(**query_params)
    revision_url_2 = query_2.to_socrata(domain, open_in_browser=False, dataset_id=dataset_id)
    assert revision_url_2
