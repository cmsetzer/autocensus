from pandas import DataFrame
import pytest
from shapely import wkt

from autocensus import Query


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


def test_query_instantiation(query_params: dict):
    query = Query(**query_params)
    for key in query_params:
        assert getattr(query, key) is not None


def test_query_instantiation_with_invalid_estimate(query_params: dict):
    query_params['estimate'] = 7
    with pytest.raises(ValueError, match='Please specify a valid estimate'):
        Query(**query_params)


def test_query_instantiation_with_invalid_geometry(query_params: dict):
    query_params['geometry'] = 'invalid value'
    with pytest.raises(ValueError, match='Please specify a valid geometry'):
        Query(**query_params)


def test_query_instantiation_with_resolution_but_not_polygons(query_params: dict, caplog):
    query_params['geometry'] = 'points'
    query_params['resolution'] = '500k'
    Query(**query_params)
    assert 'Warning: Specifying a resolution is only supported for polygons' in caplog.text


def test_query_instantiation_with_invalid_resolution(query_params: dict):
    query_params['geometry'] = 'polygons'
    query_params['resolution'] = 'invalid value'
    with pytest.raises(ValueError, match='Please specify a valid resolution'):
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
