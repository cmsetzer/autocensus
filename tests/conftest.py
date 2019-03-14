"""Configuration and fixtures for autocensus tests."""

from aiohttp import ClientSession
from aioresponses import aioresponses
import pytest

from autocensus import Query


@pytest.fixture
async def session():
    async with ClientSession() as session:
        yield session


@pytest.fixture
async def mocked():
    with aioresponses() as mocked:
        yield mocked


@pytest.fixture
def instance(request):
    query = Query(
        estimate=5,
        years=range(2013, 2018),
        variables=['B01002_001E', 'B03001_001E'],
        for_geo='tract:*',
        in_geo=['state:08', 'county:005'],
        table='detail',
        census_api_key='abcdef'
    )
    return query


@pytest.fixture
def acs_data():
    payload = [
        ['NAME', 'GEO_ID', 'B01002_001E'],
        ['Census Tract 1', '123456', '1']
    ]
    return payload


@pytest.fixture
def variables():
    variables = {
        'variables': {
            'B01002_001E': {
                'label': 'Estimate!!Median age!!Total'
            }
        }
    }
    return variables
