"""Automated tests for autocensus."""

import pytest

from autocensus import Query


def test_join_geography_with_year_prior_to_2013():
    with pytest.raises(RuntimeError):
        Query(
            estimate=5,
            years=range(2012, 2014),
            variables=['B01002_001E'],
            for_geo='state:*',
            join_geography=True
        )


def test_chunk_variables():
    expected = [(0, 1, 2), (3, 4, 5), (6, 7)]
    result = list(Query.chunk_variables(range(8), max_size=3))
    assert result == expected


def test_build_census_api_url(instance):
    year = 2013
    if instance.table != 'detail':
        table_route = f'/{instance.table}'
    else:
        table_route = ''
    expected = f'https://api.census.gov/data/{year}/acs/acs{instance.estimate}{table_route}'
    assert instance.build_census_api_url(year) == expected


@pytest.mark.asyncio
async def test_fetch_acs_data(session, mocked, instance, acs_data):
    # TODO: Better way to match exact order of URL params? aiohttp doesn't seem to preserve order
    url = 'https://api.census.gov/data/2013/acs/acs5?for=tract:*&get=NAME,GEO_ID,B01002_001E' \
        '&in=county:005&in=state:08&key=abcdef'
    mocked.get(url, payload=acs_data)
    result = await instance.fetch_acs_data(session, 2013, ['B01002_001E'])
    acs_data[0].append('year')
    acs_data[1].append(2013)
    assert result == acs_data


@pytest.mark.asyncio
async def test_fetch_acs_variable_labels(session, mocked, instance, variables):
    url = 'https://api.census.gov/data/2013/acs/acs5/variables.json'
    mocked.get(url, payload=variables)
    result = await instance.fetch_acs_variable_labels(session, 2013)
    assert result == (2013, {'B01002_001E': 'Estimate!!Median age!!Total'})
