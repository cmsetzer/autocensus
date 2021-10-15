"""Utility functions for processing Census API data."""

from datetime import datetime
from functools import lru_cache
from itertools import islice
import logging
from logging import Logger
import shutil
from typing import Iterable, Iterator, List, Union

import pandas as pd
from pandas import DataFrame
import pkg_resources

from .constants import CACHE_DIRECTORY_PATH
from .errors import InvalidGeographyError, InvalidVariableError, InvalidYearError
from .geography import Geo

# Initialize logger
logger: Logger = logging.getLogger(__name__)


def clear_cache() -> bool:
    """Clear the autocensus cache."""
    if not CACHE_DIRECTORY_PATH.exists():
        logger.warning(f'Warning: Cache directory not found: {CACHE_DIRECTORY_PATH}')
        return False
    shutil.rmtree(CACHE_DIRECTORY_PATH)
    cache_is_cleared: bool = not CACHE_DIRECTORY_PATH.exists()
    return cache_is_cleared


def wrap_scalar_value_in_list(value: Union[Iterable, int, str]) -> Iterable:
    """If a string or integer is passed, wrap it in a list."""
    if isinstance(value, (int, str)):
        return [value]
    else:
        return value


def chunk_variables(variables: Iterable[str], max_size: int = 48) -> Iterator[List[str]]:
    """Given a series of variables, yield them in even chunks.

    Uses a default maximum size of 48 to avoid exceeding the Census
    API's limit of 50 variables per request (and leaving room for 'NAME'
    and 'GEO_ID').
    """
    iterator = iter(variables)
    while True:
        chunk: List[str] = list(islice(iterator, max_size))
        if chunk:
            yield chunk
        else:
            return


@lru_cache(maxsize=1024)
def parse_table_name_from_variable(variable: str) -> str:
    """Given an ACS variable name, determine its associated table."""
    # Extract table code from variable name
    end_of_table_code: int = [character.isdigit() for character in variable].index(True)
    table_code: str = variable[:end_of_table_code]
    # Map table code to table name
    mappings = {'B': 'detail', 'C': 'detail', 'CP': 'cprofile', 'DP': 'profile', 'S': 'subject'}
    try:
        table_name: str = mappings[table_code]
    except KeyError as error:
        message = f'Variable cannot be associated with an ACS table: {variable}'
        raise InvalidVariableError(message) from error
    else:
        return table_name


def load_annotations_dataframe() -> DataFrame:
    """Load the included annotations.csv resource as a dataframe."""
    annotations_csv = pkg_resources.resource_stream(__name__, 'resources/annotations.csv')
    dataframe = pd.read_csv(annotations_csv, dtype={'value': float})
    return dataframe


def check_years(years: Iterable) -> bool:
    """Validate a range of query years.

    Raises an InvalidYearError if a given year falls outside the
    expected window of data available from the Census API.
    """
    current_year: int = datetime.today().year
    if min(years) < 2005:
        raise InvalidYearError('The Census API does not contain ACS data from before 2005')
    elif max(years) >= current_year:
        raise InvalidYearError(
            f'The Census API does not yet contain data from {current_year} or later'
        )
    else:
        return True


def check_geo_hierarchy(for_geo: Iterable[Geo], in_geo: Iterable[Geo]) -> bool:
    """Validate a given hierarchy of for_geo and in_geo values.

    Raises an InvalidGeographyError for invalid hierarchies that come up
    often.
    """
    for_geo_types = {geo.type for geo in for_geo}
    in_geo_types = {geo.type for geo in in_geo}

    geo_url = 'https://api.census.gov/data/2017/acs/acs5/geography.html'
    if ('tract' in for_geo_types) and ('place' in in_geo_types):
        raise InvalidGeographyError(f'Queries by tract cannot have place in in_geo. See {geo_url}')
    elif ('tract' in for_geo_types) and not {'state', 'county'}.issubset(in_geo_types):
        raise InvalidGeographyError(
            f'Queries by tract must include state and county. See {geo_url}'
        )
    elif ('place' in for_geo_types) and ('state' not in in_geo_types):
        raise InvalidGeographyError(f'Queries by place must have state in in_geo. See {geo_url}')
    elif ('county' in for_geo_types) and ('state' not in in_geo_types):
        raise InvalidGeographyError(
            f'Queries by county must also have state in in_geo. See {geo_url}'
        )
    else:
        return True


def check_geo_estimates(estimate: int, for_geo: Iterable) -> bool:
    """Validate a given estimate in combination with for_geo values.

    Raises an InvalidGeographyError for invalid combinations that come
    up often.
    """
    for_geo_types = {geo.type for geo in for_geo}
    if estimate in [1, 3] and 'tract' in for_geo_types:
        raise ValueError('Queries by tract can only be performed with 5-year estimates')
    else:
        return True
