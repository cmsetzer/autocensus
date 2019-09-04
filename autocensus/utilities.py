"""Utility functions for processing Census API data."""

from functools import lru_cache, wraps
from itertools import islice
from pathlib import Path
from pkg_resources import resource_stream
import re
from shutil import rmtree
from typing import Any, Callable, Iterable, Iterator, List, Union

from appdirs import user_cache_dir
import pandas as pd
from pandas import DataFrame
from titlecase import titlecase
from datetime import datetime

from .errors import InvalidVariableError

# Types
Chunk = List[str]

# Constants
CACHE_DIRECTORY_PATH = Path(user_cache_dir('autocensus', 'socrata'))


def forgive(*exceptions) -> Callable:
    """Gracefully ignore the specified exceptions when calling function.

    This is especially useful for skipping NA values in columns.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapped(value: Any) -> Any:
            try:
                return func(value)
            except tuple(exceptions):
                return value
        return wrapped
    return decorator


def clear_cache() -> bool:
    """Clear the autocensus cache."""
    rmtree(CACHE_DIRECTORY_PATH)
    cache_is_cleared: bool = not CACHE_DIRECTORY_PATH.exists()
    return cache_is_cleared


def wrap_scalar_value_in_list(value: Union[Iterable, int, str]) -> Iterable[Union[int, str]]:
    """If a string or integer is passed, wrap it in a list."""
    if isinstance(value, (int, str)):
        return [value]
    else:
        return value


def chunk_variables(variables: Iterable[str], max_size: int = 48) -> Iterator[Chunk]:
    """Given a series of variables, yield them in even chunks.

    Uses a default maximum size of 48 to avoid exceeding the Census
    API's limit of 50 variables per request (and leaving room for 'NAME'
    and 'GEO_ID').
    """
    iterator = iter(variables)
    while True:
        chunk: Chunk = list(islice(iterator, max_size))
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


@forgive(TypeError)
def tidy_variable_label(value: str) -> str:
    """Tidy a variable label to make it human-friendly."""
    estimate_trimmed = re.sub(r'^Estimate!!', '', value)
    delimiters_replaced = re.sub(r'!!', ' - ', estimate_trimmed)
    return delimiters_replaced


@forgive(TypeError)
def titleize_text(value: str) -> str:
    """Convert a text string to title case."""
    return titlecase(value)


def load_annotations_dataframe() -> DataFrame:
    """Load the included annotations.csv resource as a dataframe."""
    annotations_csv = resource_stream(__name__, 'resources/annotations.csv')
    dataframe = pd.read_csv(annotations_csv, dtype={'value': float})
    return dataframe

def check_estimate_year(estimate):
    if estimate != 1 and estimate != 3 and estimate != 5:
        raise ValueError('Please enter an estimate of 1, 3, or 5')
    else:
        return

def check_years(years):
    current_year = datetime.today().year
    if type(years) == 'int':
        years = [years]
    if min(years) < 2009:
        raise ValueError('The Census API does not contain data before 2009')
    elif max(years) == current_year:
        raise ValueError(f'The Census API does not yet contain data from {current_year} or later')
    else:
        return

def check_geo_combinations(in_geo,for_geo):
    geo_url = 'https://api.census.gov/data/2017/acs/acs5/geography.html'
    in_geo = str(in_geo)
    for_geo = str(for_geo)
    if 'tract' in for_geo and ('state' not in in_geo or 'county' not in in_geo):
        raise ValueError(f'Queries by tract must include state and county in in_geo. See {geo_url}.')
    elif 'tract' in for_geo and 'place' in in_geo:
        raise ValueError(f'Queries by tract cannot have place in in_geo. See {geo_url}.')
    elif 'place' in for_geo and 'state' not in in_geo:
        raise ValueError(f'Queries by place must have state in in_geo. See {geo_url}.')
    elif 'place' in for_geo and in_geo == '':
        raise ValueError(f'Queries by place must also have state in in_geo. See {geo_url}.')
    elif 'county' in for_geo and in_geo == '':
        raise ValueError(f'Queries by county must also have state in in_geo. See {geo_url}.')
    else:
        return

def check_geo_estimates(for_geo,estimate):
    estimate = str(estimate)
    for_geo = str(for_geo)
    if 'tract' in estimate and (estimate == '1' or estimate == '3'):
        raise ValueError(f'Queries by tract can only be performed with 5-year estimates.')
    else:
        pass
