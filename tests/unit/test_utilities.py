from datetime import datetime
from pathlib import Path
import shutil
from unittest.mock import Mock

from autocensus.constants import CACHE_DIRECTORY_PATH
from autocensus.errors import InvalidGeographyError, InvalidVariableError, InvalidYearError
from autocensus.geography import Geo
from autocensus.utilities import (
    check_geo_estimates,
    check_geo_hierarchy,
    check_years,
    chunk_variables,
    clear_cache,
    load_annotations_dataframe,
    parse_table_name_from_variable,
    wrap_scalar_value_in_list,
)
import pandas as pd
import pytest


def test_clear_cache(monkeypatch):
    path_exists_mock = Mock(return_value=True)
    shutil_rmtree_mock = Mock()
    monkeypatch.setattr(Path, 'exists', path_exists_mock)
    monkeypatch.setattr(shutil, 'rmtree', shutil_rmtree_mock)
    clear_cache()
    shutil_rmtree_mock.assert_called_once_with(CACHE_DIRECTORY_PATH)


def test_clear_cache_when_cache_dir_does_not_exist(monkeypatch):
    path_exists_mock = Mock(return_value=False)
    monkeypatch.setattr(Path, 'exists', path_exists_mock)
    result = clear_cache()
    assert result is False


def test_wrap_scalar_value_in_list():
    assert wrap_scalar_value_in_list(2015) == [2015]
    assert wrap_scalar_value_in_list('state:08') == ['state:08']
    assert wrap_scalar_value_in_list([2015, 2016]) == [2015, 2016]
    assert wrap_scalar_value_in_list(['state:08', 'state:48']) == ['state:08', 'state:48']


def test_chunk_variables():
    variables = [f'variable_{i}' for i in range(60)]
    chunks = list(chunk_variables(variables))
    assert len(chunks) == 2
    assert len(chunks[0]) == 48
    assert len(chunks[1]) == 12


def test_chunk_variables_with_max_size():
    variables = [f'variable_{i}' for i in range(60)]
    chunks = list(chunk_variables(variables, 10))
    assert len(chunks) == 6
    for chunk in chunks:
        assert len(chunk) == 10


def test_parse_table_name_from_variable():
    assert parse_table_name_from_variable('B01003_001E') == 'detail'
    assert parse_table_name_from_variable('C15002B_011E') == 'detail'
    assert parse_table_name_from_variable('CP02_2014_001E') == 'cprofile'
    assert parse_table_name_from_variable('DP05_0015E') == 'profile'
    assert parse_table_name_from_variable('S2503_C02_001E') == 'subject'


def test_parse_table_name_from_variable_with_invalid_variable():
    invalid_variable = 'A-12345'
    expected_error_message = f'Variable cannot be associated with an ACS table: {invalid_variable}'
    with pytest.raises(InvalidVariableError, match=expected_error_message):
        parse_table_name_from_variable(invalid_variable)


def test_load_annotations_dataframe():
    dataframe = load_annotations_dataframe()
    expected_annotations = pd.Series(
        [-999999999, -888888888, -666666666, -555555555, -333333333, -222222222], dtype='float64'
    )
    assert dataframe['value'].equals(expected_annotations)
    assert dataframe['annotation'].all()


def test_check_years():
    assert check_years([2015, 2016, 2017]) is True

    with pytest.raises(InvalidYearError, match='before 2005'):
        check_years([2004, 2005])

    current_year = datetime.today().year
    with pytest.raises(InvalidYearError, match=f'{current_year} or later'):
        check_years([current_year - 1, current_year])


def test_check_geo_hierarchy():
    assert check_geo_hierarchy([Geo('tract:*')], [Geo('state:48'), Geo('county:041')]) is True


def test_check_geo_hierarchy_with_invalid_hierarchy():
    # Tract in place
    with pytest.raises(InvalidGeographyError):
        check_geo_hierarchy([Geo('tract:*')], [Geo('place:24000')])

    # Tract without both state and county
    with pytest.raises(InvalidGeographyError):
        check_geo_hierarchy([Geo('tract:*')], [Geo('state:48')])

    # Place without state
    with pytest.raises(InvalidGeographyError):
        check_geo_hierarchy([Geo('place:24000')], [])

    # County without state
    with pytest.raises(InvalidGeographyError):
        check_geo_hierarchy([Geo('county:005')], [])


def test_check_geo_estimates():
    for_geo = [Geo('tract:*')]

    # Tracts with 5-year estimates
    assert check_geo_estimates(5, for_geo) is True

    # Tracts with 1- or 3-year estimates
    with pytest.raises(ValueError):
        check_geo_estimates(1, for_geo)
    with pytest.raises(ValueError):
        check_geo_estimates(3, for_geo)
