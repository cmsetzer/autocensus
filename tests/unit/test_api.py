from autocensus.api import look_up_census_api_key
from autocensus.errors import MissingCredentialsError
import pytest


def test_look_up_census_api_key(monkeypatch):
    key_in_environment = 'abc'
    monkeypatch.setenv('CENSUS_API_KEY', key_in_environment)
    assert look_up_census_api_key('xyz') == 'xyz'
    assert look_up_census_api_key() == key_in_environment

    monkeypatch.delenv('CENSUS_API_KEY', raising=False)
    with pytest.raises(
        MissingCredentialsError, match='No Census API key found in local environment'
    ):
        look_up_census_api_key()


def test_look_up_census_api_key_with_key_copied_from_readme():
    with pytest.raises(MissingCredentialsError, match='A valid Census API key is required'):
        look_up_census_api_key('Your Census API key')
