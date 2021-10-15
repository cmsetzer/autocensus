import pytest

from autocensus.errors import MissingCredentialsError
from autocensus.socrata import build_dataset_name, look_up_socrata_credentials


@pytest.mark.parametrize(
    ['identifier', 'secret'],
    [
        ('SOCRATA_KEY_ID', 'SOCRATA_KEY_SECRET'),
        ('SOCRATA_USERNAME', 'SOCRATA_PASSWORD'),
        ('MY_SOCRATA_USERNAME', 'MY_SOCRATA_PASSWORD'),
        ('SODA_USERNAME', 'SODA_PASSWORD'),
    ],
)
def test_look_up_socrata_credentials(monkeypatch, identifier: str, secret: str):
    environment_variable_pairs = [
        ('SOCRATA_KEY_ID', 'SOCRATA_KEY_SECRET'),
        ('SOCRATA_USERNAME', 'SOCRATA_PASSWORD'),
        ('MY_SOCRATA_USERNAME', 'MY_SOCRATA_PASSWORD'),
        ('SODA_USERNAME', 'SODA_PASSWORD'),
    ]
    username = 'username@socrata.com'
    password = '12345678'
    for some_identifier, some_secret in environment_variable_pairs:
        monkeypatch.delenv(some_identifier, raising=False)
        monkeypatch.delenv(some_secret, raising=False)
        if (some_identifier, some_secret) == (identifier, secret):
            monkeypatch.setenv(identifier, username)
            monkeypatch.setenv(secret, password)

    assert look_up_socrata_credentials() == (username, password)

    monkeypatch.delenv(identifier, raising=False)
    monkeypatch.delenv(secret, raising=False)
    with pytest.raises(
        MissingCredentialsError, match='No Socrata credentials found in local environment'
    ):
        look_up_socrata_credentials()


def test_build_dataset_name():
    range_expected = 'American Community Survey 1-Year Estimates, 2013–2017'
    assert build_dataset_name(1, [2017, 2013, 2014]) == range_expected

    single_expected = 'American Community Survey 5-Year Estimates, 2018'
    assert build_dataset_name(5, [2018]) == single_expected
