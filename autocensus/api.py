"""Functions for retrieving data from the Census API."""

from contextlib import contextmanager
import os
from pathlib import Path
from typing import Iterable, Iterator, List, Union

from requests import Response, Session
from tenacity import retry, stop_after_attempt, wait_exponential
from yarl import URL

from .errors import MissingCredentialsError
from .geography import determine_geo_code, extract_geo_type
from .utilities import CACHE_DIRECTORY_PATH

# Types
Table = List[List[Union[int, str]]]


def look_up_census_api_key(census_api_key: str = None) -> str:
    # If an API key was supplied, check whether it was copied from the readme
    if census_api_key == 'Your Census API key':
        census_api_url = 'https://www.census.gov/developers'
        raise MissingCredentialsError(f'A valid Census API key is required: {census_api_url}')
    elif census_api_key is not None:
        return census_api_key

    # If no API key was supplied, look in local environment
    try:
        local_key: str = os.environ['CENSUS_API_KEY']
    except KeyError as error:
        message = 'No Census API key found in local environment'
        raise MissingCredentialsError(message) from error
    else:
        return local_key


class CensusAPI:
    """A class for retrieving data from the Census API via HTTP."""
    def __init__(self, census_api_key: str, verify_ssl: bool = True):
        self.census_api_key: str = census_api_key
        self.verify_ssl: bool = verify_ssl

    @contextmanager
    def create_session(self) -> Iterator[None]:
        """Furnish an HTTP session for re-use across API calls.

        Closes and deletes the session when we're done with it.
        """
        with Session() as session:
            session.params = {'key': self.census_api_key}
            session.verify = self.verify_ssl
            self._session = session
            yield
            del self._session

    def build_url(self, estimate: int, year: int, table_name: str) -> URL:
        table_route_mappings = {
            'detail': '',
            'cprofile': '/cprofile',
            'profile': '/profile',
            'subject': '/subject'
        }
        table_route: str = table_route_mappings[table_name]
        url = URL(f'https://api.census.gov/data/{year}/acs/acs{estimate}{table_route}')
        return url

    def build_shapefile_url(self, year: int, for_geo: str, in_geo: Iterable) -> URL:
        """Build a Census shapefile URL based on the supplied parameters."""
        for_geo_type: str = extract_geo_type(for_geo)
        in_geo_dict = dict(pair.split(':') for pair in in_geo)
        state_fips = in_geo_dict.get('state', '')
        if year > 2013:
            base_url = URL(f'https://www2.census.gov/geo/tiger/GENZ{year}/shp')
        else:
            base_url = URL(f'https://www2.census.gov/geo/tiger/GENZ{year}')
        geo_code: str = determine_geo_code(year, for_geo_type, state_fips)
        url = base_url / f'cb_{year}_{geo_code}_500k.zip'
        return url

    def fetch_variable(self, estimate: int, year: int, table_name: str, variable: str) -> dict:
        url: URL = self.build_url(estimate, year, table_name) / f'variables/{variable}.json'
        response: Response = self._session.get(url)
        variable_json: dict = response.json()
        return variable_json

    @retry(wait=wait_exponential(multiplier=1, min=3, max=10), stop=stop_after_attempt(5))
    def fetch_table(
        self,
        estimate: int,
        year: int,
        table_name: str,
        variables: Iterable,
        for_geo: str,
        in_geo: Iterable
    ) -> Table:
        url: URL = self.build_url(estimate, year, table_name)
        params = {
            'get': ','.join(['NAME', 'GEO_ID', *variables]),
            'for': for_geo,
            'in': in_geo,
            'key': self.census_api_key
        }
        response: Response = self._session.get(url, params=params)
        response_json: Table = response.json()
        return response_json

    def fetch_geography(self, year: int, for_geo: str, in_geo: Iterable) -> Path:
        url: URL = self.build_shapefile_url(year, for_geo, in_geo)
        cached_filepath: Path = CACHE_DIRECTORY_PATH / url.name
        if not cached_filepath.exists():
            with open(cached_filepath, 'wb') as cached_file:
                response = self._session.get(url)
                cached_file.write(response.content)
        return cached_filepath
