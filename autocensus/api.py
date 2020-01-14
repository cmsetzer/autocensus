"""Functions for retrieving data from the Census API."""

from asyncio import Future, gather
from contextlib import asynccontextmanager
import os
from pathlib import Path
from typing import AsyncGenerator, Dict, Iterable, List, Optional, Union
from warnings import warn

from aiohttp import ClientResponseError, ClientSession, ClientTimeout, TCPConnector
from tenacity import retry, stop_after_attempt, wait_exponential
from yarl import URL

from .errors import CensusAPIUnknownError, MissingCredentialsError
from .geography import Geo, determine_geo_code
from .utilities import CACHE_DIRECTORY_PATH

# Types
Table = List[List[Union[int, str]]]


def look_up_census_api_key(census_api_key: str = None) -> str:
    """Look up a Census API key from the local environment.

    If a key is passed as an argument, confirms that it was not copied
    directly from the example in the readme, then returns the key. If
    no key has been provided, looks for one under the environment
    variable CENSUS_API_KEY.
    """
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

    def __init__(self, census_api_key: str, verify_ssl: bool = True) -> None:
        self.census_api_key: str = census_api_key
        self.verify_ssl: bool = verify_ssl

    @asynccontextmanager
    async def create_session(self) -> AsyncGenerator[None, None]:
        """Furnish an HTTP session for re-use across API calls.

        Closes and deletes the session when we're done with it.
        """
        timeout = ClientTimeout(300)
        connector = TCPConnector(limit=50)
        async with ClientSession(timeout=timeout, connector=connector) as session:
            self._session = session
            yield
            await session.close()
            del self._session

    def build_url(self, estimate: int, year: int, table_name: str) -> URL:
        """Build a Census API URL for a given estimate, year, and table."""
        table_route_mappings = {
            'detail': '',
            'cprofile': '/cprofile',
            'profile': '/profile',
            'subject': '/subject',
        }
        table_route: str = table_route_mappings[table_name]
        url = URL(f'https://api.census.gov/data/{year}/acs/acs{estimate}{table_route}')
        return url

    def build_shapefile_url(self, year: int, for_geo: Geo, in_geo: Iterable) -> URL:
        """Build a Census shapefile URL based on the supplied parameters."""
        in_geo_dict: Dict[str, str] = {geo.type: geo.code for geo in in_geo}  # type: ignore
        state_fips = in_geo_dict.get('state', '')
        if year > 2013:
            base_url = URL(f'https://www2.census.gov/geo/tiger/GENZ{year}/shp')
        else:
            base_url = URL(f'https://www2.census.gov/geo/tiger/GENZ{year}')
        geo_code: str = determine_geo_code(year, for_geo.type, state_fips)
        resolution = '500k' if not for_geo.type == 'us' else '5m'
        url: URL = base_url / f'cb_{year}_{geo_code}_{resolution}.zip'
        return url

    @retry(
        wait=wait_exponential(multiplier=1, min=3, max=15),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def fetch_variable(
        self, estimate: int, year: int, table_name: str, variable: str
    ) -> dict:
        """Fetch a given variable definition from the Census API."""
        url: URL = self.build_url(estimate, year, table_name) / f'variables/{variable}.json'
        async with self._session.get(url, ssl=self.verify_ssl) as response:
            try:
                variable_json: dict = await response.json()
            except ClientResponseError:
                # Handle erroneous variable by returning a stub with variable/year
                variable_json = {'name': variable}
            variable_json['year'] = year
            return variable_json

    @retry(
        wait=wait_exponential(multiplier=1, min=3, max=15),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def fetch_table(
        self,
        estimate: int,
        year: int,
        table_name: str,
        variables: Iterable[str],
        for_geo: Geo,
        in_geo: Iterable[str],
    ) -> Table:
        """Fetch a given ACS data table from the Census API."""
        url: URL = self.build_url(estimate, year, table_name)
        params = [
            ('get', ','.join(['NAME', 'GEO_ID', *variables])),
            ('for', str(for_geo)),
            *(('in', str(geo)) for geo in in_geo),
            ('key', self.census_api_key),
        ]
        async with self._session.get(url, params=params, ssl=self.verify_ssl) as response:
            # Raise informative exception for non-200 response
            if response.status != 200:
                text: str = await response.text()
                raise CensusAPIUnknownError(f'Non-200 response from: {response.url}\n{text}')
            response_json: Table = await response.json()
            # Add geo_type
            response_json[0].extend(['geo_type', 'year'])
            for row in response_json[1:]:
                row.extend([for_geo.type, year])
            return response_json

    async def fetch_geography(self, year: int, for_geo: Geo, in_geo: Iterable) -> Optional[Path]:
        """Fetch a given shapefile and download it to the local cache.

        Returns a path to the cached shapefile. If the shapefile is
        already cached, skips the download and returns the path.

        If the shapefile URL returns a non-200 response, warns the user
        and returns a null value.
        """
        url: URL = self.build_shapefile_url(year, for_geo, in_geo)
        cached_filepath: Path = CACHE_DIRECTORY_PATH / url.name
        if not cached_filepath.exists():
            async with self._session.get(url, ssl=self.verify_ssl) as response:
                # Handle bad response or missing shapefile (if, e.g., it hasn't been released yet)
                if response.status != 200:
                    warn(f'Failed to obtain a Census boundary shapefile from {response.url}')
                    return None
                with open(cached_filepath, 'wb') as cached_file:
                    content = await response.content.read()
                    cached_file.write(content)
        return cached_filepath

    async def gather_calls(self, calls) -> Future:
        """Gather a series of fetch calls for concurrent scheduling."""
        async with self.create_session():
            gathered: Future = await gather(*calls)
            return gathered
