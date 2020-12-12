"""Functions for retrieving data from the Census API."""

from asyncio import Future, gather
from contextlib import asynccontextmanager
from io import BytesIO
from json.decoder import JSONDecodeError
import logging
from logging import Logger
import os
from pathlib import Path
from typing import AsyncGenerator, Dict, Iterable, Optional

from httpx import AsyncClient, Limits, Response
import pandas as pd
from pandas import DataFrame
from tenacity import retry, stop_after_attempt, wait_exponential
from typing_extensions import Literal
from yarl import URL

from .constants import Table
from .errors import CensusAPIUnknownError, MissingCredentialsError
from .geography import Geo, determine_gazetteer_code, determine_geo_code
from .utilities import CACHE_DIRECTORY_PATH

# Initialize logger
logger: Logger = logging.getLogger(__name__)


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

    def __init__(self, census_api_key: str) -> None:
        self.census_api_key: str = census_api_key

    @asynccontextmanager
    async def create_session(self) -> AsyncGenerator[None, None]:
        """Furnish an HTTP session for re-use across API calls.

        Closes and deletes the session when we're done with it.
        """
        async with AsyncClient(
            timeout=300, limits=Limits(max_keepalive_connections=10)
        ) as session:
            self._session = session
            yield
            await session.aclose()
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

    def build_gazetteer_url(self, year: int, gazetteer_code: str) -> URL:
        """Build a Gazetteer file URL based on the supplied parameters."""
        base_url = URL(
            f'https://www2.census.gov/geo/docs/maps-data/data/gazetteer/{year}_Gazetteer'
        )
        url: URL = base_url / f'{year}_Gaz_{gazetteer_code}_national.zip'
        return url

    def build_shapefile_url(
        self,
        year: int,
        for_geo: Geo,
        in_geo: Iterable,
        resolution: Optional[Literal['500k', '5m', '20m']] = None,
    ) -> URL:
        """Build a Census shapefile URL based on the supplied parameters."""
        in_geo_dict: Dict[str, str] = {geo.type: geo.code for geo in in_geo}  # type: ignore
        state_fips = in_geo_dict.get('state', '')
        if year > 2013:
            base_url = URL(f'https://www2.census.gov/geo/tiger/GENZ{year}/shp')
        else:
            base_url = URL(f'https://www2.census.gov/geo/tiger/GENZ{year}')
        geo_code: str = determine_geo_code(year, for_geo.type, state_fips)

        # Determine shapefile resolution (defaults to 1 : 500,000, except for U.S. outline)
        if resolution is None:
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
        response: Response = await self._session.get(str(url))
        try:
            variable_json: dict = response.json()
        except JSONDecodeError:
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
        response: Response = await self._session.get(str(url), params=params)  # type: ignore
        # Raise informative exception for non-200 response
        if response.status_code != 200:
            raise CensusAPIUnknownError(f'Non-200 response from: {response.url}\n{response.text}')
        response_json: Table = response.json()
        # Add geo_type
        response_json[0].extend(['geo_type', 'year'])
        for row in response_json[1:]:
            row.extend([for_geo.type, year])
        return response_json

    async def fetch_gazetteer_file(self, year: int, for_geo: Geo) -> Optional[DataFrame]:
        """Fetch a given Gazetteer table as a dataframe.

        If the Gazetteer file URL returns a non-200 response, warns the
        user and returns a null value.
        """
        gazetteer_code: Optional[str] = determine_gazetteer_code(year, for_geo.type)
        if gazetteer_code is None:
            logger.warning(
                f'Warning: Failed to obtain a Gazetteer file for geography type "{for_geo.type}"'
            )
            return None

        url: URL = self.build_gazetteer_url(year, gazetteer_code)
        response: Response = await self._session.get(str(url))
        if response.status_code != 200:
            logger.warning(f'Warning: Failed to obtain a Gazetteer file from {response.url}')
            return None

        # Fetch zip file as an in-memory object and read it into a dataframe
        zip_file = BytesIO(response.content)
        dataframe: DataFrame = pd.read_table(
            zip_file, encoding='latin-1', compression='zip', dtype=str
        ).applymap(str.strip)
        dataframe.columns = [column.strip() for column in dataframe.columns]
        dataframe = dataframe.loc[:, ['GEOID', 'INTPTLAT', 'INTPTLONG']]
        dataframe['INTPTLAT'] = dataframe['INTPTLAT'].astype(float)
        dataframe['INTPTLONG'] = dataframe['INTPTLONG'].astype(float)
        dataframe['gazetteer_geo_type'] = for_geo.type
        dataframe['year'] = year
        return dataframe

    async def fetch_shapefile(
        self,
        year: int,
        for_geo: Geo,
        in_geo: Iterable,
        resolution: Optional[Literal['500k', '5m', '20m']],
    ) -> Optional[Path]:
        """Fetch a given shapefile and download it to the local cache.

        Returns a path to the cached shapefile. If the shapefile is
        already cached, skips the download and returns the path.

        If the shapefile URL returns a non-200 response, warns the user
        and returns a null value.
        """
        url: URL = self.build_shapefile_url(year, for_geo, in_geo, resolution)
        cached_filepath: Path = CACHE_DIRECTORY_PATH / url.name
        if not cached_filepath.exists():
            response: Response = await self._session.get(str(url))
            # Handle bad response or missing shapefile (if, e.g., it hasn't been released yet)
            if response.status_code != 200:
                logger.warning(
                    f'Warning: Failed to obtain a Census boundary shapefile from {response.url}'
                )
                return None
            with open(cached_filepath, 'wb') as cached_file:
                cached_file.write(response.content)
        return cached_filepath

    async def gather_calls(self, calls) -> Future:
        """Gather a series of fetch calls for concurrent scheduling."""
        async with self.create_session():
            gathered: Future = await gather(*calls)
            return gathered
