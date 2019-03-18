"""Package for collecting ACS and geospatial data from the Census API."""

import asyncio
import itertools
import logging
from operator import methodcaller
import os
from tempfile import NamedTemporaryFile

from aiohttp import ClientSession, ClientTimeout, TCPConnector
import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon
from tenacity import retry, stop_after_attempt, wait_random

from .errors import CensusAPIUnknownError, InvalidQueryError, MissingCredentialsError

# Import socrata-py if possible (optional; only needed for publishing dataset to Socrata)
try:
    from socrata import Socrata
    from socrata.authorization import Authorization
except ImportError:
    pass

# Configure logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Query:
    def __init__(
        self,
        estimate,              # 1; 3; 5
        years,                 # range(2011, 2017)
        variables,             # ['B01003_001E']
        for_geo,               # 'tract:*'
        in_geo=None,           # ['state:08', 'county:005']
        table='detail',        # 'detail'; 'profile'; 'subject'; 'cprofile'
        join_geography=True,   # True; False
        max_connections=50,    # Concurrent connections
        timeout=30,            # Seconds
        census_api_key=None    # Census API key
    ):
        self.years = years
        self.estimate = estimate
        self.variables = variables
        self.for_geo = for_geo
        self.in_geo = [] if in_geo is None else in_geo
        self.table = table
        self.join_geography = join_geography
        self.max_connections = max_connections
        self.timeout = timeout

        # Can't programmatically grab shapefiles for years prior to 2013; not available
        if join_geography is True and min(years) < 2013:
            raise InvalidQueryError('Sorry, cannot join geography for years prior to 2013')

        # If API key is not explicitly supplied, look it up under environment variable
        if census_api_key is None:
            self.census_api_key = os.getenv('CENSUS_API_KEY')
        else:
            self.census_api_key = census_api_key

    @classmethod
    def chunk_variables(cls, variables, max_size=48):
        """Given a series of variables, yield them in even chunks.

        Uses a default maximum size of 48 to avoid exceeding the Census
        API's limit of 50 variables per request (and leaving room for
        'NAME' and 'GEO_ID').
        """
        iterator = iter(variables)
        while True:
            chunk = tuple(itertools.islice(iterator, max_size))
            if chunk:
                yield chunk
            else:
                return

    def build_census_api_url(self, year):
        """Build a Census API URL based on the supplied parameters."""
        table_route_mappings = {
            'detail': '',
            'profile': '/profile',
            'subject': '/subject',
            'cprofile': '/cprofile'
        }
        table_route = table_route_mappings[self.table]
        url = f'https://api.census.gov/data/{year}/acs/acs{self.estimate}{table_route}'
        return url

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=5))
    async def fetch_acs_data(self, session, year, chunk):
        """Fetch a Census API result set for the supplied parameters."""
        # TODO: Use year, data tuples instead of inserting year column?
        url = self.build_census_api_url(year)
        params = [
            ('get', ','.join(['NAME', 'GEO_ID', *chunk])),  # get=NAME,B01003_001E
            ('for', self.for_geo),                          # for=tract:*
            *(('in', value) for value in self.in_geo),      # in=state:08&in=county:005
            ('key', self.census_api_key)
        ]
        logger.debug(f'Calling {url}')
        async with session.get(url, params=params) as response:
            if response.status == 500:
                raise CensusAPIUnknownError(f'Request to Census API failed: {response.url}')
            logger.debug(f'{response.url} response: {response.status}')
            response_json = await response.json(content_type=None)
            # Append year column
            response_json[0].append('year')
            for row in response_json[1:]:
                row.append(year)
            return response_json

    async def fetch_acs_variable_labels(self, session, year):
        """Fetch labels for the supplied ACS variables."""
        # TODO: Refactor this so we fetch per variable/year rather than entire variables.json
        url = '/'.join((self.build_census_api_url(year), 'variables.json'))
        logger.debug(f'Calling {url}')
        async with session.get(url) as response:
            logger.debug(f'{response.url} response: {response.status}')
            response_json = await response.json(content_type=None)
            variables = response_json['variables'].items()
            labels = {key: value['label'] for key, value in variables}
            return year, labels

    async def gather_results(self):
        """Gather calls to the Census API so they can be run at once."""
        fetch_calls = []
        chunks = self.chunk_variables(self.variables)
        chunks_by_year = itertools.product(chunks, self.years)
        async with ClientSession(
            timeout=ClientTimeout(self.timeout),
            connector=TCPConnector(limit=self.max_connections)
        ) as session:
            for chunk, year in chunks_by_year:
                fetch_calls.append(self.fetch_acs_data(session, year, chunk))
            for year in self.years:
                fetch_calls.append(self.fetch_acs_variable_labels(session, year))
            tasks = map(asyncio.create_task, fetch_calls)
            return await asyncio.gather(*tasks)

    def assemble_dataframe(self, results):
        """Given results from the Census API, assemble a dataframe."""
        # TODO: Break this function out into multiple other functions
        # TODO: Handle rows with, e.g., value == 'tract'
        # TODO: Finalize with nicer column names
        # Split results into data and variables
        data = results[:-len(self.years)]
        variables = dict(results[-len(self.years):])

        # Get list of geography types
        for_geo_type = self.for_geo[:self.for_geo.index(':')]
        in_geo_types = (value[:value.index(':')] for value in self.in_geo)
        geography_types = [for_geo_type, *in_geo_types]

        # Melt and concatenate dataframes
        dataframes = []
        for columns, *records in data:
            id_vars = ['NAME', 'GEO_ID', *geography_types, 'year']
            subset = pd.DataFrame \
                .from_records(records, columns=columns) \
                .melt(id_vars=id_vars) \
                .drop(columns=geography_types)
            dataframes.append(subset)
        dataframe = pd.concat(dataframes) \
            .sort_values(by=['variable', 'NAME', 'year']) \
            .reset_index(drop=True)

        # Apply variable labels
        # TODO: Could download variable labels as records and just do a join here instead
        def look_up_variable_label(row):
            try:
                return variables[row['year']][row['variable']]
            except KeyError:
                return pd.np.NaN
        dataframe['label'] = dataframe.apply(look_up_variable_label, axis=1)
        dataframe = dataframe.loc[dataframe['label'].notnull()]
        dataframe['label'] = dataframe['label'].str.replace('!!', ' - ')

        # Compute percent change
        dataframe['value'] = dataframe['value'].astype(float)
        dataframe['percent_change'] = dataframe \
            .groupby(['GEO_ID', 'variable'])['value'] \
            .pct_change() * 100
        dataframe['difference'] = dataframe \
            .groupby(['GEO_ID', 'variable'])['value'] \
            .diff()

        return dataframe

    def build_census_geospatial_url(self, year):
        """Build a Census shapefile URL based on the supplied parameters."""
        for_geo_type, _ = self.for_geo.split(':')
        in_geo = dict(pair.split(':') for pair in self.in_geo)
        state_fips = in_geo.get('state', '')
        prefix = 'shp/' if year > 2013 else ''
        geo_code_mappings = {
            'state': 'us_state',
            'zip code tabulation area': 'us_zcta510',
            'county': 'us_county',
            'tract': f'{state_fips}_tract',
            'place': f'{state_fips}_place'
        }
        geo_code = geo_code_mappings[for_geo_type]
        url = f'https://www2.census.gov/geo/tiger/GENZ{year}/{prefix}cb_{year}_{geo_code}_500k.zip'
        return url

    async def fetch_geospatial_data(self, session, year):
        """Fetch a Census shapefile for the supplied parameters.

        To work around geopandas/Fiona's limitations in opening zipped
        shapefiles, this downloads each shapefile to a temporary file on
        on disk, reads it into a geopandas dataframe, then deletes the
        temporary file.
        """
        url = self.build_census_geospatial_url(year)
        with NamedTemporaryFile(suffix='.zip') as temporary_file:
            logger.debug(f'Calling {url}')
            async with session.get(url) as response:
                logger.debug(f'{response.url} response: {response.status}')
                temporary_file.write(await response.read())
            subset = gpd.read_file(f'zip://{temporary_file.name}')
            subset['year'] = year
            return subset

    async def gather_geospatial_results(self):
        """Gather calls for shapefiles so they can be run at once."""
        fetch_calls = []
        async with ClientSession(
            timeout=ClientTimeout(self.timeout),
            connector=TCPConnector(limit=self.max_connections)
        ) as session:
            for year in self.years:
                fetch_calls.append(self.fetch_geospatial_data(session, year))
            tasks = map(asyncio.create_task, fetch_calls)
            return await asyncio.gather(*tasks)

    def join_geospatial_data(self, dataframe):
        """Given ACS data, join rows to geospatial points/boundaries."""
        results = asyncio.run(self.gather_geospatial_results())
        geo_dataframe = pd.concat(results, ignore_index=True)

        def coerce_shape_to_multipolygon(shape):
            if not isinstance(shape, MultiPolygon):
                return MultiPolygon([shape])
            else:
                return shape
        to_wkt = methodcaller('to_wkt')

        # Get centroids and serialize to WKT
        geo_dataframe['centroid'] = geo_dataframe.centroid.map(to_wkt)
        # Get representative points (guaranteed to be internal to polygons) and serialize to WKT
        geo_dataframe['representative_point'] = geo_dataframe['geometry'] \
            .representative_point() \
            .map(to_wkt)
        # Coerce geometry to series of MultiPolygons and serialize to WKT
        geo_dataframe['geometry'] = geo_dataframe['geometry'] \
            .map(coerce_shape_to_multipolygon) \
            .map(to_wkt)

        # Merge dataframes and return
        merged = dataframe.merge(
            geo_dataframe[['AFFGEOID', 'year', 'centroid', 'representative_point', 'geometry']],
            how='left',
            left_on=['GEO_ID', 'year'],
            right_on=['AFFGEOID', 'year']
        ).drop(columns='AFFGEOID')
        return merged

    def run(self):
        """Collect ACS data for the given parameters in a dataframe."""
        results = asyncio.run(self.gather_results())
        dataframe = self.assemble_dataframe(results)
        if self.join_geography is True:
            dataframe = self.join_geospatial_data(dataframe)
        return dataframe

    def collect_socrata_credentials_from_environment(self):
        """Collect Socrata auth credentials from the local environment.

        Looks up credentials under several common Socrata environment
        variable names, and returns the first complete pair it finds.
        Raises a MissingCredentialsError if no complete pair is found.
        """
        environment_variable_pairs = [
            ('SOCRATA_KEY_ID', 'SOCRATA_KEY_SECRET'),
            ('SOCRATA_USERNAME', 'SOCRATA_PASSWORD'),
            ('MY_SOCRATA_USERNAME', 'MY_SOCRATA_PASSWORD'),
            ('SODA_USERNAME', 'SODA_PASSWORD')
        ]
        for identifier, secret in environment_variable_pairs:
            try:
                credentials = (os.environ[identifier], os.environ[secret])
            except KeyError:
                continue
            else:
                return credentials
        else:
            raise MissingCredentialsError('No Socrata credentials found in local environment')

    def publish_to_socrata(self, dataframe, domain, auth=None, open_in_browser=True):
        """Publish an ACS dataframe to Socrata."""
        # TODO: Add logging
        # TODO: Use nice column names, add column metadata
        # TODO: Expand dataset metadata: title, description, source link
        if auth is None:
            auth = self.collect_socrata_credentials_from_environment()
        client = Socrata(Authorization(domain, *auth))
        dataset_name = f'ACS {self.estimate}-Year Estimates'
        revision, output = client.create(name=dataset_name).df(dataframe)
        ok, output = output.run()
        ok, job = revision.apply(output_schema=output)
        if open_in_browser is True:
            revision.open_in_browser()
