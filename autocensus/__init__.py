"""Package for collecting ACS and geospatial data from the Census API."""

import asyncio
from functools import reduce
from itertools import islice, product
import logging
import os
from pkg_resources import resource_stream
from tempfile import NamedTemporaryFile

from aiohttp import ClientSession, ClientTimeout, TCPConnector
import geopandas as gpd
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_random

from .errors import (
    CensusAPIUnknownError,
    MissingCredentialsError,
    MissingDependencyError
)
from .utilities import (
    change_column_metadata,
    coerce_polygon_to_multipolygon,
    flatten_geometry,
    serialize_to_wkt,
    titleize_text
)
from . import topics  # noqa: F401

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
        years,                 # range(2013, 2017); 2013
        variables,             # ['B01003_001E', 'B01002_001E']; 'B01003_001E'
        for_geo,               # 'tract:*'
        in_geo=tuple(),        # ['state:08', 'county:005']; 'state:08'
        table='detail',        # 'detail'; 'profile'; 'subject'; 'cprofile'
        join_geography=True,   # True; False
        max_connections=50,    # Concurrent connections
        timeout=120,           # Seconds
        census_api_key=None    # Census API key
    ):
        self.years = [years] if isinstance(years, int) else years
        self.estimate = estimate
        self.variables = [variables] if isinstance(variables, str) else variables
        self.for_geo = for_geo
        self.in_geo = [in_geo] if isinstance(in_geo, str) else in_geo
        self.table = table
        self.join_geography = join_geography
        self.max_connections = max_connections
        self.timeout = timeout

        # If API key is not explicitly supplied, look it up under environment variable
        if census_api_key is None:
            try:
                self.census_api_key = os.environ['CENSUS_API_KEY']
            except KeyError:
                raise MissingCredentialsError('No Census API key found in local environment')
        elif census_api_key == 'Your Census API key':
            census_api_url = 'https://www.census.gov/developers'
            raise MissingCredentialsError(f'A valid Census API key is required: {census_api_url}')
        else:
            self.census_api_key = census_api_key

    @property
    def query_name(self):
        """Produce a nicely formatted query name."""
        if len(self.years) > 1:
            years_range = '{}–{}'.format(min(self.years), max(self.years))
        else:
            years_range = self.years[0]
        name = f'American Community Survey {self.estimate}-Year Estimates, {years_range}'
        return name

    @property
    def years_with_geography(self):
        """Select years for which geography should be obtained."""
        return [year for year in self.years if year >= 2013 and self.join_geography is True]

    @classmethod
    def chunk_variables(cls, variables, max_size=48):
        """Given a series of variables, yield them in even chunks.

        Uses a default maximum size of 48 to avoid exceeding the Census
        API's limit of 50 variables per request (and leaving room for
        'NAME' and 'GEO_ID').
        """
        iterator = iter(variables)
        while True:
            chunk = tuple(islice(iterator, max_size))
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
        url = self.build_census_api_url(year)
        params = [
            ('get', ','.join(['NAME', 'GEO_ID', *chunk])),  # get=NAME,B01003_001E
            ('for', self.for_geo),                          # for=tract:*
            *(('in', value) for value in self.in_geo),      # in=state:08&in=county:005
            ('key', self.census_api_key)
        ]
        logger.debug(f'Calling {url}')
        async with session.get(url, params=params) as response:
            if response.status != 200:
                raise CensusAPIUnknownError(f'Request to Census API failed: {response.url}')
            logger.debug(f'{response.url} response: {response.status}')
            response_json = await response.json(content_type=None)
            # Append year column
            response_json[0].append('year')
            for row in response_json[1:]:
                row.append(year)
            return response_json

    async def fetch_acs_variable_labels(self, session, year):
        """Fetch labels for the supplied ACS variables.

        Pulls down a JSON array of rows representing variable labels
        for a given query and year, then collects the results in a
        dataframe for easy joining to data.
        """
        url = '/'.join((self.build_census_api_url(year), 'variables'))
        logger.debug(f'Calling {url}')
        async with session.get(url) as response:
            logger.debug(f'{response.url} response: {response.status}')
            response_json = await response.json(content_type=None)
            columns, *rows = response_json
            labels = pd.DataFrame(rows, columns=columns)
            labels['year'] = year
            return labels

    def build_census_geospatial_url(self, year):
        # TODO: Investigate querying Census TIGERweb GeoServices REST API for geospatial data
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

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=5))
    async def fetch_geospatial_data(self, session, year):
        """Fetch a Census shapefile for the supplied parameters.

        To work around geopandas/Fiona's limitations in opening zipped
        shapefiles, this downloads each shapefile to a temporary file on
        disk, reads it into a geopandas dataframe, then deletes the
        temporary file.
        """
        url = self.build_census_geospatial_url(year)
        with NamedTemporaryFile(suffix='.zip') as temporary_file:
            logger.debug(f'Calling {url}')
            async with session.get(url) as response:
                logger.debug(f'{response.url} response: {response.status}')
                temporary_file.write(await response.read())
            # Temporarily set environment variable to avoid showing needless vsizip recode warnings
            os.environ['CPL_ZIP_ENCODING'] = 'UTF-8'
            subset = gpd.read_file(f'zip://{temporary_file.name}')
            subset['year'] = year
            return subset

    async def gather_results(self):
        """Gather calls to the Census API so they can be run at once."""
        fetch_calls = []
        chunks = self.chunk_variables(self.variables)
        chunks_by_year = product(chunks, self.years)
        async with ClientSession(
            timeout=ClientTimeout(self.timeout),
            connector=TCPConnector(limit=self.max_connections)
        ) as session:
            for chunk, year in chunks_by_year:
                fetch_calls.append(self.fetch_acs_data(session, year, chunk))
            for year in self.years:
                fetch_calls.append(self.fetch_acs_variable_labels(session, year))
            for year in self.years_with_geography:
                fetch_calls.append(self.fetch_geospatial_data(session, year))
            tasks = map(asyncio.create_task, fetch_calls)
            return await asyncio.gather(*tasks)

    def join_labels(self, dataframe, labels_dataframe):
        """Join data to variable labels/concepts and clean them up."""
        dataframe = dataframe.merge(right=labels_dataframe, how='left', on=['variable', 'year'])
        dataframe = dataframe.loc[dataframe['label'].notnull()]  # Drop rows without labels

        # Clean up
        dataframe['label'] = dataframe['label'] \
            .str.replace('^Estimate!!', '') \
            .str.replace('!!', ' - ')
        dataframe['concept'] = dataframe['concept'].fillna(pd.np.NaN).map(titleize_text)

        return dataframe

    def join_annotations(self, dataframe):
        """Join data to ACS annotations for specific flagged values."""
        dataframe['value'] = dataframe['value'].astype(float)
        annotations_csv = resource_stream(__name__, 'resources/annotations.csv')
        annotations = pd.read_csv(annotations_csv, dtype={'value': float})
        dataframe = dataframe.merge(annotations, how='left', left_on='value', right_on='value')
        dataframe.loc[dataframe['annotation'].notnull(), 'value'] = pd.np.NaN
        return dataframe

    def join_geospatial(self, dataframe, geo_dataframe):
        """Given ACS data, join rows to geospatial points/boundaries."""
        # Get centroids
        geo_dataframe['centroid'] = geo_dataframe.centroid
        # Get internal points (guaranteed to be internal to shape)
        geo_dataframe['internal_point'] = geo_dataframe['geometry'] \
            .representative_point()
        # Coerce geometry to a series of MultiPolygons
        geo_dataframe['geometry'] = geo_dataframe['geometry'] \
            .map(coerce_polygon_to_multipolygon) \
            .map(flatten_geometry)

        # Merge dataframes and return
        affgeoid_field = ({'AFFGEOID', 'AFFGEOID10'} & set(geo_dataframe.columns)).pop()
        merged = dataframe.merge(
            geo_dataframe[[affgeoid_field, 'year', 'centroid', 'internal_point', 'geometry']],
            how='left',
            left_on=['geo_id', 'year'],
            right_on=[affgeoid_field, 'year']
        ).drop(columns=affgeoid_field)
        return merged

    def assemble_dataframe(self, results):
        """Given results from the Census API, assemble a dataframe."""
        # TODO: Refactor into multiple smaller functions
        # Split results into data, variable labels
        geospatial_index = len(results) - len(self.years_with_geography)
        labels_index = geospatial_index - len(self.years)
        data = results[:labels_index]
        labels = results[labels_index:geospatial_index]
        labels_dataframe = pd.concat(labels).rename(columns={'name': 'variable'})

        # Get list of geography types
        for_geo_type = self.for_geo.split(':')[0]
        in_geo_types = (value.split(':')[0] for value in self.in_geo)
        geography_types = [for_geo_type, *in_geo_types]

        # Melt and concatenate dataframes
        dataframes = []
        for columns, *records in data:
            id_vars = ['NAME', 'GEO_ID', *geography_types, 'year']
            subset = pd.DataFrame \
                .from_records(records, columns=columns)
            subset = subset.loc[:, ~subset.columns.duplicated()] \
                .melt(id_vars=id_vars) \
                .drop(columns=geography_types)
            dataframes.append(subset)
        # Must sort here for proper calculation of percent change and difference below
        dataframe = pd.concat(dataframes) \
            .sort_values(by=['variable', 'NAME', 'year']) \
            .reset_index(drop=True)

        # Join label and annotation data
        dataframe = self.join_labels(dataframe, labels_dataframe)
        dataframe = self.join_annotations(dataframe)

        # Compute percent change and difference
        dataframe['percent_change'] = dataframe \
            .groupby(['GEO_ID', 'variable'])['value'] \
            .pct_change()
        dataframe['difference'] = dataframe \
            .groupby(['GEO_ID', 'variable'])['value'] \
            .diff()

        # Create year date column
        datetime_strings = dataframe['year'].map('{}-12-31'.format)
        dataframe['date'] = pd.to_datetime(datetime_strings, format='%Y-%m-%d')

        # Finalize column names and order
        columns = {
            'NAME': 'name',
            'GEO_ID': 'geo_id',
            'year': 'year',
            'date': 'date',
            'variable': 'variable_code',
            'label': 'variable_label',
            'concept': 'variable_concept',
            'annotation': 'annotation',
            'value': 'value',
            'percent_change': 'percent_change',
            'difference': 'difference'
        }
        dataframe = dataframe.rename(columns=columns)
        dataframe = dataframe[columns.values()]

        # Join geospatial data
        if self.join_geography is True:
            geospatial_data = results[-len(self.years_with_geography):]
            geo_dataframe = pd.concat(geospatial_data, ignore_index=True)
            dataframe = self.join_geospatial(dataframe, geo_dataframe)

        return dataframe

    def run(self):
        """Collect ACS data for the given parameters in a dataframe."""
        results = asyncio.run(self.gather_results())
        dataframe = self.assemble_dataframe(results)
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

    def prepare_output_schema(self, output):
        """Add column metadata for improved data display on Socrata."""
        columns = pd.read_csv(resource_stream(__name__, 'resources/columns.csv'))

        # Filter out fields that aren't part of our output schema (e.g., geospatial fields)
        field_names = [column['field_name'] for column in output.attributes['output_columns']]
        columns = columns[columns['field_name'].isin(field_names)]

        # Reduce output schema with all metadata changes and return
        output = reduce(change_column_metadata, columns.to_dict(orient='records'), output)
        return output.run()

    def create_new_dataset(self, client, dataframe, name):
        """Create and publish a dataframe as a new Socrata dataset."""
        revision, output = client.create(
            name=name if name is not None else self.query_name,
            attributionLink='https://api.census.gov'
        ).df(dataframe)
        ok, output = self.prepare_output_schema(output)
        logger.debug('Publishing dataset on Socrata')
        ok, job = revision.apply(output_schema=output)
        if ok is True:
            logger.debug(f'Dataset published to {revision.ui_url()}')
        else:
            logger.error(f'Failed to publish dataset')
        return revision

    def update_existing_dataset(self, client, dataframe, dataset_id):
        """Use a dataframe to update an existing Socrata dataset."""
        ok, view = client.views.lookup(dataset_id)
        ok, revision = view.revisions.create_replace_revision()
        ok, upload = revision.create_upload('autocensus-update')
        ok, source = upload.df(dataframe)
        output = source.get_latest_input_schema().get_latest_output_schema()
        ok, job = revision.apply(output_schema=output)
        if ok is True:
            logger.debug(f'Dataset published to {revision.ui_url()}')
        else:
            logger.error(f'Failed to publish dataset')
        return revision

    def to_socrata(self, domain, *, dataset_id=None, name=None, auth=None, open_in_browser=True):
        """Run query and publish the resulting dataframe to Socrata."""
        # TODO: Refactor into multiple smaller functions
        try:
            Socrata
        except NameError:
            message = 'socrata-py must be installed in order to publish to Socrata'
            raise MissingDependencyError(message)

        dataframe = self.run()

        # Serialize polygons to WKT (avoids issue with three-dimensional geometry)
        try:
            dataframe['geometry'] = dataframe['geometry'].map(serialize_to_wkt)
        except KeyError:
            pass

        # Initialize client
        if auth is None:
            auth = self.collect_socrata_credentials_from_environment()
        client = Socrata(Authorization(domain, *auth))

        # If no 4x4 was supplied, create a new dataset
        logger.debug('Creating draft on Socrata')
        if dataset_id is None:
            revision = self.create_new_dataset(client, dataframe, name)
        # Otherwise, update an existing dataset
        else:
            revision = self.update_existing_dataset(client, dataframe, dataset_id)

        # Return URL
        if open_in_browser is True:
            revision.open_in_browser()
        return revision.ui_url()
