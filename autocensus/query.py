"""A class and methods for performing Census API queries."""

import asyncio
from collections import defaultdict
from contextlib import contextmanager
import csv
from functools import partial
import importlib
from itertools import product
import logging
from operator import is_not
import os
from typing import DefaultDict, Iterable, Optional, Union, get_args
from zipfile import BadZipFile

import geopandas as gpd
from geopandas.geodataframe import GeoDataFrame
import pandas as pd
from pandas import DataFrame

from autocensus.api import CensusAPI, look_up_census_api_key
from autocensus.constants import (
    CACHE_DIRECTORY_PATH,
    GazetteerFile,
    QueryEstimate,
    QueryGeometry,
    QueryResolution,
    Shapefile,
    Table,
    Variables,
)
from autocensus.geography import (
    Geo,
    coerce_polygon_to_multipolygon,
    flatten_geometry,
    get_geo_mappings,
    identify_affgeoid_field,
    load_geodataframe,
    normalize_geo_id,
)
from autocensus.utilities import (
    check_geo_estimates,
    check_geo_hierarchy,
    check_years,
    chunk_variables,
    load_annotations_dataframe,
    parse_table_name_from_variable,
    wrap_scalar_value_in_list,
)

# Initialize logger
logger = logging.getLogger(__name__)


class Query:
    """A query for American Community Survey data from the Census API.

    A Query instance can be used to fetch ACS variables, tables, and
    geometry for a given ACS estimate (1-, 3-, or 5-year), year(s), and
    geographic units.
    """

    def __init__(
        self,
        estimate: QueryEstimate,
        years: Union[Iterable, int],
        variables: Union[Iterable, str],
        for_geo: Union[Iterable, str],
        in_geo: Optional[Union[Iterable, str]] = None,
        geometry: Optional[QueryGeometry] = None,
        resolution: Optional[QueryResolution] = None,
        census_api_key: Optional[str] = None,
    ):
        if estimate in get_args(QueryEstimate):
            self.estimate: int = estimate
        else:
            raise ValueError(f'Please specify a valid estimate: {get_args(QueryEstimate)}')
        self._years = wrap_scalar_value_in_list(years)
        self._variables = wrap_scalar_value_in_list(variables)
        self.for_geo = [Geo(geo) for geo in wrap_scalar_value_in_list(for_geo)]
        self.in_geo = (
            [] if in_geo is None else [Geo(geo) for geo in wrap_scalar_value_in_list(in_geo)]
        )

        # Validate geometry and resolution
        if (geometry is None) or (geometry in get_args(QueryGeometry)):
            self.geometry = geometry
        else:
            raise ValueError(f'Please specify a valid geometry: {get_args(QueryGeometry)}')
        if resolution is None or resolution in get_args(QueryResolution):
            if resolution is not None and geometry != 'polygons':
                logger.warning('Warning: Specifying a resolution is only supported for polygons')
            self.resolution = resolution
        else:
            raise ValueError(f'Please specify a valid resolution: {get_args(QueryResolution)}')

        # Use Census API key if supplied, or fall back to environment variable if not
        self.census_api_key = look_up_census_api_key(census_api_key)

        # Validate query parameters to avoid common pitfalls
        self._validate_query_parameters()

        # Initialize invalid variables defaultdict
        self._invalid_variables: DefaultDict[int, list] = defaultdict(list)

        # Create cache directory, plus any enclosing directories, as needed
        CACHE_DIRECTORY_PATH.mkdir(parents=True, exist_ok=True)

    def __repr__(self):  # pragma: no cover
        attributes = ['estimate', 'years', 'variables', 'for_geo', 'in_geo']
        attribute_representations = [repr(getattr(self, attribute)) for attribute in attributes]
        representation = '<Query estimate={} years={} variables={} for_geo={} in_geo={}>'.format(
            *attribute_representations
        )
        return representation

    @property
    def years(self):
        """Return the supplied years as a sorted, unique list."""
        return sorted(set(self._years))

    @property
    def variables(self):
        """Return the supplied variables as a sorted, unique list."""
        return sorted(set(self._variables))

    def get_variables_by_year_and_table_name(self):
        """Group the supplied variables by year and table name."""
        variables: DefaultDict[tuple[int, str], list[str]] = defaultdict(list)
        for year, variable in product(self.years, self.variables):
            if variable in self._invalid_variables[year]:
                continue
            table_name = parse_table_name_from_variable(variable)
            variables[year, table_name].append(variable)
        return variables

    def _validate_query_parameters(self):
        """Validate query parameters to avoid common pitfalls."""
        check_years(self._years)
        check_geo_hierarchy(self.for_geo, self.in_geo)
        check_geo_estimates(self.estimate, self.for_geo)
        return True

    @contextmanager
    def create_census_api_session(self):
        """Initialize a Census API session.

        Adds an instance of the CensusAPI class to this Query as an
        internal attribute, then removes it upon completion.
        """
        self._census_api = CensusAPI(self.census_api_key)
        yield
        del self._census_api

    def get_variables(self) -> Variables:
        """Get labels and concepts for the supplied variables.

        Sets aside any variables that are invalid for a given year.
        """
        # Assemble API calls for concurrent execution
        calls = []
        for (year, table_name), group in self.get_variables_by_year_and_table_name().items():
            for variable in group:
                call = self._census_api.fetch_variable(self.estimate, year, table_name, variable)
                calls.append(call)

        # Make concurrent API calls
        gathered_calls = self._census_api.gather_calls(calls)
        try:
            results = asyncio.run(gathered_calls)
        except RuntimeError as error:
            # Handle Jupyter issue with multiple running event loops by importing nest_asyncio
            if error.args[0] == 'asyncio.run() cannot be called from a running event loop':
                import nest_asyncio

                nest_asyncio.apply()
                results = asyncio.run(gathered_calls)  # type: ignore
            else:
                raise error

        # Compile invalid variables
        variables = {}
        for variable_json in results:
            year = variable_json['year']
            if not variable_json.get('label', False):
                invalid_variable = variable_json['name']
                self._invalid_variables[year].append(invalid_variable)
                message = f'Warning: {invalid_variable} is not a recognized variable for {year}'
                logger.warning(message)
            else:
                variables[year, variable_json['name']] = variable_json
        return variables

    def get_tables(self) -> list[Table]:
        """Get data tables for the supplied variables."""
        # Assemble API calls for concurrent execution
        calls = []
        for (year, table_name), variables in self.get_variables_by_year_and_table_name().items():
            # Handle multiple for_geo values by year
            chunked_variables_by_for_geo = product(self.for_geo, chunk_variables(variables))
            for for_geo, chunk in chunked_variables_by_for_geo:
                call = self._census_api.fetch_table(
                    self.estimate, year, table_name, chunk, for_geo, self.in_geo
                )
                calls.append(call)
        # Make concurrent API calls
        results = asyncio.run(self._census_api.gather_calls(calls))
        tables = list(results)
        return tables

    def get_gazetteer_files(self) -> list[GazetteerFile]:
        """Get Gazetteer files for the supplied years and geographies."""
        # Assemble API calls for concurrent execution
        calls = []
        years_with_gazetteer_files = [year for year in self.years if year >= 2012]
        # Handle multiple for_geo values by year
        for year, for_geo in product(years_with_gazetteer_files, self.for_geo):
            call = self._census_api.fetch_gazetteer_file(year, for_geo)
            calls.append(call)
        # Make concurrent API calls
        results = asyncio.run(self._census_api.gather_calls(calls))
        gazetteer_files = list(results)
        return gazetteer_files

    def get_shapefiles(self) -> list[Shapefile]:
        """Get shapefiles for the supplied years and geographies."""
        # Assemble API calls for concurrent execution
        calls = []
        years_with_shapefiles = [year for year in self.years if year >= 2013]

        # Handle multiple for_geo values by year
        for year, for_geo in product(years_with_shapefiles, self.for_geo):
            call = self._census_api.fetch_shapefile(year, for_geo, self.in_geo, self.resolution)
            calls.append(call)

        # Make concurrent API calls
        results = asyncio.run(self._census_api.gather_calls(calls))
        shapefiles = list(results)
        return shapefiles

    def convert_variables_to_dataframe(self, variables: Variables):
        """Convert Census API variable data to a dataframe."""
        records = []
        for (year, variable_name), variable in variables.items():
            variable['year'] = year
            records.append(variable)
        dataframe = DataFrame.from_records(records)

        # Drop/rename columns, clean up label/concept values
        extra_columns = {'attributes', 'group', 'limit', 'predicateType'} & set(dataframe.columns)
        dataframe = dataframe.drop(columns=extra_columns).rename(columns={'name': 'variable'})

        return dataframe

    def convert_tables_to_dataframe(self, tables: list[Table]):
        """Reshape and convert ACS data tables to a dataframe."""
        geography_types = get_geo_mappings('geo_codes').keys()

        # Melt each subset to adopt common schema
        subsets = []
        for header, *rows in tables:
            subset = DataFrame(rows, columns=header)
            # Consolidate geography type in a single column
            geography_columns = set(geography_types) & set(subset.columns)
            id_vars = ['NAME', 'GEO_ID', 'geo_type', *geography_columns, 'year']
            melted = subset.melt(id_vars=id_vars).drop(columns=geography_columns)
            subsets.append(melted)

        # Ensure correct sort order
        dataframe = (
            pd.concat(subsets)
            .sort_values(by=['geo_type', 'variable', 'NAME', 'year'])
            .reset_index(drop=True)
        )
        return dataframe

    def convert_gazetteer_files_to_dataframe(self, gazetteer_files: list[GazetteerFile]):
        """Convert one or more Gazetteer files to a dataframe.

        Skips over null values produced by invalid responses from the
        Gazetteer file endpoint.
        """
        subsets = []
        gazetteer_tables = filter(partial(is_not, None), gazetteer_files)
        nad_83_epsg = 4269
        for gazetteer_table in gazetteer_tables:
            subset = GeoDataFrame(
                gazetteer_table,
                geometry=gpd.points_from_xy(
                    gazetteer_table['INTPTLONG'], gazetteer_table['INTPTLAT']
                ),
            )
            subset.crs = f'EPSG:{nad_83_epsg}'
            subset['gazetteer_geo_id'] = subset.apply(
                lambda row: normalize_geo_id(row['GEOID'], row['gazetteer_geo_type']), axis=1
            )
            subsets.append(subset)

        # Return null value if no dataframes were obtained
        if not subsets:
            return None

        # Concatenate dataframes and return
        dataframe = pd.concat(subsets)
        return dataframe

    def convert_shapefiles_to_dataframe(self, shapefiles: list[Shapefile]):
        """Convert one or more shapefiles to a dataframe.

        Skips over null filepaths produced by invalid responses from the
        Census shapefile endpoint. For corrupt or invalid zip files
        (typically caused by an interrupted shapefile download to the
        autocensus cache), issues a warning and skips to the next
        filepath.
        """
        # Avoid needless encoding warnings
        os.environ['CPL_ZIP_ENCODING'] = 'UTF-8'
        subsets = []
        # Drop null values (e.g., for not-yet-released shapefiles) from list of filepaths
        filepaths = filter(None, shapefiles)
        for filepath in filepaths:
            try:
                subset = load_geodataframe(filepath)
            except BadZipFile:
                logger.warning(
                    f'Warning: Failed to load zip file {filepath}. It may be corrupted. You might '
                    'try clearing your autocensus cache by calling autocensus.clear_cache() or '
                    f'manually deleting the cache folder at {CACHE_DIRECTORY_PATH}. Continuing…'
                )
                continue
            subsets.append(subset)
        dataframe = pd.concat(subsets, ignore_index=True, sort=True)

        # Geometry columns
        if self.geometry == 'polygons':
            dataframe['geometry'] = (
                dataframe['geometry'].map(coerce_polygon_to_multipolygon).map(flatten_geometry)
            )

        # Clean up
        affgeoid_field = identify_affgeoid_field(dataframe.columns)
        columns_to_keep = [affgeoid_field, 'year', 'geometry']
        dataframe = dataframe.loc[:, columns_to_keep]
        return dataframe

    def finalize_dataframe(self, dataframe: DataFrame):
        """Clean up and finalize a dataframe.

        Drops duplicates, adds columns, normalizes column names, and
        reorders columns.
        """
        # Drop duplicates (some geospatial datasets, like ZCTAs, include redundant rows)
        geo_names = {'geometry'}
        non_geo_names = set(dataframe.columns) - geo_names
        dataframe = dataframe.drop_duplicates(subset=non_geo_names, ignore_index=True)

        # Insert NAs for annotated row values to avoid outlier values like -999,999,999
        dataframe.loc[dataframe['annotation'].notna(), 'value'] = ''
        dataframe['value'] = pd.to_numeric(dataframe['value'], errors='coerce')

        # Create year date column
        dataframe['date'] = pd.to_datetime(
            dataframe['year'].astype('string') + '-12-31', format='%Y-%m-%d'
        )

        # Rename and reorder columns
        names_csv = importlib.resources.files(__name__).joinpath('resources/names.csv')
        with names_csv.open() as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header row
            names = dict(csv_reader)  # type: ignore
        if self.geometry in ['points', 'polygons'] and (set(dataframe.columns) & geo_names):
            name_order = [*names.values(), *geo_names]
        else:
            name_order = list(names.values())
        dataframe = dataframe.rename(columns=names)[name_order]

        return dataframe

    def assemble_dataframe(
        self,
        variables: Variables,
        tables: list[Table],
        gazetteer_files: list[GazetteerFile],
        shapefiles: list[Shapefile],
    ):
        """Merge and finalize the query dataframe.

        Joins tables, variables, annotations, and geospatial data, then
        cleans up and finalizes the combined dataframe.
        """
        # Merge tables with variables, annotations
        logger.info('Merging ACS tables and variables...')
        tables_dataframe = self.convert_tables_to_dataframe(tables)
        variables_dataframe = self.convert_variables_to_dataframe(variables)
        dataframe = tables_dataframe.merge(
            right=variables_dataframe, how='left', on=['variable', 'year']
        )
        logger.info('Merging annotations...')
        annotations_dataframe = load_annotations_dataframe()
        dataframe = dataframe.merge(right=annotations_dataframe, how='left', on=['value'])

        # Merge geospatial data if included
        geometry_dataframe: Union[DataFrame, None]
        right_geo_id_field: str
        if self.geometry in ['points', 'polygons']:
            if self.geometry == 'points':
                logger.info('Merging Gazetteer files...')
                geometry_dataframe = self.convert_gazetteer_files_to_dataframe(gazetteer_files)
                right_geo_id_field = 'gazetteer_geo_id'
            else:
                logger.info('Merging shapefiles...')
                geometry_dataframe = self.convert_shapefiles_to_dataframe(shapefiles)
                right_geo_id_field = identify_affgeoid_field(geometry_dataframe.columns)
            if geometry_dataframe is not None:
                dataframe = dataframe.merge(
                    right=geometry_dataframe,
                    how='left',
                    left_on=['GEO_ID', 'year'],
                    right_on=[right_geo_id_field, 'year'],
                )

        # Finalize dataframe
        logger.info('Finalizing data...')
        dataframe = self.finalize_dataframe(dataframe)

        return dataframe

    def run(self) -> DataFrame:
        """Run the supplied query and return the output as a dataframe.

        Depending on the complexity of the query, may take anywhere from
        a few seconds to several minutes.
        """
        with self.create_census_api_session():
            logger.info('Retrieving variables...')
            variables: Variables = self.get_variables()
            logger.info('Retrieving ACS tables...')
            tables = self.get_tables()

            # Add geometry
            gazetteer_files: list[GazetteerFile] = []
            shapefiles: list[Shapefile] = []
            if self.geometry == 'points':
                logger.info('Retrieving Gazetteer files...')
                gazetteer_files.extend(self.get_gazetteer_files())
            elif self.geometry == 'polygons':
                logger.info('Retrieving shapefiles...')
                shapefiles.extend(self.get_shapefiles())
        dataframe = self.assemble_dataframe(variables, tables, gazetteer_files, shapefiles)
        return dataframe
