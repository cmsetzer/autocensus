"""Functions for publishing autocensus data to Socrata."""

from functools import reduce
import json
import logging
from logging import Logger
import os
from typing import Dict, Iterable, Optional, Tuple, Union

import pandas as pd
from pandas import DataFrame
from pkg_resources import resource_stream
from socrata import Socrata
from socrata.authorization import Authorization
from socrata.job import Job
from socrata.output_schema import OutputSchema
from socrata.revisions import Revision
from typing_extensions import Literal
from yarl import URL

from .errors import MissingCredentialsError
from .geography import serialize_to_wkt

# Initialize logger
logger: Logger = logging.getLogger(__name__)


def look_up_socrata_credentials() -> Tuple[str, str]:
    """Collect Socrata auth credentials from the local environment.

    Looks up credentials under several common Socrata environment
    variable names, and returns the first complete pair it finds. Raises
    a MissingCredentialsError if no complete pair is found.
    """
    environment_variable_pairs = [
        ('SOCRATA_KEY_ID', 'SOCRATA_KEY_SECRET'),
        ('SOCRATA_USERNAME', 'SOCRATA_PASSWORD'),
        ('MY_SOCRATA_USERNAME', 'MY_SOCRATA_PASSWORD'),
        ('SODA_USERNAME', 'SODA_PASSWORD'),
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


def change_column(prev: OutputSchema, record: Dict[str, str]) -> OutputSchema:
    """Add a column change to a Socrata revision object.

    To be used in reducing a series of such changes.
    """
    value = json.loads(record['value']) if record['field'] == 'format' else record['value']
    if record['field'] == 'transform':
        return prev.change_column_transform(record['field_name']).to(value)
    else:
        return prev.change_column_metadata(record['field_name'], record['field']).to(value)


def prepare_output_schema(output_schema: OutputSchema):
    """Add column metadata and transforms to Socrata output schema."""
    columns = pd.read_csv(resource_stream(__name__, 'resources/columns.csv'))

    # Filter out fields that aren't part of our output schema (e.g., geospatial fields)
    field_names = [column['field_name'] for column in output_schema.attributes['output_columns']]
    columns = columns[columns['field_name'].isin(field_names)]

    # Reduce output schema with all metadata changes and return
    output_schema = reduce(change_column, columns.to_dict(orient='records'), output_schema)
    return output_schema.run()


def add_geometry_to_output_schema(
    output_schema: OutputSchema, geometry: Optional[Literal['points', 'polygons']]
) -> OutputSchema:
    """Add a transform to Socrata output schema based on geometry type.

    Specifies the geometry type and reprojects the data from NAD 83 to
    WGS 84.
    """
    transform: str
    if geometry == 'points':
        base_transform = 'to_point(`geometry`)'
    elif geometry == 'polygons':
        base_transform = 'to_multipolygon(`geometry`)'
    else:
        return output_schema

    # Reproject geometry from NAD 83 to WGS 84
    transform = f"reproject_to_wgs84(set_projection({base_transform}, '+init=epsg:4269'))"
    output_schema.change_column_transform('geometry').to(transform)
    return output_schema.run()


def create_new_dataset(
    client: Socrata,
    dataframe: DataFrame,
    name: str,
    description: str,
    *,
    wait_for_finish: bool = False,
):
    """Create and publish a dataframe as a new Socrata dataset."""
    revision: Revision
    output_schema: OutputSchema
    revision, output_schema = client.create(
        name=name, description=description, attributionLink='https://api.census.gov'
    ).df(dataframe)
    output_schema = prepare_output_schema(output_schema)

    # Handle geometry column type
    if 'geometry' in dataframe.columns:
        geometry: Optional[Literal['points', 'polygons']]
        if len(dataframe.loc[dataframe['geometry'].fillna('').str.match('^POINT')]):
            geometry = 'points'
        elif len(dataframe.loc[dataframe['geometry'].fillna('').str.match('^MULTIPOLYGON')]):
            geometry = 'polygons'
        else:
            geometry = None
        output_schema = add_geometry_to_output_schema(output_schema, geometry)

    # Handle pre-1.x versions of Socrata-py
    if isinstance(output_schema, tuple):
        _, output_schema = output_schema

    output_schema.wait_for_finish()
    job: Job = revision.apply(output_schema=output_schema)
    if wait_for_finish is True:
        job.wait_for_finish()
    return revision


def update_existing_dataset(
    client: Socrata, dataframe: DataFrame, dataset_id: str, *, wait_for_finish: bool = False
):
    """Use a dataframe to update an existing Socrata dataset."""
    view = client.views.lookup(dataset_id)
    # Handle pre-1.x versions of Socrata-py
    if isinstance(view, tuple):
        ok, view = view
        ok, revision = view.revisions.create_replace_revision()
        ok, upload = revision.create_upload('autocensus-update')
        ok, source = upload.df(dataframe)
        source.wait_for_finish()
        output = source.get_latest_input_schema().get_latest_output_schema()
        output.wait_for_finish()
        job = revision.apply(output_schema=output)
    else:
        revision = view.revisions.create_replace_revision()
        upload = revision.create_upload('autocensus-update')
        source = upload.df(dataframe)
        source.wait_for_finish()
        output = source.get_latest_input_schema().get_latest_output_schema()
        output.wait_for_finish()
        job = revision.apply(output_schema=output)
    if wait_for_finish is True:
        job.wait_for_finish()
    return revision


def build_dataset_name(estimate: int, years: Iterable) -> str:
    """Produce a nicely formatted dataset name."""
    unique_years = sorted(set(years))
    if len(unique_years) > 1:
        years_range = f'{min(unique_years)}–{max(unique_years)}'
    else:
        years_range = unique_years[0]
    query_name = f'American Community Survey {estimate}-Year Estimates, {years_range}'
    return query_name


def to_socrata(
    domain: Union[URL, str],
    dataframe: DataFrame,
    dataset_id: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    auth: Optional[Tuple[str, str]] = None,
    open_in_browser: bool = True,
    wait_for_finish: bool = False,
) -> URL:
    """Publish an autocensus dataframe to Socrata."""
    # Serialize geometry to WKT
    try:
        dataframe['geometry'] = dataframe['geometry'].map(serialize_to_wkt)
    except KeyError:
        pass

    # Initialize client
    if auth is None:
        auth = look_up_socrata_credentials()
    client = Socrata(Authorization(str(domain), *auth))

    # If no 4x4 was supplied, create a new dataset
    if dataset_id is None:
        name = name if name is not None else 'American Community Survey Data'
        description = description if description is not None else ''
        revision = create_new_dataset(
            client, dataframe, name, description, wait_for_finish=wait_for_finish
        )
    else:
        revision = update_existing_dataset(
            client, dataframe, dataset_id, wait_for_finish=wait_for_finish
        )

    # Return URL
    if open_in_browser is True:
        revision.open_in_browser()
    return URL(revision.ui_url())
