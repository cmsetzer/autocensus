"""Functions for publishing autocensus data to Socrata."""

from functools import reduce
import json
import os
from typing import Dict, Iterable, Tuple, Union

import pandas as pd
from pandas import DataFrame
from pkg_resources import resource_stream
from socrata import Socrata
from socrata.authorization import Authorization
from socrata.output_schema import OutputSchema
from yarl import URL

from .errors import MissingCredentialsError
from .geography import serialize_to_wkt

# Types
Credentials = Tuple[str, str]


def look_up_socrata_credentials(credentials: Credentials = None) -> Credentials:
    """Collect Socrata auth credentials from the local environment.

    Looks up credentials under several common Socrata environment
    variable names, and returns the first complete pair it finds. Raises
    a MissingCredentialsError if no complete pair is found.
    """
    if credentials is not None:
        return credentials
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
    """Add column metadata for improved data display on Socrata."""
    columns = pd.read_csv(resource_stream(__name__, 'resources/columns.csv'))

    # Filter out fields that aren't part of our output schema (e.g., geospatial fields)
    field_names = [column['field_name'] for column in output_schema.attributes['output_columns']]
    columns = columns[columns['field_name'].isin(field_names)]

    # Reduce output schema with all metadata changes and return
    output_schema = reduce(change_column, columns.to_dict(orient='records'), output_schema)
    return output_schema.run()


def create_new_dataset(client: Socrata, dataframe: DataFrame, name: str, description: str):
    """Create and publish a dataframe as a new Socrata dataset."""
    revision, output = client.create(
        name=name, description=description, attributionLink='https://api.census.gov'
    ).df(dataframe)
    output = prepare_output_schema(output)
    # Handle pre-1.x versions of Socrata-py
    if isinstance(output, tuple):
        ok, output = output
    output.wait_for_finish()
    revision.apply(output_schema=output)
    return revision


def update_existing_dataset(client: Socrata, dataframe, dataset_id):
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
        revision.apply(output_schema=output)
    else:
        revision = view.revisions.create_replace_revision()
        upload = revision.create_upload('autocensus-update')
        source = upload.df(dataframe)
        source.wait_for_finish()
        output = source.get_latest_input_schema().get_latest_output_schema()
        output.wait_for_finish()
        revision.apply(output_schema=output)
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
    dataset_id: str = None,
    name: str = None,
    description: str = None,
    auth: Credentials = None,
    open_in_browser: bool = True,
) -> URL:
    """Publish an autocensus dataframe to Socrata."""
    # Serialize geometry to WKT
    try:
        dataframe['geometry'] = dataframe['geometry'].map(serialize_to_wkt)
    except KeyError:
        pass

    # Initialize client
    client = Socrata(Authorization(str(domain), *look_up_socrata_credentials(auth)))

    # If no 4x4 was supplied, create a new dataset
    if dataset_id is None:
        name = name if name is not None else 'American Community Survey Data'
        description = description if description is not None else ''
        revision = create_new_dataset(client, dataframe, name, description)
    else:
        revision = update_existing_dataset(client, dataframe, dataset_id)

    # Return URL
    if open_in_browser is True:
        revision.open_in_browser()
    return URL(revision.ui_url())
