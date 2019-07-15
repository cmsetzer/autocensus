"""Utility functions for processing Census API data."""

from functools import wraps
import json
import math

from shapely.geometry import MultiPolygon, Polygon
from titlecase import titlecase


def forgive(*exceptions):
    """Decorator for gracefully ignoring specified exception types.

    This is especially useful for skipping NA values in columns.
    """
    def decorator(func):
        @wraps(func)
        def wrapped(value):
            try:
                return func(value)
            except tuple(exceptions):
                return value
        return wrapped
    return decorator


def calculate_congress_for_year(year):
    """Given a year, calculate the number of the U.S. Congress."""
    congress = math.ceil((year - 1789) / 2) + 1
    return congress


def determine_geo_code(year, for_geo_type, state_fips):
    """Determine the shapefile naming code for a given geography."""
    if for_geo_type == 'congressional district':
        congress = calculate_congress_for_year(year)
    else:
        congress = None
    geo_code_mappings = {
        # National geographies
        'nation': 'us_nation',
        'region': 'us_region',
        'division': 'us_division',
        'state': 'us_state',
        'urban area': 'us_ua10',
        'zip code tabulation area': 'us_zcta510',
        'county': 'us_county',
        'congressional district': f'us_cd{congress}',
        'metropolitan statistical area/micropolitan statistical area': 'us_cbsa',
        'combined statistical area': 'us_csa',
        'american indian area/alaska native area/hawaiian home land': 'us_aiannh',
        'new england city and town area': 'us_necta',
        # State-level geographies
        'alaska native regional corporation': '02_anrc',
        'block group': f'{state_fips}_bg',
        'county subdivision': f'{state_fips}_cousub',
        'tract': f'{state_fips}_tract',
        'place': f'{state_fips}_place',
        'public use microdata area': f'{state_fips}_puma10',
        'state legislative district (upper chamber)': f'{state_fips}_sldu',
        'state legislative district (lower chamber)': f'{state_fips}_sldl',
        # Note: consolidated city doesn't work (Census API won't permit inclusion of state, but
        # state is required to download a shapefile)
        # 'consolidated city': f'{state_fips}_concity'
    }
    return geo_code_mappings[for_geo_type]


def change_column_metadata(prev, record):
    """Add a column metadata change to a Socrata revision object.

    To be used in reducing a series of such changes.
    """
    value = json.loads(record['value']) if record['field'] == 'format' else record['value']
    return prev.change_column_metadata(record['field_name'], record['field']).to(value)


def coerce_polygon_to_multipolygon(shape):
    """Convert a polygon into a MultiPolygon if it's not one already."""
    if not isinstance(shape, MultiPolygon):
        return MultiPolygon([shape])
    else:
        return shape


@forgive(AttributeError)
def flatten_geometry(multipolygon):
    """Flatten a three-dimensional multipolygon to two dimensions."""
    if not multipolygon.has_z:
        return multipolygon
    polygons = []
    for polygon in multipolygon:
        new_coordinates = [(x, y) for (x, y, *_) in polygon.exterior.coords]
        polygons.append(Polygon(new_coordinates))
    return MultiPolygon(polygons)


@forgive(AttributeError)
def serialize_to_wkt(value):
    """Serialize a geometry value to well-known text (WKT)."""
    return value.to_wkt()


@forgive(TypeError)
def titleize_text(value):
    """Convert a text string to title case."""
    return titlecase(value)
