"""Utility functions for processing Census API data."""

from functools import wraps
import json

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
