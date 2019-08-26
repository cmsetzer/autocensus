"""Package for collecting ACS and geospatial data from the Census API."""

__version__ = '1.0.0'

from . import api, errors, geography, query, socrata, topics, utilities  # noqa
from .query import Query  # noqa
