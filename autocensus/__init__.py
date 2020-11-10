"""Package for collecting ACS and geospatial data from the Census API."""

__version__ = '1.1.0'

import logging
from logging import Logger, StreamHandler

from . import api, errors, geography, query, socrata, utilities  # noqa
from .query import Query  # noqa
from .utilities import clear_cache  # noqa

# Initialize logger
logger: Logger = logging.getLogger(__name__)
logger.addHandler(StreamHandler())
logger.setLevel(logging.INFO)
