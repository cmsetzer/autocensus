"""Package for collecting ACS and geospatial data from the Census API."""

import logging
from logging import Logger, StreamHandler

from . import api, constants, errors, geography, query, socrata, utilities  # noqa
from .query import Query  # noqa
from .utilities import clear_cache  # noqa

try:
    from importlib.metadata import version
except ImportError:  # pragma: no cover
    from importlib_metadata import version  # type: ignore

# Initialize version string from package metadata
__version__ = version(__name__)

# Initialize logger
logger: Logger = logging.getLogger(__name__)
logger.addHandler(StreamHandler())
logger.setLevel(logging.INFO)
