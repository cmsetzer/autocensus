"""Package for collecting ACS and geospatial data from the Census API."""

from importlib.metadata import version
import logging
from logging import StreamHandler

from autocensus.query import Query  # noqa
from autocensus.utilities import clear_cache  # noqa

# Initialize version string from package metadata
__version__ = version(__name__)

# Initialize logger
logger = logging.getLogger(__name__)
logger.addHandler(StreamHandler())
logger.setLevel(logging.INFO)
