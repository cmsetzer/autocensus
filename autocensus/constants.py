"""Constants and types for use throughout other modules."""

from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from appdirs import user_cache_dir
from pandas import DataFrame
from typing_extensions import Literal

# Cache directory path
CACHE_DIRECTORY_PATH = Path(user_cache_dir('autocensus', 'socrata'))

# Query parameters
ESTIMATES = [1, 3, 5]
QueryEstimate = Literal[1, 3, 5]
GEOMETRIES = ['points', 'polygons']
QueryGeometry = Literal['points', 'polygons']
RESOLUTIONS = ['500k', '5m', '20m']
QueryResolution = Literal['500k', '5m', '20m']

# Types representing data tables, variables, and geometry files returned from from Census API
Table = List[List[Union[int, str]]]
Variables = Dict[Tuple[int, str], dict]
GazetteerFile = List[Optional[DataFrame]]
Shapefile = Optional[Path]
