"""Constants and types for use throughout other modules."""

from pathlib import Path
from typing import Optional, Union

from pandas import DataFrame
from platformdirs import user_cache_dir
from typing_extensions import Literal

# Cache directory path
CACHE_DIRECTORY_PATH = Path(user_cache_dir('autocensus'))

# Query parameters
QueryEstimate = Literal[1, 3, 5]
QueryGeometry = Literal['points', 'polygons']
QueryResolution = Literal['500k', '5m', '20m']

# Types representing data tables, variables, and geometry files returned from from Census API
Table = list[list[Union[int, str]]]
Variables = dict[tuple[int, str], dict]
GazetteerFile = list[Optional[DataFrame]]
Shapefile = Optional[Path]
