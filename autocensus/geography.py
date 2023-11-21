"""Utility functions for working with geospatial data."""

from csv import reader
from dataclasses import dataclass
from functools import lru_cache
from io import StringIO
import logging
import math
from pathlib import Path
from typing import Dict, Iterable, Optional, Union

import geopandas as gpd
from geopandas import GeoDataFrame
from pkg_resources import resource_string
from shapely import wkt
from shapely.geometry import MultiPolygon, Point, Polygon
import us

# Initialize logger
logger = logging.getLogger(__name__)


@dataclass
class Geo:
    """A Census geography, e.g. "state:08", to be used in a query."""

    type: str
    code: str

    def __init__(self, value: str, code: Optional[str] = None):
        # Process value, code
        if value == 'us' and code is None:
            self.type = 'us'
            self.code = '*'
        elif code is None:
            try:
                self.type, self.code = value.split(':')
            except ValueError as error:
                message = 'Please specify a valid geography value, e.g. "state:08"'
                raise ValueError(message) from error
        else:
            self.type = value
            self.code = code

        # Convert state abbreviation to FIPS code as needed
        if (self.type == 'state') and (code != '*'):
            state = us.states.lookup(self.code)
            if state is not None:
                self.code = state.fips

    def __str__(self):
        return f'{self.type}:{self.code}'


def calculate_congress_number(year: int) -> int:
    """Given a year, calculate the number of the U.S. Congress."""
    congress = math.ceil((year - 1789) / 2) + 1
    return congress


@lru_cache(maxsize=1024)
def get_geo_mappings(source: str) -> Dict[str, str]:
    """Read filename codes from a local CSV."""
    codes_csv = resource_string(__name__, f'resources/{source}.csv')
    csv_reader = reader(StringIO(codes_csv.decode('utf-8')))
    codes = {type_: code for type_, code in csv_reader}
    return codes


def determine_gazetteer_code(year: int, for_geo_type: str) -> Union[str, None]:
    """Determine the Gazetteer file naming code for a given year/geography."""
    gazetteer_codes = get_geo_mappings('gazetteer_codes')
    gazetteer_code: Union[str, None]
    if for_geo_type != 'congressional district':
        gazetteer_code = gazetteer_codes.get(for_geo_type)
    else:
        congress = calculate_congress_number(year)
        gazetteer_code = gazetteer_codes[for_geo_type].format(congress=congress)
    return gazetteer_code


def determine_geo_code(year: int, for_geo_type: str, state_fips: str) -> str:
    """Determine the shapefile naming code for a given year/geography."""
    geo_codes = get_geo_mappings('geo_codes')
    geo_code: str
    if for_geo_type == 'congressional district':
        congress = calculate_congress_number(year)
        geo_code = geo_codes[for_geo_type].format(congress=congress)
    elif for_geo_type in ['urban area', 'zip code tabulation area']:
        decade = ((year // 10) % 10) * 10
        geo_code = geo_codes[for_geo_type].format(decade=decade)
    else:
        geo_code = geo_codes[for_geo_type].format(state_fips=state_fips)
    return geo_code


def load_geodataframe(filepath: Path) -> GeoDataFrame:
    """Given a filepath for a cached shapefile, load it as a dataframe."""
    geodataframe = gpd.read_file(filepath, engine='pyogrio')

    # Add year column
    geodataframe['year'] = int(filepath.name[3:7])
    return geodataframe


def coerce_polygon_to_multipolygon(shape: Union[MultiPolygon, Polygon]) -> MultiPolygon:
    """Convert a polygon into a MultiPolygon if it's not one already."""
    if not isinstance(shape, MultiPolygon):
        return MultiPolygon([shape])
    else:
        return shape


def flatten_geometry(multipolygon: MultiPolygon) -> MultiPolygon:
    """Flatten a three-dimensional multipolygon to two dimensions."""
    try:
        has_z = multipolygon.has_z
    except AttributeError:
        return multipolygon
    else:
        if has_z is not True:
            return multipolygon
    polygons = []
    for polygon in multipolygon.geoms:
        new_coordinates = [(x, y) for (x, y, *_) in polygon.exterior.coords]
        polygons.append(Polygon(new_coordinates))
    flattened_multipolygon = MultiPolygon(polygons)
    return flattened_multipolygon


def serialize_to_wkt(value: Union[MultiPolygon, Point, Polygon]) -> str:
    """Serialize a geometry value to well-known text (WKT)."""
    try:
        return wkt.dumps(value)
    except ValueError:
        return value


def identify_affgeoid_field(fields: Iterable[str]) -> str:
    """Given a series of fields, identify an AFFGEOID field among them.

    Since Census shapefiles sometimes feature different names for the
    AFFGEOID field, it's necessary to build in some extra handling when
    merging geospatial data with table data.
    """
    known_field_names = {'AFFGEOID', *(f'AFFGEOID{decade:02}' for decade in range(0, 99, 10))}
    affgeoid_field = (known_field_names & set(fields)).pop()
    return affgeoid_field


@lru_cache(maxsize=1024)
def normalize_geo_id(geo_id: str, geo_type: str) -> Union[str, None]:
    """Convert a Census geographic identifier to a common format.

    This enables joins between entities like Gazetteer tables and ACS
    tables.
    """
    geo_id_mappings = get_geo_mappings('geo_ids')
    template = geo_id_mappings.get(geo_type)
    if template is None:
        return None
    return template.format(geo_id=geo_id)
