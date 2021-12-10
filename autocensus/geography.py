"""Utility functions for working with geospatial data."""

from csv import reader
from dataclasses import dataclass
from functools import lru_cache
from io import StringIO
import logging
from logging import Logger
import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union
from zipfile import ZipFile, ZipInfo

from fiona.io import ZipMemoryFile
from geopandas import GeoDataFrame
from pkg_resources import resource_string
from shapely import wkt
from shapely.geometry import MultiPolygon, Point, Polygon
import us
from us.states import State

# Initialize logger
logger: Logger = logging.getLogger(__name__)


@dataclass
class Geo:
    """A Census geography, e.g. "state:08", to be used in a query."""

    type: str
    code: str

    def __init__(self, value: str, code: str = None):
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
        if self.type == 'state':
            state: State = us.states.lookup(self.code)
            if state is not None:
                self.code = state.fips

    def __str__(self):
        return f'{self.type}:{self.code}'


def calculate_congress_number(year: int) -> int:
    """Given a year, calculate the number of the U.S. Congress."""
    congress: int = math.ceil((year - 1789) / 2) + 1
    return congress


@lru_cache(maxsize=1024)
def get_geo_mappings(source: str) -> Dict[str, str]:
    """Read filename codes from a local CSV."""
    codes_csv: bytes = resource_string(__name__, f'resources/{source}.csv')
    csv_reader: Iterable[List[str]] = reader(StringIO(codes_csv.decode('utf-8')))
    codes = {type_: code for type_, code in csv_reader}
    return codes


def determine_gazetteer_code(year: int, for_geo_type: str) -> Optional[str]:
    """Determine the Gazetteer file naming code for a given year/geography."""
    gazetteer_codes: Dict[str, str] = get_geo_mappings('gazetteer_codes')
    gazetteer_code: Optional[str]
    if for_geo_type != 'congressional district':
        gazetteer_code = gazetteer_codes.get(for_geo_type)
    else:
        congress: int = calculate_congress_number(year)
        gazetteer_code = gazetteer_codes[for_geo_type].format(congress=congress)
    return gazetteer_code


def determine_geo_code(year: int, for_geo_type: str, state_fips: str) -> str:
    """Determine the shapefile naming code for a given year/geography."""
    geo_codes: Dict[str, str] = get_geo_mappings('geo_codes')
    geo_code: str
    if for_geo_type != 'congressional district':
        geo_code = geo_codes[for_geo_type].format(state_fips=state_fips)
    else:
        congress: int = calculate_congress_number(year)
        geo_code = geo_codes[for_geo_type].format(congress=congress)
    return geo_code


def is_shp_file(zipped_file: ZipInfo) -> bool:
    """Determine whether a zipped file's filename ends with .shp."""
    return zipped_file.filename.casefold().endswith('.shp')


def load_geodataframe(filepath: Path) -> GeoDataFrame:
    """Given a filepath for a cached shapefile, load it as a dataframe.

    This function takes a roundabout approach to reading the zipped
    shapefile due to cryptic and unpredictable errors that occur when
    opening zip files on disk with Fiona/GDAL.
    """
    # Get .shp filename from within zipped shapefile
    with ZipFile(filepath, 'r') as zip_file:
        shp_filename: str = next(filter(is_shp_file, zip_file.filelist)).filename

    # Use default Python opener to prevent cryptic GDAL filepath errors
    with open(filepath, 'rb') as bytes_file:
        with ZipMemoryFile(bytes_file.read()) as zip_memory_file:
            with zip_memory_file.open(shp_filename) as collection:
                # Load GeoDataFrame using NAD83 projection (EPSG 4269)
                geodataframe = GeoDataFrame.from_features(collection, crs='EPSG:4269')

    # Add year column
    geodataframe['year'] = int(shp_filename[3:7])

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
        has_z: bool = multipolygon.has_z
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
    known_field_names = {'AFFGEOID', 'AFFGEOID10'}
    affgeoid_field: str = (known_field_names & set(fields)).pop()
    return affgeoid_field


@lru_cache(maxsize=1024)
def normalize_geo_id(geo_id: str, geo_type: str) -> Optional[str]:
    """Convert a Census geographic identifier to a common format.

    This enables joins between entities like Gazetteer tables and ACS
    tables.
    """
    geo_id_mappings: Dict[str, str] = get_geo_mappings('geo_ids')
    template: Optional[str] = geo_id_mappings.get(geo_type)
    if template is None:
        return None
    return template.format(geo_id=geo_id)
