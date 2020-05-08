"""Utility functions for working with geospatial data."""

# Must import shapely before fiona to prevent GEOS race condition (see
# https://github.com/Toblerity/Shapely/issues/553 for more information)
from shapely.geometry import MultiPolygon, Point, Polygon  # isort:skip

from csv import reader
from dataclasses import dataclass
from io import StringIO
import math
from pathlib import Path
from typing import Dict, Iterable, List, Union
from zipfile import ZipFile, ZipInfo

from fiona.io import ZipMemoryFile
from geopandas import GeoDataFrame
from pkg_resources import resource_string

from .utilities import forgive

# Types
Geometry = Union[MultiPolygon, Point, Polygon]
Shape = Union[MultiPolygon, Polygon]


@dataclass
class Geo:
    """A Census geography, e.g. "state:08", to be used in a query."""

    type: str
    code: str

    def __init__(self, value: str, code: str = None):
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

    def __str__(self):
        return f'{self.type}:{self.code}'


def calculate_congress_number(year: int) -> int:
    """Given a year, calculate the number of the U.S. Congress."""
    congress: int = math.ceil((year - 1789) / 2) + 1
    return congress


def get_geo_codes() -> Dict[str, str]:
    """Read shapefile naming codes from a local CSV."""
    geo_codes_csv: bytes = resource_string(__name__, 'resources/geo_codes.csv')
    csv_reader: Iterable[List[str]] = reader(StringIO(geo_codes_csv.decode('utf-8')))
    geo_codes = {type_: geo_code for type_, geo_code in csv_reader}
    return geo_codes


def determine_geo_code(year: int, for_geo_type: str, state_fips: str) -> str:
    """Determine the shapefile naming code for a given geography."""
    geo_codes: Dict[str, str] = get_geo_codes()
    if for_geo_type != 'congressional district':
        geo_code: str = geo_codes[for_geo_type].format(state_fips=state_fips)
    else:
        congress: int = calculate_congress_number(year)
        geo_code: str = geo_codes[for_geo_type].format(congress=congress)  # type: ignore
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


def coerce_polygon_to_multipolygon(shape: Shape) -> MultiPolygon:
    """Convert a polygon into a MultiPolygon if it's not one already."""
    if not isinstance(shape, MultiPolygon):
        return MultiPolygon([shape])
    else:
        return shape


@forgive(AttributeError)
def flatten_geometry(multipolygon: MultiPolygon) -> MultiPolygon:
    """Flatten a three-dimensional multipolygon to two dimensions."""
    if not multipolygon.has_z:
        return multipolygon
    polygons = []
    for polygon in multipolygon:
        new_coordinates = [(x, y) for (x, y, *_) in polygon.exterior.coords]
        polygons.append(Polygon(new_coordinates))
    flattened_multipolygon = MultiPolygon(polygons)
    return flattened_multipolygon


@forgive(AttributeError)
def serialize_to_wkt(value: Geometry) -> str:
    """Serialize a geometry value to well-known text (WKT)."""
    return value.to_wkt()


def identify_affgeoid_field(fields: Iterable[str]) -> str:
    """Given a series of fields, identify an AFFGEOID field among them.

    Since Census shapefiles sometimes feature different names for the
    AFFGEOID field, it's necessary to build in some extra handling when
    merging geospatial data with table data.
    """
    known_field_names = {'AFFGEOID', 'AFFGEOID10'}
    affgeoid_field: str = (known_field_names & set(fields)).pop()
    return affgeoid_field
