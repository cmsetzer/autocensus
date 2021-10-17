from unittest.mock import Mock

import fiona  # noqa
import pytest
from shapely.geometry import MultiPolygon, Polygon

from autocensus.geography import (
    Geo,
    calculate_congress_number,
    coerce_polygon_to_multipolygon,
    determine_gazetteer_code,
    determine_geo_code,
    flatten_geometry,
    identify_affgeoid_field,
    is_shp_file,
    load_geodataframe,
    serialize_to_wkt,
)


def test_geo():
    geo = Geo('state', '08')
    assert geo.type == 'state'
    assert geo.code == '08'
    assert str(geo) == 'state:08'


def test_geo_handles_joined_string():
    geo = Geo('state:08')
    assert geo.type == 'state'
    assert geo.code == '08'
    assert str(geo) == 'state:08'


def test_geo_handles_nation():
    geo = Geo('us')
    assert geo.type == 'us'
    assert geo.code == '*'
    assert str(geo) == 'us:*'


def test_geo_handles_state_abbreviation():
    geo = Geo('state:WA')
    assert geo.type == 'state'
    assert geo.code == '53'
    assert str(geo) == 'state:53'


def test_geo_handles_invalid_state_abbreviation():
    geo = Geo('state:XX')
    assert geo.type == 'state'
    assert geo.code == 'XX'
    assert str(geo) == 'state:XX'


def test_calculate_congress_number():
    congress_numbers = [
        (2019, 116),
        (2018, 116),
        (2017, 115),
        (2016, 115),
        (2015, 114),
        (2014, 114),
        (2013, 113),
        (2012, 113),
        (2011, 112),
        (2010, 112),
    ]
    for year, number in congress_numbers:
        assert calculate_congress_number(year) == number


@pytest.mark.parametrize(
    'geo_type,expected',
    [
        ('us', 'us_nation'),
        ('region', 'us_region'),
        ('division', 'us_division'),
        ('state', 'us_state'),
        ('urban area', 'us_ua10'),
        ('zip code tabulation area', 'us_zcta510'),
        ('county', 'us_county'),
        ('congressional district', 'us_cd116'),
        ('metropolitan statistical area/micropolitan statistical area', 'us_cbsa'),
        ('combined statistical area', 'us_csa'),
        ('american indian area/alaska native area/hawaiian home land', 'us_aiannh'),
        ('new england city and town area', 'us_necta'),
        ('alaska native regional corporation', '02_anrc'),
        ('block group', '08_bg'),
        ('county subdivision', '08_cousub'),
        ('tract', '08_tract'),
        ('place', '08_place'),
        ('public use microdata area', '08_puma10'),
        ('state legislative district (upper chamber)', '08_sldu'),
        ('state legislative district (lower chamber)', '08_sldl'),
    ],
)
def test_determine_geo_code(geo_type, expected):
    assert determine_geo_code(2019, geo_type, '08') == expected


@pytest.mark.parametrize(
    'geo_type,expected',
    [
        ('urban area', 'ua'),
        ('zip code tabulation area', 'zcta'),
        ('county', 'counties'),
        ('congressional district', '116CDs'),
        ('metropolitan statistical area/micropolitan statistical area', 'cbsa'),
        ('american indian area/alaska native area/hawaiian home land', 'aiannh'),
        ('county subdivision', 'cousubs'),
        ('tract', 'tracts'),
        ('place', 'place'),
        ('state legislative district (upper chamber)', 'sldu'),
        ('state legislative district (lower chamber)', 'sldl'),
    ],
)
def test_determine_gazetteer_code(geo_type, expected):
    assert determine_gazetteer_code(2019, geo_type) == expected


def test_is_shp_file():
    shp_file = Mock()
    shp_file.filename = 'shapefile.shp'

    shp_file_upper = Mock()
    shp_file_upper.filename = 'SHAPEFILE.SHP'

    not_shp_file = Mock()
    not_shp_file.filename = 'shapefile.proj'

    assert is_shp_file(shp_file) is True
    assert is_shp_file(shp_file_upper) is True
    assert is_shp_file(not_shp_file) is False


def test_load_geodataframe(shapefile_path):
    shapefile_year = int(shapefile_path.name[3:7])
    geodataframe = load_geodataframe(shapefile_path)
    expected_columns = {
        'geometry',
        'STATEFP',
        'STATENS',
        'AFFGEOID',
        'GEOID',
        'STUSPS',
        'NAME',
        'LSAD',
        'ALAND',
        'AWATER',
        'year',
    }

    assert geodataframe.shape == (2, 11)
    assert set(geodataframe.columns) == expected_columns
    assert geodataframe.crs.name == 'NAD83'
    assert set(geodataframe['year']) == {shapefile_year}


def test_coerce_polygon_to_multipolygon():
    polygon = Polygon()
    multipolygon = MultiPolygon([polygon])
    assert coerce_polygon_to_multipolygon(polygon) == multipolygon
    assert coerce_polygon_to_multipolygon(multipolygon) == multipolygon


def test_flatten_geometry():
    multipolygon_no_z = MultiPolygon([Polygon([(0, 1), (1, 2), (2, 3)])])
    multipolygon_has_z = MultiPolygon([Polygon([(0, 1, 10), (1, 2, 10), (2, 3, 11)])])
    assert flatten_geometry(multipolygon_no_z) == multipolygon_no_z
    assert flatten_geometry(multipolygon_has_z) == multipolygon_no_z
    assert flatten_geometry(None) is None


def test_serialize_to_wkt():
    multipolygon = MultiPolygon([Polygon([(0, 1), (1, 2), (2, 3)])])
    expected = (
        'MULTIPOLYGON ((('
        '0.0000000000000000 1.0000000000000000, '
        '1.0000000000000000 2.0000000000000000, '
        '2.0000000000000000 3.0000000000000000, '
        '0.0000000000000000 1.0000000000000000'
        ')))'
    )
    assert serialize_to_wkt(multipolygon) == expected
    assert serialize_to_wkt(None) is None


@pytest.mark.parametrize('field', ['AFFGEOID', 'AFFGEOID10'])
def test_identify_affgeoid_field(field):
    fields = ['STATEFP', field]
    assert identify_affgeoid_field(fields) == field
