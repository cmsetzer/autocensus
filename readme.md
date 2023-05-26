# autocensus

A Python package for collecting American Community Survey (ACS) data and associated geometry from the [Census API] in a [pandas] dataframe.

[Census API]: https://www.census.gov/developers
[pandas]: https://pandas.pydata.org

## Contents

- [Installation](#installation)
- [Quickstart](#quickstart)
- [Geometry](#geometry)
  - [Points](#points)
  - [Polygons](#polygons)
    - [Shapefile resolution](#shapefile-resolution)
    - [Shapefile caching](#shapefile-caching)

## Installation

autocensus requires Python 3.8 or higher. Install as follows:

```sh
pip install autocensus
```

To run autocensus, you must specify a [Census API key] via either the `census_api_key` keyword argument (as shown in the example below) or by setting the environment variable `CENSUS_API_KEY`.

[Census API key]: https://api.census.gov/data/key_signup.html

## Quickstart

```python
from autocensus import Query

# Configure query
query = Query(
    estimate=1,
    years=[2017, 2018],
    variables=['DP03_0025E', 'S0103_C01_104E'],
    for_geo='county:033',
    in_geo=['state:53'],
    # Optional arg to add geometry: 'points', 'polygons', or None (default)
    geometry='points',
    # Fill in the following with your actual Census API key
    census_api_key='Your Census API key'
)

# Run query and collect output in dataframe
dataframe = query.run()
```

Output:

| name                    | geo_id         | geo_type | year | date       | variable_code  | variable_label                                                                             | variable_concept                                  | annotation |  value | geometry  |
| :---------------------- | :------------- | :------- | ---: | :--------- | :------------- | :----------------------------------------------------------------------------------------- | :------------------------------------------------ | ---------: | -----: | :-------- |
| King County, Washington | 0500000US53033 | county   | 2017 | 2017-12-31 | DP03_0025E     | Estimate!!COMMUTING TO WORK!!Mean travel time to work (minutes)                            | SELECTED ECONOMIC CHARACTERISTICS                 |            |   30.0 | POINT (…) |
| King County, Washington | 0500000US53033 | county   | 2018 | 2018-12-31 | DP03_0025E     | Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Mean travel time to work (minutes) | SELECTED ECONOMIC CHARACTERISTICS                 |            |   30.2 | POINT (…) |
| King County, Washington | 0500000US53033 | county   | 2017 | 2017-12-31 | S0103_C01_104E | Total!!Estimate!!GROSS RENT!!Median gross rent (dollars)                                   | POPULATION 65 YEARS AND OVER IN THE UNITED STATES |            | 1555.0 | POINT (…) |
| King County, Washington | 0500000US53033 | county   | 2018 | 2018-12-31 | S0103_C01_104E | Estimate!!Total!!Renter-occupied housing units!!GROSS RENT!!Median gross rent (dollars)    | POPULATION 65 YEARS AND OVER IN THE UNITED STATES |            | 1674.0 | POINT (…) |

## Geometry

autocensus supports point- and polygon-based geometry data for many years and geographies by way of the Census Bureau's [Gazetteer Files] and [Cartographic Boundary Files].

Here's how to add geometry to your data:

[Gazetteer Files]: https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html
[Cartographic Boundary Files]: https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html

### Points

Point data from the Census Bureau's Gazetteer Files is generally available for years from 2012 on in the following geographies:

- Nation-level
  - `urban area`
  - `zip code tabulation area`
  - `county`
  - `congressional district`
  - `metropolitan statistical area/micropolitan statistical area`
  - `american indian area/alaska native area/hawaiian home land`
- State-level
  - `county subdivision`
  - `tract`
  - `place`
  - `state legislative district (upper chamber)`
  - `state legislative district (lower chamber)`

Example:

```python
from autocensus import Query

query = Query(
    estimate=5,
    years=[2018],
    variables=['DP03_0025E'],
    for_geo=['county:033'],
    in_geo=['state:53'],
    geometry='points'
)
dataframe = query.run()
```

### Polygons

Polygon data from the Census Bureau's Cartographic Boundary Shapefiles is generally available for years from 2013 on in the following geographies:

- Nation-level
  - `nation`
  - `region`
  - `division`
  - `state`
  - `urban area`
  - `zip code tabulation area`
  - `county`
  - `congressional district`
  - `metropolitan statistical area/micropolitan statistical area`
  - `combined statistical area`
  - `american indian area/alaska native area/hawaiian home land`
  - `new england city and town area`
- State-level
  - `alaska native regional corporation`
  - `block group`
  - `county subdivision`
  - `tract`
  - `place`
  - `public use microdata area`
  - `state legislative district (upper chamber)`
  - `state legislative district (lower chamber)`

Example:

```python
from autocensus import Query

query = Query(
    estimate=5,
    years=[2018],
    variables=['DP03_0025E'],
    for_geo=['county:033'],
    in_geo=['state:53'],
    geometry='polygons'
)
dataframe = query.run()
```

#### Shapefile resolution

By default, autocensus will attempt to fetch almost all shapefiles at a resolution of 1 : 500,000 (`500k`). Some sources among the Cartographic Boundary Shapefiles are also available at the lower resolutions of 1 : 5,000,000 (`5m`) or 1 : 20,000,000 (`20m`). To attempt to download a shapefile at a specific resolution, pass a value to `Query`'s optional `resolution` parameter:

```python
from autocensus import Query

query = Query(
    estimate=5,
    years=[2018],
    variables=['DP03_0025E'],
    for_geo=['county:*'],
    in_geo=['state:53'],
    geometry='polygons',
    # Optional arg to set a specific resolution: '500k', '5m', or '20m'
    resolution='20m'
)
```

Setting a specific resolution is only supported for polygon-based geometry.

#### Shapefile caching

To improve performance across queries that include polygon-based geometry data, autocensus will cache Census shapefiles on disk by default. The cache directory location depends on your OS; you can look it up from `autocensus.constants.CACHE_DIRECTORY_PATH` like so:

```shell
python -c "import autocensus; print(autocensus.constants.CACHE_DIRECTORY_PATH)"
```

Sometimes it is useful to clear this cache directory, especially if you're running into persistent shapefile-related problems. You can clear the cache by manually deleting the cache directory or by executing the `autocensus.clear_cache` function:

```shell
python -c "import autocensus; autocensus.clear_cache()"
```
