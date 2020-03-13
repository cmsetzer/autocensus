# autocensus

Python package for collecting American Community Survey (ACS) data from the [Census API], along with associated geospatial points and boundaries, in a pandas dataframe. Uses asyncio/aiohttp to request data concurrently.

[Census API]: https://www.census.gov/developers

## Contents

* [Installation](#installation)
* [Example](#example)
* [Joining geospatial data](#joining-geospatial-data)
  + [Caching](#caching)
* [Publishing to Socrata](#publishing-to-socrata)
  + [Credentials](#credentials)
  + [Example: Create a new dataset](#example-create-a-new-dataset)
  + [Example: Replace rows in an existing dataset](#example-replace-rows-in-an-existing-dataset)
  + [Example: Create a new dataset from multiple queries](#example-create-a-new-dataset-from-multiple-queries)
* [Troubleshooting](#troubleshooting)
  + [Clearing the cache](#clearing-the-cache)
  + [SSL errors](#ssl-errors)

## Installation

autocensus requires Python 3.7 or higher. Install as follows:

```sh
pip install autocensus
```

To run autocensus, you must specify a [Census API key] via either the `census_api_key` keyword argument (as shown in the example below) or by setting the environment variable `CENSUS_API_KEY`.

## Example

```python
from autocensus import Query

# Configure query
query = Query(
    estimate=1,
    years=[2017, 2018],
    variables=['DP03_0025E', 'S0103_C01_104E'],
    for_geo='county:033',
    in_geo=['state:53'],
    # Fill in the following with your actual Census API key
    census_api_key='Your Census API key'
)

# Run query and collect output in dataframe
dataframe = query.run()
```

Output:

| name                    | geo_id         | geo_type | year | date       | variable_code  | variable_label                                                                     | variable_concept                                  | annotation |  value | percent_change | difference | centroid  | internal_point | geometry         |
|-------------------------|----------------|----------|------|------------|----------------|------------------------------------------------------------------------------------|---------------------------------------------------|------------|--------|----------------|------------|-----------|----------------|------------------|
| King County, Washington | 0500000US53033 | county   | 2017 | 2017-12-31 | DP03_0025E     | COMMUTING TO WORK - Mean travel time to work (minutes)                             | Selected Economic Characteristics                 |            |   30.0 |                |            | POINT (…) | POINT (…)      | MULTIPOLYGON (…) |
| King County, Washington | 0500000US53033 | county   | 2018 | 2018-12-31 | DP03_0025E     | COMMUTING TO WORK - Workers 16 years and over - Mean travel time to work (minutes) | Selected Economic Characteristics                 |            |   30.2 |           0.01 |        0.2 | POINT (…) | POINT (…)      | MULTIPOLYGON (…) |
| King County, Washington | 0500000US53033 | county   | 2017 | 2017-12-31 | S0103_C01_104E | Total - Estimate - GROSS RENT - Median gross rent (dollars)                        | Population 65 Years and Over in the United States |            | 1555.0 |                |            | POINT (…) | POINT (…)      | MULTIPOLYGON (…) |
| King County, Washington | 0500000US53033 | county   | 2018 | 2018-12-31 | S0103_C01_104E | Total - Renter-occupied housing units - GROSS RENT - Median gross rent (dollars)   | Population 65 Years and Over in the United States |            | 1674.0 |           0.08 |      119.0 | POINT (…) | POINT (…)      | MULTIPOLYGON (…) |

[Census API key]: https://api.census.gov/data/key_signup.html

## Joining geospatial data

autocensus will automatically join geospatial data (centroids, representative points, and geometry) for the following geography types for years 2013 and on:

* Nation-level
  + `nation`
  + `region`
  + `division`
  + `state`
  + `urban area`
  + `zip code tabulation area`
  + `county`
  + `congressional district`
  + `metropolitan statistical area/micropolitan statistical area`
  + `combined statistical area`
  + `american indian area/alaska native area/hawaiian home land`
  + `new england city and town area`
* State-level
  + `alaska native regional corporation`
  + `block group`
  + `county subdivision`
  + `tract`
  + `place`
  + `public use microdata area`
  + `state legislative district (upper chamber)`
  + `state legislative district (lower chamber)`

For queries spanning earlier years, these geometry fields will be populated with null values. (Census boundary shapefiles are not available for years prior to 2013.)

If you don't need geospatial data, set the keyword arg `join_geography` to `False` when initializing your query:

```python
query = Query(
    estimate=1,
    years=[2017, 2018],
    variables=['DP03_0025E', 'S0103_C01_104E'],
    for_geo='county:033',
    in_geo=['state:53'],
    join_geography=False
)
```

If `join_geography` is `False`, the `centroid`, `internal_point`, and `geometry` columns will not be included in your results.

### Caching

To improve performance across queries, autocensus caches shapefiles on disk by default. The cache location varies by platform:

* Linux: `/home/{username}/.cache/autocensus`
* Mac: `/Users/{username}/Library/Application Support/Caches/autocensus`
* Windows: `C:\\Users\\{username}\\AppData\\Local\\socrata\\autocensus`

You can clear the cache by manually deleting the cache directory or by executing the `autocensus.clear_cache` function. See the section [Troubleshooting: Clearing the cache] for more details.

[Troubleshooting: Clearing the cache]: #clearing-the-cache

## Publishing to Socrata

If [socrata-py] is installed, you can publish query results (or dataframes containing the results of multiple queries) directly to Socrata via the method `Query.to_socrata`.

[socrata-py]: https://github.com/socrata/socrata-py

### Credentials

You must have a Socrata account with appropriate permissions on the domain to which you are publishing. By default, autocensus will look up your Socrata account credentials under the following pairs of common environment variables:

* `SOCRATA_KEY_ID`, `SOCRATA_KEY_SECRET`
* `SOCRATA_USERNAME`, `SOCRATA_PASSWORD`
* `MY_SOCRATA_USERNAME`, `MY_SOCRATA_PASSWORD`
* `SODA_USERNAME`, `SODA_PASSWORD`

Alternatively, you can supply credentials explicitly by way of the `auth` keyword argument:

```python
auth = (os.environ['MY_SOCRATA_KEY'], os.environ['MY_SOCRATA_KEY_SECRET'])
query.to_socrata(
    'some-domain.data.socrata.com',
    auth=auth
)
```

### Example: Create a new dataset

```python
# Run query and publish results as a new dataset on Socrata domain
query.to_socrata(
    'some-domain.data.socrata.com',
    name='Median Commute Time by Colorado County, 2013–2017',  # Optional
    description='1-year estimates from the American Community Survey'  # Optional
)
```

### Example: Replace rows in an existing dataset

```python
# Run query and publish results to an existing dataset on Socrata domain
query.to_socrata(
    'some-domain.data.socrata.com',
    dataset_id='xxxx-xxxx'
)
```

### Example: Create a new dataset from multiple queries

```python
from autocensus import Query
from autocensus.socrata import to_socrata
import pandas as pd

# County-level query
county_query = Query(
    estimate=1,
    years=range(2013, 2018),
    variables=['DP03_0025E'],
    for_geo='county:*',
    in_geo='state:08'
)
county_dataframe = county_query.run()

# State-level query
state_query = Query(
    estimate=1,
    years=range(2013, 2018),
    variables=['DP03_0025E'],
    for_geo='state:08'
)
state_dataframe = state_query.run()

# Concatenate dataframes and upload to Socrata
combined_dataframe = pd.concat([
    county_dataframe,
    state_dataframe
])
to_socrata(
    'some-domain.data.socrata.com',
    dataframe=combined_dataframe,
    name='Median Commute Time for Colorado State and Counties, 2013–2017',  # Optional
    description='1-year estimates from the American Community Survey'  # Optional
)
```

## Troubleshooting

### Clearing the cache

Sometimes it is useful to clear the [cache directory] that autocensus uses to store downloaded shapefiles for future queries, especially if you're running into `BadZipFile: File is not a zip file` errors or other shapefile-related problems. Clear your cache like so:

```python
import autocensus

autocensus.clear_cache()
```

[cache directory]: #caching

### SSL errors

To disable SSL verification, specify `verify_ssl=False` when initializing your `Query`:

```python
query = Query(
    estimate=1,
    years=[2017, 2018],
    variables=['DP03_0025E', 'S0103_C01_104E'],
    for_geo='county:033',
    in_geo=['state:53'],
    verify_ssl=False
)
```
