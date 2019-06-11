# autocensus

Python package for collecting American Community Survey (ACS) data from the [Census API], along with associated geospatial points and boundaries, in a pandas dataframe.

Uses aiohttp to call Census endpoints with a series of concurrent requests, which saves a bit of time.

This package is under active development and breaking changes to its API are expected.

[Census API]: https://www.census.gov/developers

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
    estimate=5,
    years=[2014, 2015, 2016, 2017],
    variables=['B01002_001E', 'B03001_001E'],
    for_geo='tract:*',
    in_geo=['state:08', 'county:005'],
    # Fill in the following with your actual Census API key
    census_api_key='Your Census API key'
)

# Run query and collect output in dataframe
dataframe = query.run()
```

Output:

| name                                          | geo_id               | year | date       | variable_code | variable_label     | variable_concept  | annotation | value | percent_change | difference | centroid  | internal_point | geometry         |
|-----------------------------------------------|----------------------|------|------------|---------------|--------------------|-------------------|------------|-------|----------------|------------|-----------|----------------|------------------|
| Census Tract 151, Arapahoe County, Colorado   | 1400000US08005015100 | 2014 | 2014-12-31 | B01002_001E   | Median age - Total | Median Age by Sex |            | 45.7  |                |            | POINT (…) | POINT (…)      | MULTIPOLYGON (…) |
| Census Tract 151, Arapahoe County, Colorado   | 1400000US08005015100 | 2015 | 2015-12-31 | B01002_001E   | Median age - Total | Median Age by Sex |            | 45.2  | -1.1           | -0.5       | POINT (…) | POINT (…)      | MULTIPOLYGON (…) |
| Census Tract 151, Arapahoe County, Colorado   | 1400000US08005015100 | 2016 | 2016-12-31 | B01002_001E   | Median age - Total | Median Age by Sex |            | 45.9  | 1.6            | 0.7        | POINT (…) | POINT (…)      | MULTIPOLYGON (…) |
| Census Tract 151, Arapahoe County, Colorado   | 1400000US08005015100 | 2017 | 2017-12-31 | B01002_001E   | Median age - Total | Median Age by Sex |            | 45.7  | -0.4           | -0.2       | POINT (…) | POINT (…)      | MULTIPOLYGON (…) |
| Census Tract 49.51, Arapahoe County, Colorado | 1400000US08005004951 | 2014 | 2018-12-31 | B01002_001E   | Median age - Total | Median Age by Sex |            | 26.4  |                |            | POINT (…) | POINT (…)      | MULTIPOLYGON (…) |

[Census API key]: https://api.census.gov/data/key_signup.html

## Other tables

By default, autocensus queries the detailed tables of the ACS. If your variables are located in other tables, use the `table` keyword argument:

```python
query = Query(
    estimate=5,
    years=[2016, 2017],
    variables=['DP03_0025E'],
    for_geo='tract:*',
    in_geo=['state:17', 'county:031'],
    table='profile'
)
```

autocensus will map the following table codes to their associated Census API endpoints:

* Detailed tables: `detail`
* Data profiles: `profile`
* Subject tables: `subject`
* Comparison profiles: `cprofile`

## Joining geospatial data

autocensus will automatically join geospatial data (centroids, representative points, and geometry) for the geography types `state`, `county`, `zip code tabulation area`, `tract`, and `place` for years 2013 and on. For queries spanning earlier years, these geometry fields will be populated with null values. (Census boundary shapefiles are not available for years prior to 2013.)

If you don't need geospatial data, set the keyword arg `join_geography` to `False` when initializing your query:

```python
query = Query(
    estimate=5,
    years=[2011, 2012, 2013, 2014, 2015, 2016, 2017],
    variables=['B01002_001E', 'B03001_001E'],
    for_geo='tract:*',
    in_geo=['state:08', 'county:005'],
    join_geography=False
)
```

If `join_geography` is `False`, the `centroid`, `internal_point`, and `geometry` columns will not be included in your results.

## Publishing to Socrata

If [socrata-py] is installed, you can publish query results directly to Socrata via the method `Query.to_socrata`.

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
from autocensus import Query

# Configure query
query = Query(
    estimate=5,
    years=range(2013, 2018),
    variables=['DP03_0025E'],
    for_geo='county:*',
    in_geo=['state:08'],
    table='profile'
)

# Run query and publish results as a new dataset on Socrata domain
query.to_socrata(
    'some-domain.data.socrata.com',
    name='Average Commute Time by Colorado County, 2013–2017'  # Optional
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

## Topics

autocensus is packaged with some pre-built lists of pertinent ACS variables around topics like race, education, and housing. These live within the `autocensus.topics` module:

```python
import autocensus
from autocensus import Query

query = Query(
    estimate=5,
    years=[2013, 2014, 2015, 2016, 2017],
    # Housing variables: B25035_001E, B25064_001E, B25077_001E
    variables=autocensus.topics.housing,
    for_geo='tract:*',
    in_geo=['state:08', 'county:005']
)
```

Topics currently included with autocensus are `population`, `race`, `education`, `income`, and `housing`.

## Tests

Use [pytest] to run the test suite:

```sh
pytest
```

[pytest]: https://pytest.org
