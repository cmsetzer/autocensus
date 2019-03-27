autocensus
----------

Python package for collecting American Community Survey (ACS) data from the [Census API], along with associated geospatial points and boundaries, in a pandas dataframe.

Uses aiohttp to call Census endpoints with a series of concurrent requests, which saves a bit of time.

This package is under active development and breaking changes to its API are expected.

[Census API]: https://www.census.gov/developers

### Installation

Install from this repository as follows:

```sh
pip install git+git://github.com/socrata/autocensus@master
```

### Example

```python
from autocensus import Query

# Configure query
query = Query(
    estimate=5,
    years=range(2014, 2018),
    variables=['B01002_001E', 'B03001_001E'],
    for_geo='tract:*',
    in_geo=['state:08', 'county:005']
)

# Run query and collect output in dataframe
dataframe = query.run()
```

Output:

| name                                          | geo_id               | year | date       | variable_code | variable_label                | value | percent_change | difference | centroid  | internal_point       | geometry         |
|-----------------------------------------------|----------------------|------|------------|---------------|-------------------------------|-------|----------------|------------|-----------|----------------------|------------------|
| Census Tract 151, Arapahoe County, Colorado   | 1400000US08005015100 | 2014 | 2014-12-31 | B01002_001E   | Estimate - Median age - Total | 45.7  |                |            | POINT (…) | POINT (…)            | MULTIPOLYGON (…) |
| Census Tract 151, Arapahoe County, Colorado   | 1400000US08005015100 | 2015 | 2015-12-31 | B01002_001E   | Estimate - Median age - Total | 45.2  | -1.1           | -0.5       | POINT (…) | POINT (…)            | MULTIPOLYGON (…) |
| Census Tract 151, Arapahoe County, Colorado   | 1400000US08005015100 | 2016 | 2016-12-31 | B01002_001E   | Estimate - Median age - Total | 45.9  | 1.6            | 0.7        | POINT (…) | POINT (…)            | MULTIPOLYGON (…) |
| Census Tract 151, Arapahoe County, Colorado   | 1400000US08005015100 | 2017 | 2017-12-31 | B01002_001E   | Estimate - Median age - Total | 45.7  | -0.4           | -0.2       | POINT (…) | POINT (…)            | MULTIPOLYGON (…) |
| Census Tract 49.51, Arapahoe County, Colorado | 1400000US08005004951 | 2014 | 2018-12-31 | B01002_001E   | Estimate - Median age - Total | 26.4  |                |            | POINT (…) | POINT (…)            | MULTIPOLYGON (…) |

### Joining geospatial data

At present, autocensus supports joining geospatial data for the geography types `state`, `county`, `zip code tabulation area`, `tract`, and `place` for years 2013 and on. For earlier years, you'll need to set the keyword arg `join_geography` to `False` when initializing your query:

```python
query = Query(
    estimate=5,
    years=range(2011, 2018),
    variables=['B01002_001E', 'B03001_001E'],
    for_geo='tract:*',
    in_geo=['state:08', 'county:005'],
    join_geography=False
)
```

### Publishing to Socrata

If [socrata-py] is installed, you can publish to Socrata like so:

```python
from autocensus import Query

# Configure query
query = Query(
    estimate=5,
    years=range(2013, 2018),
    variables=['B01002_001E', 'B03001_001E'],
    for_geo='tract:*',
    in_geo=['state:08', 'county:005']
)

# Run query and publish results to Socrata domain
query.to_socrata('some-domain.data.socrata.com')
```

[socrata-py]: https://github.com/socrata/socrata-py

### Tests

Use [pytest] to run the test suite:

```sh
pytest
```

[pytest]: https://pytest.org
