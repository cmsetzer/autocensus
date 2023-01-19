# Changelog

## [v2.1.1]

* Fixed a bug affecting certain queries with `in_geo=['state:*']`.
* Added `to_socrata` deprecation warning.
* Consolidated dev tooling under Ruff and refreshed dependencies.

## [v2.1.0]

* Increased minimum Python version to 3.8 and refreshed dependencies.

## [v2.0.5]

* Fixed a bug affecting environments where an expected system cache directory does not exist.

## [v2.0.4]

* Updated deprecated `MultiPolygon` iteration logic to quiet Shapely warnings.

## [v2.0.3]

* Refreshed dependencies and removed unnecessary NumPy import.

## [v2.0.2]

* Added optional `wait_for_finish` parameter to `Query.to_socrata`.
* Expanded test coverage and made various internal tweaks/fixes.

## [v2.0.1]

* Added support for multiple Cartographic Boundary Shapefile resolutions (`500k`, `5m`, `20m`) via the optional `Query` parameter `resolution`.
* Consolidated package constants and types in the `constants` module.

## [v2.0.0]

* Adopted Census Gazetteer files as source for point geometry.
* Adopted HTTPX in place of aiohttp for async HTTP requests.
* Relocated logic to reproject geometry from NAD 83 to WGS 84 so that it is only executed when publishing a dataframe to Socrata.
* Changed `Query`'s geospatial parameter from the Boolean `join_geography` to the (optional) string `geometry`, which supports joining either points or polygons.
* Replaced the three default geometry columns `centroid`, `internal_point`, and `geometry` (produced by `join_geography=True`) with the single column `geometry`, which may contain point or polygon data.
* Removed support for `Query` parameters `join_geography` and `verify_ssl`, as well as the long-obsolete `table`.
* Removed the `difference` and `percent_change` columns from output.
* Removed custom logic to clean up and title-case variable label and concept strings from the Census API.
* Streamlined logging for `Query` output and warnings.
* Refreshed minimum versions for all dependencies.

## [v1.1.3]

* Fixed a bug affecting environments where an expected system cache directory does not exist.

## [v1.1.2]

* Refreshed dependencies and loosened some version restrictions.

## [v1.1.1]

* Added logic to drop duplicate rows when finalizing `Query` output.

## [v1.1.0]

* Added support for pandas 1.x.
* Added support for socrata-py 1.x.
* Added logic to programmatically specify column types in Socrata dataset schema, fixing a rare issue wherein all columns would be left as plain text.
* Changed logic for CRS assignment to silence annoying GeoPandas warnings.
* Fixed a bug wherein extra rows were produced for certain geography hierarchies.

## [v1.0.7]

* Added logic to warn and skip missing or corrupted shapefiles.

## [v1.0.6]

* Added logic to insert NAs in place of annotated values.
* Fixed a bug caused by attempting to drop nonexistent columns when pulling in variable metadata.

## [v1.0.5]

* Added `Geo` class to parse specified geographies.
* Fixed a bug wherein extraneous rows were produced for certain geographic hierarchies.
* Removed `topics` module.

## [v1.0.4]

* Added improved error handling and warnings throughout.
* Improved error reporting upon failed retry of a request to the Census API.
* Fixed a bug afflicting shapefile downloads.

## [v1.0.2]

* Adopted Poetry as dependency manager for project.
* Fixed a stubborn race condition bug caused by order of geospatial package imports.

## [v1.0.1]

* Adopted nest-asyncio to enable easier use in Jupyter Notebooks.

## [v1.0.0]

* First major release.

[v2.1.1]: https://github.com/socrata/autocensus/releases/tag/v2.1.1
[v2.1.0]: https://github.com/socrata/autocensus/releases/tag/v2.1.0
[v2.0.5]: https://github.com/socrata/autocensus/releases/tag/v2.0.5
[v2.0.4]: https://github.com/socrata/autocensus/releases/tag/v2.0.4
[v2.0.3]: https://github.com/socrata/autocensus/releases/tag/v2.0.3
[v2.0.2]: https://github.com/socrata/autocensus/releases/tag/v2.0.2
[v2.0.1]: https://github.com/socrata/autocensus/releases/tag/v2.0.1
[v2.0.0]: https://github.com/socrata/autocensus/releases/tag/v2.0.0
[v1.1.3]: https://github.com/socrata/autocensus/releases/tag/v1.1.3
[v1.1.2]: https://github.com/socrata/autocensus/releases/tag/v1.1.2
[v1.1.1]: https://github.com/socrata/autocensus/releases/tag/v1.1.1
[v1.1.0]: https://github.com/socrata/autocensus/releases/tag/v1.1.0
[v1.0.7]: https://github.com/socrata/autocensus/releases/tag/v1.0.7
[v1.0.6]: https://github.com/socrata/autocensus/releases/tag/v1.0.6
[v1.0.5]: https://github.com/socrata/autocensus/releases/tag/v1.0.5
[v1.0.4]: https://github.com/socrata/autocensus/releases/tag/v1.0.4
[v1.0.2]: https://github.com/socrata/autocensus/releases/tag/v1.0.2
[v1.0.1]: https://github.com/socrata/autocensus/releases/tag/v1.0.1
[v1.0.0]: https://github.com/socrata/autocensus/releases/tag/v1.0.0
