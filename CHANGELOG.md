# Changelog

## 0.16.1 - TBD

#### Bug fixes
- Fixed an issue where starting a `Live` client before subscribing gave an incorrect error message

## 0.16.0 - 2023-07-25

This release includes updates to the fields in text encodings (CSV and JSON), you can read more about the changes [here](https://databento.com/blog/CSV-JSON-updates-july-2023).

#### Enhancements
- Added `rtype` field to all schemas that was previously excluded

#### Breaking changes
- Reordered fields of DataFrame and CSV encoded records to match historical API

## 0.15.2 - 2023-07-19

#### Bug fixes
- Fixed an issue where the `end` parameter in `timeseries.get_range_async` did not support a value of `None`
- Fixed an issue where `timeseries.get_range` requests would begin with an invalid `path` parameter

## 0.15.1 - 2023-07-06

#### Bug fixes
- Fixed an issue with release tests
- Fixed an issue with release workflow

## 0.15.0 - 2023-07-05

#### Enhancements
- Added `symbology_map` property to `Live` client
- Added `optional_symbols_list_to_list` parsing function
- Changed `Live.add_callback` and `Live.add_stream` to accept an exception callback
- Changed `Live.__iter__()` and `Live.__aiter__()` to send the session start message if the session is connected but not started
- Upgraded `databento-dbn` to 0.7.1
- Removed exception chaining from exceptions emitted by the library

#### Bug fixes
- Fixed issue where a large unreadable symbol subscription message could be sent
- Fixed an `ImportError` observed in Python 3.8

#### Breaking changes
- Removed `Encoding`, `Compression`, `Schema`, and `SType` enums as they are now exposed by `databento-dbn`
- Renamed `func` parameter to `record_callback` for `Live.add_callback` and `Live.add_stream`
- Removed `optional_symbols_list_to_string` parsing function

## 0.14.1 - 2023-06-16

#### Bug fixes
- Fixed issue where `DBNStore.to_df()` would raise an exception if no records were present
- Fixed exception message when creating a DBNStore from an empty data source

## 0.14.0 - 2023-06-14

#### Enhancements
- Added `DatatbentoLiveProtocol` class
- Added `metadata` property to `Live`
- Added support for reusing a `Live` client to reconnect
- Added support for emitting warnings in API response headers
- Relaxed 10 minute minimum request time range restriction
- Upgraded `aiohttp` to 3.8.3
- Upgraded `numpy` to 1.23.5
- Upgraded `pandas` to 1.5.3
- Upgraded `requests` to 2.28.1
- Upgraded `zstandard` to 0.21.0

#### Breaking changes
- Removed support for Python 3.7
- Renamed `symbol` to `raw_symbol` in definition schema when converting to a DataFrame
- Changed iteration of `Live` to no longer yield DBN metadata
- Changed `Live` callbacks to no longer yield DBN metadata

#### Bug fixes
- Fixed issue where `Historical.timeseries.get_range` would write empty files on error
- Fixed issue with `numpy` types not being handled in symbols field
- Fixed optional `end` parameter for `batch.submit_job(...)`

## 0.13.0 - 2023-06-02

#### Enhancements
- Added support for `statistics` schema
- Added batch download support data files (`condition.json` and `symbology.json`)
- Renamed `booklevel` MBP field to `levels` for brevity and consistent naming
- Upgraded `databento-dbn` to 0.6.1

#### Breaking changes
- Changed `flags` field to an unsigned int
- Changed default of `ts_out` to `False` for `Live` client
- Changed `instrument_class` DataFrame representation to be consistent with other `char` types
- Removed `open_interest_qty` and `cleared_volume` fields that were always unset from definition schema
- Removed sunset `timeseries.stream` method
- Removed support for legacy stypes

## 0.12.0 - 2023-05-01

#### Enhancements
- Added `Live` client for connecting to Databento's live service
- Added `degraded`, `pending` and `missing` condition variants for `batch.get_dataset_condition`
- Added `last_modified_date` field to `batch.get_dataset_condition` response
- Upgraded `databento-dbn` to 0.5.0
- Upgraded `DBNStore` to support mixed schema types to support live data

#### Breaking changes
- Changed iteration `DBNStore` to return record types from `databento-dbn` instead of numpy arrays
- Renamed the `cost` field to `cost_usd` for `batch.submit_job` and `batch.list_jobs` (value now expressed as US dollars)
- Renamed `product_id` field to `instrument_id`
- Renamed `symbol` field in definitions to `raw_symbol`
- Removed `dtype` property from `DBNStore`
- Removed `record_size` property from `DBNStore`
- Removed `bad` condition variant from `batch.get_dataset_condition`
- Removed unused `LiveGateway` enum
- Removed `STATSTICS` from `Schema` enum
- Removed `STATUS` from `Schema` enum
- Removed `GATEWAY_ERROR` from `Schema` enum
- Removed `SYMBOL_MAPPING` from `Schema` enum

#### Deprecations
- Deprecated `SType.PRODUCT_ID` to `SType.INSTRUMENT_ID`
- Deprecated `SType.NATIVE` to `SType.RAW_SYMBOL`
- Deprecated `SType.SMART` to `SType.PARENT` and `SType.CONTINUOUS`

## 0.11.0 - 2023-04-13

#### Bug fixes
- Changed `end` and `end_date` to optional to support new forward-fill behaviour
- Upgraded `zstandard` to 0.20.0

## 0.10.0 - 2023-04-07

#### Enhancements
- Added support for `imbalance` schema
- Added `instrument_class`, `strike_price`, and `strike_price_currency` to definition
  schema
- Changed parsing of `end` and `end_date` params throughout the API
- Improved exception messages for server and client timeouts
- Upgraded `databento-dbn` to 0.4.3

#### Breaking changes
- Renamed `Bento` class to `DBNStore`
- Removed `metadata.list_compressions` (redundant with docs)
- Removed `metadata.list_encodings` (redundant with docs)
- Removed optional `start` and `end` params from `metadata.list_schemas` (redundant)
- Removed `related` and `related_security_id` from definition schema

## 0.9.0 - 2023-03-10

#### Enhancements
- Improved use of the logging module

#### Breaking changes
- Removed `record_count` property from Bento class
- Changed `metadata.get_dataset_condition` response to a list of condition per date

#### Bug fixes
- Fixed bug in `Bento` where invalid metadata would prevent iteration

## 0.8.1 - 2023-03-05

#### Enhancements
- Added `from_dbn` convenience alias for loading DBN files

#### Bug fixes
- Fixed bug in `Bento` iteration where multiple readers were created

## 0.8.0 - 2023-03-03

#### Enhancements
- Added `batch.list_files(...)` method
- Added `batch.download(...)` method
- Added `batch.download_async(...)` method
- Integrated DBN encoding 0.3.2

#### Breaking changes
- Dropped support for DBZ encoding
- Renamed `timeseries.stream` to `timeseries.get_range`
- Renamed `timeseries.stream_async` to `timeseries.get_range_async`
- Changed `.to_df(...)` `pretty_ts` default argument to `True`
- Changed `.to_df(...)` `pretty_px` default argument to `True`
- Changed `.to_df(...)` `map_symbols` default argument to `True`

#### Deprecations
- Deprecated `timeseries.stream(...)` method
- Deprecated `timeseries.stream_async(...)` method

## 0.7.0 - 2023-01-10

- Added support for `definition` schema
- Updated `Flags` enum
- Upgraded `dbz-python` to 0.2.1
- Upgraded `zstandard` to 0.19.0

## 0.6.0 - 2022-12-02

- Added `metadata.get_dataset_condition` method to `Historical` client
- Upgraded `dbz-python` to 0.2.0

## 0.5.0 - 2022-11-07

 - Fixed dataframe columns for derived data schemas (dropped `channel_id`)
 - Fixed `batch.submit_job` requests for `dbz` encoding
 - Updated `quickstart.ipynb` jupyter notebook

## 0.4.0 - 2022-09-14

 - Upgraded `dbz-python` to 0.1.5
 - Added `map_symbols` option for `.to_df()` (experimental)

## 0.3.0 - 2022-08-30

 - Initial release
