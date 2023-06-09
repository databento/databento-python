# Changelog

## 0.14.0 - TBD
- Added support for reusing a `Live` client to reconnect
- Changed iteration of `Live` to no longer yield DBN metadata
- Changed `Live` callbacks to no longer yield DBN metadata
- Added `metadata` property to `Live`
- Added `DatatbentoLiveProtocol` class
- Added support for emitting warnings in API response headers
- Upgraded `aiohttp` to 3.8.3
- Upgraded `numpy` to to 1.23.5
- Upgraded `pandas` to to 1.5.3
- Upgraded `requests` to to 2.28.1
- Upgraded `zstandard` to to 0.21.0
- Removed support for Python 3.7

## 0.13.0 - 2023-06-02
- Added support for `statistics` schema
- Added batch download support data files (`condition.json` and `symbology.json`)
- Upgraded `databento-dbn` to 0.6.1
- Renamed `booklevel` MBP field to `levels` for brevity and consistent naming
- Changed `flags` field to an unsigned int
- Changed default of `ts_out` to `False` for `Live` client
- Changed `instrument_class` DataFrame representation to be consistent with other `char` types
- Removed `open_interest_qty` and `cleared_volume` fields that were always unset from definition schema
- Removed sunset `timeseries.stream` method
- Removed support for legacy stypes

## 0.12.0 - 2023-05-01
- Added `Live` client for connecting to Databento's live service
- Upgraded `databento-dbn` to 0.5.0
- Upgraded `DBNStore` to support mixed schema types to support live data
- Changed iteration `DBNStore` to return record types from `databento-dbn` instead of numpy arrays
- Removed `dtype` property from `DBNStore`
- Removed `record_size` property from `DBNStore`
- Renamed the `cost` field to `cost_usd` for `batch.submit_job` and `batch.list_jobs` (value now expressed as US dollars)
- Removed `bad` condition variant from `batch.get_dataset_condition`
- Added `degraded`, `pending` and `missing` condition variants for `batch.get_dataset_condition`
- Added `last_modified_date` field to `batch.get_dataset_condition` response
- Renamed `product_id` field to `instrument_id`
- Renamed `symbol` field in definitions to `raw_symbol`
- Deprecated `SType.PRODUCT_ID` to `SType.INSTRUMENT_ID`
- Deprecated `SType.NATIVE` to `SType.RAW_SYMBOL`
- Deprecated `SType.SMART` to `SType.PARENT` and `SType.CONTINUOUS`
- Removed unused `LiveGateway` enum
- Removed `STATSTICS` from `Schema` enum
- Removed `STATUS` from `Schema` enum
- Removed `GATEWAY_ERROR` from `Schema` enum
- Removed `SYMBOL_MAPPING` from `Schema` enum

## 0.11.0 - 2023-04-13
- Changed `end` and `end_date` to optional to support new forward-fill behaviour
- Upgraded `zstandard` to 0.20.0

## 0.10.0 - 2023-04-07
- Upgraded `databento-dbn` to 0.4.3
- Renamed `Bento` class to `DBNStore`
- Removed `metadata.list_compressions` (redundant with docs)
- Removed `metadata.list_encodings` (redundant with docs)
- Removed optional `start` and `end` params from `metadata.list_schemas` (redundant)
- Removed `related` and `related_security_id` from definition schema
- Added `instrument_class`, `strike_price`, and `strike_price_currency` to definition
  schema
- Added support for `imbalance` schema
- Improved exception messages for server and client timeouts

## 0.9.0 - 2023-03-10
- Removed `record_count` property from Bento class
- Fixed bug in `Bento` where invalid metadata would prevent iteration
- Improved use of the logging module
- Changed `metadata.get_dataset_condition` response to a list of condition per date

## 0.8.1 - 2023-03-05
- Fixed bug in `Bento` iteration where multiple readers were created
- Added `from_dbn` convenience alias for loading DBN files

## 0.8.0 - 2023-03-03
- Integrated DBN encoding 0.3.2
- Renamed `timeseries.stream` to `timeseries.get_range`
- Renamed `timeseries.stream_async` to `timeseries.get_range_async`
- Deprecated `timeseries.stream(...)` method
- Deprecated `timeseries.stream_async(...)` method
- Added `batch.list_files(...)` method
- Added `batch.download(...)` method
- Added `batch.download_async(...)` method
- Changed `.to_df(...)` `pretty_ts` default argument to `True`
- Changed `.to_df(...)` `pretty_px` default argument to `True`
- Changed `.to_df(...)` `map_symbols` default argument to `True`
- Drop support for DBZ encoding

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
