# Changelog

## 0.50.0 - TBD

#### Enhancements
- Added new venues, datasets, and publishers for Eurex and European Energy Exchange
  (EEX)
- Added export of the following enums from `databento_dbn` to the root `databento` package:
  - `Action`
  - `InstrumentClass`
  - `MatchAlgorithm`
  - `RType`
  - `SecurityUpdateAction`
  - `Side`
  - `StatUpdateAction`
  - `TriState`
  - `UserDefinedInstrument`
  - `VersionUpgradePolicy`
- Added export of the following constants from `databento_dbn` to the root `databento` package:
  - `DBN_VERSION`
  - `FIXED_PRICE_SCALE`
  - `UNDEF_ORDER_SIZE`
  - `UNDEF_PRICE`
  - `UNDEF_STAT_QUANTITY`
  - `UNDEF_TIMESTAMP`
- Added export of `BidAskPair` and `ConsolidatedBidAskPair` from `databento_dbn` to the root `databento` package

## 0.49.0 - 2025-03-04

#### Enhancements
- Added new venues, datasets, and publishers for ICE Futures US and for ICE Futures
  Europe (Financial products)
- Added a `keep_zip` parameter to `Historical.batch.download()`. When `True`, and downloading all files, the jobs contents will be saved as a ZIP file
- Calling `Live.terminate()` will now attempt to write EOF before aborting the connection to help close the remote end

## 0.48.0 - 2025-01-21

#### Breaking changes
- Updated enumerations for unreleased datasets and publishers.

#### Enhancements
- Added export of `StatusAction` enum from `databento_dbn` to the root `databento` package
- Added export of `StatusReason` enum from `databento_dbn` to the root `databento` package
- Added export of `TradingEvent` enum from `databento_dbn` to the root `databento` package
- Added new dataset `EQUS.MINI` and new publishers `EQUS.MINI.EQUS`, `XNYS.TRADES.EQUS`
- Removed upper bound for supported `python` versions; the constraint is now `^3.9`
- Upgraded `databento-dbn` to 0.27.0
    - Fixed export of `InstrumentDefMsgV3` to Python

#### Bug fixes
- Fixed an issue where sending a `KeyboardInterrupt` during iteration of the `Live` client could block execution waiting for the connection to close
- Fixed an issue with submitting historical metadata requests for a large number of symbols.

## 0.47.0 - 2024-12-17

#### Enhancements
- Upgraded `databento-dbn` to 0.25.0
    - Added type aliases for `TBBOMsg`, `BBO1SMsg`, `BBO1MMsg`, `TCBBOMsg`, `CBBO1SMsg`,
      `CBBO1MMsg` in Python
- Removed exports for `CBBOMsg` and `BBOMsg` in the root `databento` package in favor of aliased versions from `databento-dbn`

## 0.46.0 - 2024-12-10

#### Enhancements
- Removed deprecated `packaging` parameter from `Historical.batch.submit_job`. Job files can be downloaded individually or as zip files after the job completes
- Upgraded `databento-dbn` to 0.24.0
    - Added handling for `UNDEF_TIMESTAMP` in `pretty_` timestamp getters for Python. They now return `None` in the case of `UNDEF_TIMESTAMP`

## 0.45.0 - 2024-11-12

This release adds support for Python v3.13.

#### Enhancements
- Added support for Python 3.13
- Added new IntelligentCross venues `ASPN`, `ASMT`, and `ASPI`
- Upgraded `databento-dbn` to 0.23.1
    - Fixed `pretty_activation` getter in `databento_dbn` returning `expiration` instead
    - Fixed some `pretty_` getters in `databento_dbn` didn't correctly handle `UNDEF_PRICE`

#### Deprecations
- Deprecated `packaging` parameter for `Historical.batch.submit_job` which will be removed in a future release

## 0.44.1 - 2024-10-29

#### Enhancements
- Improved exception messages emitted by the `Live` client to always include contents of any `ErrorMsg` sent by the gateway

#### Bug fixes
- Fixed an issue where calling `Live.stop` would not close the connection within a reasonable time

## 0.44.0 - 2024-10-22

#### Enhancements
- Removed deprecated `databento.from_dbn`; `databento.read_dbn` can be used instead
- Upgraded `databento-dbn` to 0.23.0

#### Bug fixes
- Fixed an issue where `DBNStore.request_symbology` could request the wrong end date

## 0.43.1 - 2024-10-15

#### Enhancements
- Keyword arguments to `DBNStore.to_parquet` will now allow `where` and `schema` to be specified
- Improved record processing time for the `Live` client

#### Bug fixes
- Fixed an issue where validating the checksum of a batch file loaded the entire file into memory

## 0.43.0 - 2024-10-09

This release drops support for Python 3.8 which has reached end-of-life.

#### Enhancements
- Added `PriceType` enum for validation of `price_type` parameter in `DBNStore.to_df`
- Upgraded `databento-dbn` to 0.22.1

#### Bug fixes
- Fixed return type hint for `metadata.get_dataset_condition`

#### Breaking changes
- Removed support for Python 3.8 due to end of life

## 0.42.0 - 2024-09-23

#### Enhancements
- Added `mode` parameter to `DBNStore.to_csv` to control the file writing mode
- Added `mode` parameter to `DBNStore.to_json` to control the file writing mode
- Added `mode` parameter to `DBNStore.to_parquet` to control the file writing mode
- Added `compression` parameter to `DBNStore.to_file` which controls the output compression format
- Added new consolidated publisher values for `XNAS.BASIC` and `DBEQ.MAX`
- Changed `DBNStore` to be more tolerant of truncated DBN streams

#### Breaking changes
- Changed default write mode for `DBNStore.to_csv` to overwrite ("w")
- Changed default write mode for `DBNStore.to_json` to overwrite ("w")
- Changed default write mode for `DBNStore.to_parquet` to overwrite ("w")

## 0.41.0 - 2024-09-03

#### Enhancements
- Added `databento.read_dbn` alias
- Added `mode` parameter to `DBNStore.to_file` to control the file writing mode

#### Breaking changes
- Changed default write mode for `DBNStore.to_file` to overwrite ("w")

#### Deprecations
- Deprecated `databento.from_dbn` and will be removed in a future release, use `databento.read_dbn` instead

## 0.40.0 - 2024-08-27

#### Enhancements
- Added `adjustment_factors.get_range(...)` method for `Reference` client
- Added `security_master.get_range(...)` method for `Reference` client
- Added `security_master.get_last(...)` method for `Reference` client
- Upgraded `databento-dbn` to 0.20.1

## 0.39.3 - 2024-08-20

#### Enhancements
- Added new publisher values for `XCIS.BBOTRADES` and `XNYS.BBOTRADES`

#### Bug fixes
- Fixed an issue receiving multiple DBN v1 `ErrorMsg` in the `Live` client would cause an `InvalidState` error
- Fixed an issue where creating `Live` clients in multiple threads could cause a `RuntimeError` upon initialization

## 0.39.2 - 2024-08-13

#### Enhancements
- Changed `corporate_actions.get_range(...)` to stream compressed zstd data

## 0.39.1 - 2024-08-13

#### Bug fixes
- Fixed an issue where a symbol list which contained a `None` would produce a convoluted exception

## 0.39.0 - 2024-07-30

#### Enhancements
- Added new publisher value for `DBEQ.SUMMARY`
- Upgraded `databento-dbn` to 0.20.0

## 0.38.0 - 2024-07-23

This release adds a new feature to the `Live` client for automatically reconnecting when an unexpected disconnection occurs.

#### Enhancements
- Added `Reference` data client with `corporate_actions.get_range(...)` method
- Added `ReconnectPolicy` enumeration
- Added `reconnect_policy` parameter to the `Live` client to specify client reconnection behavior
- Added `Live.add_reconnect_callback` method for specifying a callback to handle client reconnections
- Added platform information to the user agent reported by the `Historical` and `Live` clients
- Upgraded `databento-dbn` to 0.19.1
- Added `BBOMsg`, `CBBOMsg`, and `StatusMsg` exports to the root `databento` package

#### Breaking changes
- Calling `Live.stop` will now clear all user streams and callbacks
- Renamed `Session` to `LiveSession` in the `databento.live.session` module

## 0.37.0 - 2024-07-09

#### Enhancements
- A disconnected `Live` client can now be reused with a different dataset
- Upgraded `databento-dbn` to 0.19.0

## 0.36.3 - 2024-07-02

#### Enhancements
- Added export of `StatType` enum from `databento_dbn` to the root `databento` package

## 0.36.2 - 2024-06-25

#### Enhancements
- Upgraded `databento-dbn` to 0.18.2

## 0.36.1 - 2024-06-18

#### Enhancements
- Added type alias `TBBOMsg` for `MBP1Msg`
- Added support for `bbo-1s`, `bbo-1m`, and `status` schemas
- Instances of the `Live` client will now call `Live.stop` when garbage collected
- Added new publisher values for `XNAS.BASIC` and `XNAS.NLS`

## 0.36.0 - 2024-06-11

#### Enhancements
- Upgraded `databento-dbn` to 0.18.1

#### Bug fixes
- Fixed an issue where `heartbeat_interval_s` was not being sent to the gateway
- Fixed an issue where a truncated DBN stream could be written by the `Live` client in the event of an ungraceful disconnect

#### Breaking changes
- Output streams of the `Live` client added with `Live.add_stream` will now upgrade to the latest DBN version before being written

## 0.35.0 - 2024-06-04

#### Enhancements
- Added optional `heartbeat_interval_s` parameter to `Live` client for configuring the
  interval at which the gateway will send heartbeat records
- Upgraded `databento-dbn` to 0.18.0
- Added new off-market publisher values for `IFEU.IMPACT` and `NDEX.IMPACT`

#### Breaking changes
- Renamed `CbboMsg` to `CBBOMsg`
- Renamed `use_snapshot` parameter in `Live.subscribe` function to `snapshot`
- All Python exceptions raised by `databento-dbn` have been changed to use the `DBNError` type

## 0.34.1 - 2024-05-21

#### Enhancements
- Added `use_snapshot` parameter to `Live.subscribe`, defaults to `False`

## 0.34.0 - 2024-05-14

#### Enhancements
- Added `pip-system-certs` dependency for Windows platforms to prevent a connection issue in `requests` when behind a proxy
- Iteration of the `Live` client will now automatically call `Live.stop` when the iterator is destroyed, such as when a for loop is escaped with an exception or `break` statement

#### Bug fixes
- Fixed an issue where `batch.download` and `batch.download_async` would fail if requested files already existed in the output directory
- Fixed an issue where `batch.download`, `batch.download_async`, and `timeseries.get_range` could use a lot of memory while streaming data
- Fixed an issue where reusing a `Live` client with an open output stream would drop DBN records when received at the same time as the `Metadata` header

##### Deprecations
- The `start_date` and `end_date` keys in the response from `Historical.metadata.get_dataset_range` will be removed in a future release. Use the new `start` and `end` keys instead, which include time resolution

## 0.33.0 - 2024-04-16

#### Enhancements
- The `Historical.batch.download` and `Historical.batch.download_async` methods will now automatically retry the download if a rate limit (HTTP 429) error is received
- The `Historical.batch.download` and `Historical.batch.download_async` methods will now retry failed downloads automatically
- The `Historical.batch.download` and `Historical.batch.download_async` methods will now download files concurrently
- The `output_dir` parameter for `Historical.batch.download` and `Historical.batch.download_async` is now optional and will default to the current working directory if unspecified

#### Breaking changes
- The `enable_partial_downloads` parameter for `Historical.batch.download` and `Historical.batch.download_async` has been removed, partial files will always be resumed which was the default behavior
- The parameters for `Historical.batch.download` and `Historical.batch.download_async` have been reordered because `output_dir` is now optional, `job_id` now comes first

## 0.32.0 - 2024-04-04

#### Enhancements
- Improved exception messages when multiple `ErrorMsg` are received by the `Live` client
- Upgraded `databento-dbn` to 0.17.1

#### Bug fixes
- Removed live session ID parsing to `int`, that could cause a session to fail when
  nothing was wrong

#### Breaking changes
- Renamed publishers from deprecated datasets to their respective sources (`XNAS.NLS`
and `XNYS.TRADES` respectively)

#### Deprecations
- Deprecated dataset values `FINN.NLS` and `FINY.TRADES`

## 0.31.1 - 2024-03-20

#### Enhancements
- Increase `Live` session connection and authentication timeouts
- Added new `F_TOB` and `F_MAYBE_BAD_BOOK` variants to `RecordFlags`

#### Bug fixes
- Fixed an issue where calling `Live.subscribe` from a `Live` client callback would cause a deadlock

## 0.31.0 - 2024-03-05

#### Enhancements
- Added `DBNStore.insert_symbology_json` convenience method for adding symbology data from a JSON dict or file path
- Upgraded `databento-dbn` to 0.16.0

## 0.30.0 - 2024-02-22

#### Enhancements
- Changed how `SymbolMappingMsg` objects are ingested by `InstrumentMap` to single source the timestamp parsing from the `databento-dbn` package

#### Bug fixes
- Fixed an issue where setting a timezone in `DBNStore.to_df` could cause invalid symbol mappings

#### Breaking changes
- Changed `Live.add_stream` to use the exclusive write mode when handling file paths so existing files won't be overwritten

## 0.29.0 - 2024-02-13

#### Enhancements
- Added `tz` parameter to `DBNStore.to_df` which will convert all timestamp fields from UTC to a specified timezone when used with `pretty_ts`
- Added new publisher values for consolidated DBEQ.MAX

#### Bug fixes
- `Live.block_for_close` and `Live.wait_for_close` will now call `Live.stop` when a timeout is reached instead of `Live.terminate` to close the stream more gracefully

## 0.28.0 - 2024-02-01

#### Enhancements
- Substantially increased iteration queue size
- Added methods `DBNQueue.enable` and `DBNQueue.disable` for controlling queue consumption
- Added method `DBNQueue.is_enabled` to signal the queue can accept records
- Added method `DBNQueue.is_full` to signal the queue has reached capacity
- Added enabled checks to `DBNQueue.put` and `DBNQueue.put_nowait`

#### Breaking changes
- Iterating a `Live` client after the streaming session has started will now raise a `ValueError`. Calling `Live.start` is not necessary when iterating the `Live` client
- Moved constant `databento.live.client.DEFAULT_QUEUE_SIZE` to `databento.live.session.DBN_QUEUE_CAPACITY`
- Removed `maxsize` parameter from `DBNQueue` constructor. `DBNQueue` now subclasses `SimpleQueue` instead
- Removed property `DBNQueue.enabled`, use `DBNQueue.is_enabled` instead
- Removed method `DBNQueue.is_half_full`, use `DBNQueue.is_full` instead

#### Bug fixes
- Fixed an issue where DBN records could be dropped while iterating
- Fixed an issue where async iteration would block the event loop

## 0.27.0 - 2024-01-23

#### Enhancements
- Added `Session.session_id` property which will contain the numerical session ID once a live session has been authenticated
- Upgraded `databento-dbn` to 0.15.1

#### Breaking changes
- Renamed `DatabentoLiveProtocol.started` to `DatabentoLiveProtocol.is_started` which now returns a bool instead of an `asyncio.Event`

#### Bug fixes
- Fixed an issue where an error message from the live gateway would not properly raise an exception if the connection closed before `Live.start` was called

## 0.26.0 - 2024-01-16

This release adds support for transcoding DBN data into Apache parquet.

#### Enhancements
- Added `DBNStore.to_parquet` for transcoding DBN data into Apache parquet using `pyarrow`
- Upgraded `databento-dbn` to 0.15.0

## 0.25.0 - 2024-01-09

#### Breaking changes
- Removed deprecated `pretty_px` parameter for `DBNStore.to_df`; `price_type` can be used instead

#### Bug fixes
- Fixed an issue where the `Live` client would not raise an exception when reading an incompatible DBN version
- Fixed an issue where sending lots of subscriptions could cause a `BufferError`
- Fixed an issue where `Historical.batch.download` was slow
- Fixed an issue where `Historical.timeseries.get_range` was slow
- Fixed an issue where reading a DBN file with non-empty metadata symbol mappings and mixed `SType` would cause an error when mapping symbols (credit: Jakob Lövhall)

## 0.24.1 - 2023-12-15

#### Enhancements
- Added new publisher value for OPRA MIAX Sapphire

#### Bug fixes
- Fixed issue where a large unreadable symbol subscription message could be sent
- Fixed issue where calling `Live.stop` could cause a truncated DBN record to be written to a stream

## 0.24.0 - 2023-11-23

This release adds support for DBN v2 as well as Python v3.12.

DBN v2 delivers improvements to the `Metadata` header symbology, new `stype_in` and `stype_out` fields for `SymbolMappingMsg`, and extends the symbol field length for `SymbolMappingMsg` and `InstrumentDefMsg`. The entire change notes are available [here](https://github.com/databento/dbn/releases/tag/v0.14.0). Users who wish to convert DBN v1 files to v2 can use the `dbn-cli` tool available in the [databento-dbn](https://github.com/databento/dbn/) crate. On a future date, the Databento live and historical APIs will stop serving DBN v1.

This release of `databento-python` is fully compatible with both DBN v1 and v2, so this upgrade should be seamless for most users.

In some cases, DBN v1 records will be converted to their v2 counterparts:
- When iterating a `DBNStore` and with `DBNStore.replay`
- When iterating a `Live` client and records dispatched to callbacks

#### Enhancements
- Added support for Python 3.12
- Improved the performance for stream writes in the `Live` client
- Upgraded `databento-dbn` to 0.14.2
- Added `databento.common.types` module to hold common type annotations

#### Bug fixes
- Fixed an issue where specifying an OHLCV schema in `DBNStore.to_ndarray` or `DBNStore.to_df` would not properly filter records by their interval
- Fixed an issue where `DBNStore.to_ndarray` and `DBNStore.to_df` with a non-zero count could get stuck in a loop if the DBN data did not contain any records

#### Breaking Changes
- `DBNStore` iteration and `DBNStore.replay` will upgrade DBN version 1 messages to version 2
- `Live` client iteration and callbacks upgrade DBN version 1 messages to version 2
- Moved `DBNRecord`, `RecordCallback`, and `ExceptionCallback` types to them `databento.common.types` module
- Moved `AUTH_TIMEOUT_SECONDS` and `CONNECT_TIMEOUT_SECONDS` constants from the `databento.live` module to `databento.live.session`
- Moved `INT64_NULL` from the `databento.common.dbnstore` module to `databento.common.constants`
- Moved `SCHEMA_STRUCT_MAP` from the `databento.common.data` module to `databento.common.constants`
- Removed `schema` parameter from `DataFrameIterator` constructor, `struct_type` is to be used instead
- Removed `NON_SCHEMA_RECORD_TYPES` constant as it is no longer used
- Removed `DERIV_SCHEMAS` constant as it is no longer used
- Removed `SCHEMA_COLUMNS` constant as it is no longer used
- Removed `SCHEMA_DTYPES_MAP` constant as it is no longer used
- Removed empty `databento.common.data` module

## 0.23.1 - 2023-11-10

#### Enhancements
- Added new publishers for consolidated DBEQ.BASIC and DBEQ.PLUS

#### Bug fixes
- Fixed an issue where `Live.block_for_close` and `Live.wait_for_close` would not flush streams if the timeout was reached
- Fixed a performance regression when reading a historical DBN file into a numpy array

## 0.23.0 - 2023-10-26

#### Enhancements
- Added `map_symbols_csv` function to the `databento` module for using `symbology.json` files to map a symbol column onto a CSV file
- Added `map_symbols_json` function to the `databento` module for using `symbology.json` files to add a symbol key to a file of JSON records
- Added new publisher values in preparation for IFEU.IMPACT and NDEX.IMPACT datasets

#### Bug fixes
- Fixed issue where a large unreadable symbol subscription message could be sent
- Fixed an issue where `DBNStore.to_df` with `pretty_ts=True` was very slow

## 0.22.1 - 2023-10-24

#### Bug fixes
- Fixed an issue where `DBNStore.to_csv` and `DBNStore.to_json` were mapping symbols even when `map_symbols` was set to `False`
- Fixed an issue where empty symbology mappings caused a `ValueError` when loading symbols into the `DBNStore` instrument map

## 0.22.0 - 2023-10-23

#### Enhancements
- Added `price_type` argument for `DBNStore.to_df` to specify if price fields should be `fixed`, `float` or `decimal.Decimal`
- Added `py.typed` marker file
- Upgraded `databento-dbn` to 0.13.0

#### Breaking Changes
- Changed outputs of `DBNStore.to_csv` and `DBNStore.to_json` to match the encoding formats from the Databento API

#### Deprecations
- Deprecated `pretty_px` argument for `DBNStore.to_df` to be removed in a future release; the default `pretty_px=True` is now equivalent to `price_type="float"` and `pretty_px=False` is now equivalent to `price_type="fixed"`

## 0.21.0 - 2023-10-11

#### Enhancements
- Added `map_symbols` support for DBN data generated by the `Live` client
- Added support for file paths in `Live.add_stream`
- Added new publisher values in preparation for DBEQ.PLUS
- Upgraded `databento-dbn` to 0.11.1

#### Bug fixes
- Fixed an issue where `DBNStore.from_bytes` did not rewind seekable buffers
- Fixed an issue where the `DBNStore` would not map symbols with input symbology of `SType.INSTRUMENT_ID`
- Fixed an issue with `DBNStore.request_symbology` when the DBN metadata's start date and end date were the same
- Fixed an issue where closed streams were not removed from a `Live` client on shutdown

## 0.20.0 - 2023-09-21

#### Enhancements
- Added `ARCX.PILLAR.ARCX` publisher
- Added `pretty_px` option for `batch.submit_job`, which formats prices to the correct scale using the fixed-precision scalar 1e-9 (available for CSV and JSON text encodings)
- Added `pretty_ts` option for `batch.submit_job`, which formats timestamps as ISO 8601 strings (available for CSV and JSON text encodings)
- Added `map_symbols` option for `batch.submit_job`, which appends a symbol field to each text-encoded record (available for CSV and JSON text encodings)
- Added `split_symbols` option for `batch.submit_job`, which will split files by raw symbol
- Upgraded `databento-dbn` to 0.10.2

#### Bug fixes
- Fixed an issue where no disconnection exception were raised when iterating the `Live` client
- Fixed an issue where calling `DBNStore.to_df`, `DBNStore.to_json`, or `DBNStore.to_csv` with `map_symbols=True` would cause a `TypeError`

#### Breaking changes
- Removed `default_value` parameter from `Historical.symbology.resolve`
- Swapped the ordering for the `pretty_px` and `pretty_ts` boolean parameters

## 0.19.1 - 2023-09-08

#### Bug fixes
- Fixed an issue where the index column was not serialized with `DBNStore.to_json`
- Fixed an issue where timestamps serialized by `DBNStore.to_json` had reduced precision

## 0.19.0 - 2023-08-25

This release includes improvements to handling large DBN data and adds support for future datasets.

#### Enhancements
- Added `count` parameter to `DBNStore.to_df` and `DBNStore.to_ndarray` to help process large files incrementally
- Improved memory usage of `DBNStore.to_csv` and `DBNStore.to_json`
- Added the `Publisher`, `Venue`, and `Dataset` enums
- Replace null prices with `NaN` when `pretty_px=True` in `DBNStore.to_df()`
- Upgraded `databento-dbn` to 0.8.3

#### Bug fixes
- Fixed issue where exception messages were displaying JSON encoded data
- Fixed typo in `BATY.PITCH.BATY` publisher
- Reduced floating error when converting prices to floats with `pretty_px=True`

#### Breaking changes
- `DBNStore.to_df` now always utf-8 decodes string fields

## 0.18.1 - 2023-08-16

#### Bug fixes
- Fixed issue where extra `python` key was sent by the `Live` client

## 0.18.0 - 2023-08-14

#### Breaking changes
- Renamed the `TimeSeriesHttpAPI` class to `TimeseriesHttpAPI`

#### Bug fixes
- Fixed an issue where `DBNStore.to_csv()`, `DBNStore.to_df()`, `DBNStore.to_json()`, and `DBNStore.to_ndarray()` would consume large amounts of memory

## 0.17.0 - 2023-08-10

This release includes improvements to the ergonomics of the clients metadata API, you can read more about the changes [here](https://databento.com/blog/api-improvements-august-2023).

#### Enhancements
- Upgraded `databento-dbn` to 0.8.2

#### Breaking changes
- Changed `metadata.list_publishers()` to return a list of publisher details objects
- Changed `metadata.list_fields(...)` to return a list of field detail objects for a particular schema and encoding
- Changed `metadata.list_fields(...)` to require the `schema` and `encoding` parameters
- Changed `metadata.list_unit_prices(...)` to return a list of unit prices for each feed mode and data schema
- Changed `metadata.list_unit_prices(...)` to require the `dataset` parameter
- Removed `metadata.list_unit_prices(...)` `mode` and `schema` parameters
- Removed `metadata.list_fields(...)` `dataset` parameter

## 0.16.1 - 2023-08-03

#### Bug fixes
- Fixed an issue where starting a `Live` client before subscribing gave an incorrect error message
- Fixed an issue where a `Live` client exception callback would fail when the callback function does not have a `__name__` attribute

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
- Added `DatabentoLiveProtocol` class
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
