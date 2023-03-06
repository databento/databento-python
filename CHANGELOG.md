# Changelog

## 0.8.1 - 2023-03-05
- Fixed bug in `Bento` iteration where multiple readers were created
- Added `from_dbn` convenience alias for loading DBN files

## 0.8.0 - 2023-03-03
- Integrated DBN encoding `0.3.2`
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
- Upgraded `dbz-python` to `0.2.1`
- Upgraded `zstandard` to `0.19.0`

## 0.6.0 - 2022-12-02
- Added `metadata.get_dataset_condition` method to `Historical` client
- Upgraded `dbz-python` to `0.2.0`

## 0.5.0 - 2022-11-07
 - Fixed dataframe columns for derived data schemas (dropped `channel_id`)
 - Fixed `batch.submit_job` requests for `dbz` encoding
 - Updated `quickstart.ipynb` jupyter notebook

## 0.4.0 - 2022-09-14
 - Upgraded `dbz-python` to `0.1.5`
 - Added `map_symbols` option for `.to_df()` (experimental)

## 0.3.0 - 2022-08-30
 - Initial release
