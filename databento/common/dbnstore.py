from __future__ import annotations

import abc
import datetime as dt
import functools
import logging
from collections.abc import Generator
from io import BytesIO
from os import PathLike
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
)

import databento_dbn
import numpy as np
import pandas as pd
import zstandard
from databento_dbn import Compression
from databento_dbn import DBNDecoder
from databento_dbn import ErrorMsg
from databento_dbn import Metadata
from databento_dbn import Schema
from databento_dbn import SType
from databento_dbn import SymbolMappingMsg
from databento_dbn import SystemMsg

from databento.common.data import COLUMNS
from databento.common.data import DEFINITION_CHARARRAY_COLUMNS
from databento.common.data import DEFINITION_PRICE_COLUMNS
from databento.common.data import DEFINITION_TYPE_MAX_MAP
from databento.common.data import DERIV_SCHEMAS
from databento.common.data import SCHEMA_STRUCT_MAP
from databento.common.data import STRUCT_MAP
from databento.common.error import BentoError
from databento.common.symbology import InstrumentIdMappingInterval
from databento.common.validation import validate_file_write_path
from databento.common.validation import validate_maybe_enum
from databento.live import DBNRecord


NON_SCHEMA_RECORD_TYPES = [
    ErrorMsg,
    SymbolMappingMsg,
    SystemMsg,
    Metadata,
]


logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from databento.historical.client import Historical


def is_zstandard(reader: IO[bytes]) -> bool:
    """
    Determine if an `IO[bytes]` reader contains zstandard compressed data.

    Parameters
    ----------
    reader : IO[bytes]
        The data to check.

    Returns
    -------
    bool

    """
    reader.seek(0)  # ensure we read from the beginning
    try:
        zstandard.get_frame_parameters(reader.read(18))
    except zstandard.ZstdError:
        return False
    else:
        return True


def is_dbn(reader: IO[bytes]) -> bool:
    """
    Determine if an `IO[bytes]` reader contains dbn data.

    Parameters
    ----------
    reader : IO[bytes]
        The data to check.

    Returns
    -------
    bool

    """
    reader.seek(0)  # ensure we read from the beginning
    return reader.read(3) == b"DBN"


class DataSource(abc.ABC):
    """
    Abstract base class for backing DBNStore instances with data.
    """

    def __init__(self, source: object) -> None:
        ...

    @property
    def name(self) -> str:
        ...

    @property
    def nbytes(self) -> int:
        ...

    @property
    def reader(self) -> IO[bytes]:
        ...


class FileDataSource(DataSource):
    """
    A file-backed data source for a DBNStore object.

    Attributes
    ----------
    name : str
        The name of the file.
    nbytes : int
        The size of the data in bytes; equal to the file size.
    path : PathLike or str
        The path of the file.
    reader : IO[bytes]
        A `BufferedReader` for this file-backed data.

    """

    def __init__(self, source: PathLike[str] | str):
        self._path = Path(source)

        if not self._path.is_file() or not self._path.exists():
            raise FileNotFoundError(source)

        if self._path.stat().st_size == 0:
            raise ValueError(
                f"Cannot create data source from empty file: {self._path.name}",
            )

        self._name = self._path.name
        self.__buffer: IO[bytes] | None = None

    @property
    def name(self) -> str:
        """
        Return the name of the file.

        Returns
        -------
        str

        """
        return self._name

    @property
    def nbytes(self) -> int:
        """
        Return the size of the file in bytes.

        Returns
        -------
        int

        """
        return self._path.stat().st_size

    @property
    def path(self) -> Path:
        """
        Return the path to the file.

        Returns
        -------
        pathlib.Path

        """
        return self._path

    @property
    def reader(self) -> IO[bytes]:
        """
        Return a reader for this file.

        Returns
        -------
        IO

        """
        if self.__buffer is None:
            self.__buffer = open(self._path, "rb")
        self.__buffer.seek(0)
        return self.__buffer


class MemoryDataSource(DataSource):
    """
    A memory-backed data source for a DBNStore object.

    Attributes
    ----------
    name : str
        The repr of the source object.
    nbytes : int
        The size of the data in bytes.
    reader : IO[bytes]
        A `BytesIO` for this in-memory buffer.

    """

    def __init__(self, source: BytesIO | bytes | IO[bytes]):
        initial_data = source if isinstance(source, bytes) else source.read()
        if len(initial_data) == 0:
            raise ValueError(
                f"Cannot create data source from empty {type(source).__name__}",
            )
        self.__buffer = BytesIO(initial_data)
        self._name = repr(source)

    @property
    def name(self) -> str:
        """
        Return the name of the source buffer. Equivalent to `repr` of the
        input.

        Returns
        -------
        str

        """
        return self._name

    @property
    def nbytes(self) -> int:
        """
        Return the size of the memory buffer in bytes.

        Returns
        -------
        int

        """
        return self.__buffer.getbuffer().nbytes

    @property
    def reader(self) -> IO[bytes]:
        """
        Return a reader for this buffer. The reader beings at the start of the
        buffer.

        Returns
        -------
        IO

        """
        self.__buffer.seek(0)
        return self.__buffer


class DBNStore:
    """
    A container for Databento Binary Encoding (DBN) data.

    Attributes
    ----------
    compression : Compression
        The data compression format (if any).
    dataset : str
        The dataset code.
    end : pd.Timestamp or None
        The query end for the data.
    limit : int | None
        The query limit for the data.
    mappings : dict[str, list[dict[str, Any]]]:
        The symbology mappings for the data.
    metadata : dict[str, Any]
        The metadata for the data.
    nbytes : int
        The size of the data in bytes.
    raw : bytes
        The raw compressed data in bytes.
    reader : IO[bytes]
        A zstd decompression stream.
    schema : Schema or None
        The data record schema.
    start : pd.Timestamp
        The query start for the data.
    stype_in : SType or None
        The query input symbology type for the data.
    stype_out : SType
        The query output symbology type for the data.
    symbology : dict[str, Any]
        The symbology resolution mappings for the data.
    symbols : list[str]
        The query symbols for the data.

    Methods
    -------
    to_csv
        Write the data to a file in CSV format.
    to_df : pd.DataFrame
        The data as a `pd.DataFrame`.
    to_file
        Write the data to a DBN file at the given path.
    to_json
        Write the data to a file in JSON format.
    to_ndarray : np.ndarray
        The data as a numpy `ndarray`.

    Raises
    ------
    BentoError
        When the data_source does not contain valid DBN data or is corrupted.

    See Also
    --------
    https://docs.databento.com/knowledge-base/new-users/dbn-encoding

    """

    DBN_READ_SIZE = 64 * 1024  # 64kb

    def __init__(self, data_source: DataSource) -> None:
        self._data_source = data_source

        # Check compression
        buffer = self._data_source.reader

        if is_zstandard(buffer):
            self._compression = Compression.ZSTD
            buffer = zstandard.ZstdDecompressor().stream_reader(data_source.reader)
        elif is_dbn(buffer):
            self._compression = Compression.NONE
            buffer = data_source.reader
        else:
            # We don't know how to read this file
            raise BentoError(
                f"Could not determine compression format of {self._data_source.name}",
            )

        # Get metadata length
        metadata_bytes = BytesIO(buffer.read(8))
        metadata_bytes.seek(4)
        metadata_length = int.from_bytes(
            metadata_bytes.read(4),
            byteorder="little",
        )
        self._metadata_length = metadata_length + 8

        metadata_bytes.write(buffer.read(metadata_length))

        # Read metadata
        self._metadata: Metadata = Metadata.decode(
            metadata_bytes.getvalue(),
        )

        # This is populated when _map_symbols is called
        self._instrument_id_index: dict[
            dt.date,
            dict[int, str],
        ] = {}

    def __iter__(self) -> Generator[DBNRecord, None, None]:
        reader = self.reader
        decoder = DBNDecoder()
        while True:
            raw = reader.read(DBNStore.DBN_READ_SIZE)
            if raw:
                decoder.write(raw)
                try:
                    records = decoder.decode()
                except ValueError:
                    continue
                for record in records:
                    if isinstance(record, databento_dbn.Metadata):
                        continue
                    yield record
            else:
                if len(decoder.buffer()) > 0:
                    raise BentoError(
                        "DBN file is truncated or contains an incomplete record",
                    )
                break

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return f"<{name}(schema={self.schema})>"

    def _apply_pretty_ts(self, df: pd.DataFrame) -> pd.DataFrame:
        df.index = pd.to_datetime(df.index, utc=True)
        for column in df.columns:
            if column.startswith("ts_") and "delta" not in column:
                df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)

        if self.schema == Schema.DEFINITION:
            df["expiration"] = pd.to_datetime(
                df["expiration"],
                errors="coerce",
                utc=True,
            )
            df["activation"] = pd.to_datetime(
                df["activation"],
                errors="coerce",
                utc=True,
            )

        return df

    def _apply_pretty_px(self, df: pd.DataFrame) -> pd.DataFrame:
        for column in list(df.columns):
            if (
                column in ("price", "open", "high", "low", "close")
                or column.startswith("bid_px")  # MBP
                or column.startswith("ask_px")  # MBP
            ):
                df[column] = df[column] * 1e-9

        if self.schema == Schema.DEFINITION:
            for column in DEFINITION_PRICE_COLUMNS:
                df[column] = df[column] * 1e-9

        return df

    def _build_instrument_id_index(self) -> dict[dt.date, dict[int, str]]:
        intervals: list[InstrumentIdMappingInterval] = []
        for raw_symbol, i in self.mappings.items():
            for row in i:
                symbol = row["symbol"]
                if symbol == "":
                    continue
                intervals.append(
                    InstrumentIdMappingInterval(
                        start_date=row["start_date"],
                        end_date=row["end_date"],
                        raw_symbol=raw_symbol,
                        instrument_id=int(row["symbol"]),
                    ),
                )

        instrument_id_index: dict[dt.date, dict[int, str]] = {}
        for interval in intervals:
            for ts in pd.date_range(
                start=interval.start_date,
                end=interval.end_date,
                # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.date_range.html
                **{"inclusive" if pd.__version__ >= "1.4.0" else "closed": "left"},
            ):
                d: dt.date = ts.date()
                date_map: dict[int, str] = instrument_id_index.get(d, {})
                if not date_map:
                    instrument_id_index[d] = date_map
                date_map[interval.instrument_id] = interval.raw_symbol

        return instrument_id_index

    def _prepare_dataframe(
        self,
        df: pd.DataFrame,
        schema: Schema,
    ) -> pd.DataFrame:
        if schema == Schema.MBO or schema in DERIV_SCHEMAS:
            df["flags"] = df["flags"] & 0xFF  # Apply bitmask
            df["side"] = df["side"].str.decode("utf-8")
            df["action"] = df["action"].str.decode("utf-8")
        elif schema == Schema.DEFINITION:
            for column in DEFINITION_CHARARRAY_COLUMNS:
                df[column] = df[column].str.decode("utf-8")
            for column, type_max in DEFINITION_TYPE_MAX_MAP.items():
                if column in df.columns:
                    df[column] = df[column].where(df[column] != type_max, np.nan)
        return df

    def _get_index_column(self, schema: Schema) -> str:
        return (
            "ts_event"
            if schema
            in (
                Schema.OHLCV_1S,
                Schema.OHLCV_1M,
                Schema.OHLCV_1H,
                Schema.OHLCV_1D,
            )
            else "ts_recv"
        )

    def _map_symbols(self, df: pd.DataFrame, pretty_ts: bool) -> pd.DataFrame:
        # Build instrument ID index
        if not self._instrument_id_index:
            self._instrument_id_index = self._build_instrument_id_index()

        # Map instrument IDs to raw symbols
        if self._instrument_id_index:
            df_index = df.index if pretty_ts else pd.to_datetime(df.index, utc=True)
            dates = [ts.date() for ts in df_index]
            df["symbol"] = [
                self._instrument_id_index[dates[i]][p]
                for i, p in enumerate(df["instrument_id"])
            ]

        return df

    @property
    def compression(self) -> Compression:
        """
        Return the data compression format (if any). This is determined by
        inspecting the data.

        Returns
        -------
        Compression

        """
        return self._compression

    @property
    def dataset(self) -> str:
        """
        Return the dataset code.

        Returns
        -------
        str

        """
        return str(self._metadata.dataset)

    @property
    def end(self) -> pd.Timestamp | None:
        """
        Return the query end for the data. If None, the end time was not known
        when the data was generated.

        Returns
        -------
        pd.Timestamp or None

        Notes
        -----
        The data timestamps will not occur after `end`.

        """
        end = self._metadata.end
        if end:
            return pd.Timestamp(self._metadata.end, tz="UTC")
        return None

    @property
    def limit(self) -> int | None:
        """
        Return the query limit for the data.

        Returns
        -------
        int or None

        """
        return self._metadata.limit

    @property
    def nbytes(self) -> int:
        """
        Return the size of the data in bytes.

        Returns
        -------
        int

        """
        return self._data_source.nbytes

    @property
    def mappings(self) -> dict[str, list[dict[str, Any]]]:
        """
        Return the symbology mappings for the data.

        Returns
        -------
        dict[str, list[dict[str, Any]]]

        """
        return self._metadata.mappings

    @property
    def metadata(self) -> Metadata:
        """
        Return the metadata for the data.

        Returns
        -------
        Metadata

        """
        return self._metadata

    @property
    def raw(self) -> bytes:
        """
        Return the raw data from the I/O stream.

        Returns
        -------
        bytes

        See Also
        --------
        DBNStore.reader

        """
        return self._data_source.reader.read()

    @property
    def reader(self) -> IO[bytes]:
        """
        Return an I/O reader for the DBN records.

        Returns
        -------
        BinaryIO

        See Also
        --------
        DBNStore.raw

        """
        if self.compression == Compression.ZSTD:
            reader: IO[bytes] = zstandard.ZstdDecompressor().stream_reader(
                self._data_source.reader,
            )
        else:
            reader = self._data_source.reader

        return reader

    @property
    def schema(self) -> Schema | None:
        """
        Return the DBN record schema. If None, may contain one or more schemas.

        Returns
        -------
        Schema or None

        """
        schema = self._metadata.schema
        if schema:
            return Schema(self._metadata.schema)
        return None

    @property
    def start(self) -> pd.Timestamp:
        """
        Return the query start for the data.

        Returns
        -------
        pd.Timestamp

        Notes
        -----
        The data timestamps will not occur prior to `start`.

        """
        return pd.Timestamp(self._metadata.start, tz="UTC")

    @property
    def stype_in(self) -> SType | None:
        """
        Return the query input symbology type for the data. If None, the
        records may contain mixed STypes.

        Returns
        -------
        SType or None

        """
        stype = self._metadata.stype_in
        if stype:
            return SType(self._metadata.stype_in)
        return None

    @property
    def stype_out(self) -> SType:
        """
        Return the query output symbology type for the data.

        Returns
        -------
        SType

        """
        return SType(self._metadata.stype_out)

    @property
    def symbology(self) -> dict[str, Any]:
        """
        Return the symbology resolution mappings for the data.

        Returns
        -------
        dict[str, Any]

        """
        return {
            "symbols": self.symbols,
            "stype_in": str(self.stype_in),
            "stype_out": str(self.stype_out),
            "start_date": str(self.start.date()),
            "end_date": str(self.end.date()),
            "partial": self._metadata.partial,
            "not_found": self._metadata.not_found,
            "mappings": self.mappings,
        }

    @property
    def symbols(self) -> list[str]:
        """
        Return the query symbols for the data.

        Returns
        -------
        list[str]

        """
        return self._metadata.symbols

    @classmethod
    def from_file(cls, path: PathLike[str] | str) -> DBNStore:
        """
        Load the data from a DBN file at the given path.

        Parameters
        ----------
        path : Path or str
            The path to read from.

        Returns
        -------
        DBNStore

        Raises
        ------
        FileNotFoundError
            If a non-existant file is specified.
        ValueError
            If an empty file is specified.

        """
        return cls(FileDataSource(path))

    @classmethod
    def from_bytes(cls, data: BytesIO | bytes | IO[bytes]) -> DBNStore:
        """
        Load the data from a raw bytes.

        Parameters
        ----------
        data : BytesIO or bytes
            The bytes to read from.

        Returns
        -------
        DBNStore

        Raises
        ------
        ValueError
            If an empty buffer is specified.

        """
        return cls(MemoryDataSource(data))

    def replay(self, callback: Callable[[Any], None]) -> None:
        """
        Replay data by passing records sequentially to the given callback.

        Parameters
        ----------
        callback : callable
            The callback to the data handler.

        """
        for record in self:
            try:
                callback(record)
            except Exception:
                logger.exception(
                    "exception while replaying to user callback",
                )
                raise

    def request_full_definitions(
        self,
        client: Historical,
        path: Path | str | None = None,
    ) -> DBNStore:
        """
        Request full instrument definitions based on the metadata properties.

        Makes a `GET /timeseries.get_range` HTTP request.

        Parameters
        ----------
        client : Historical
            The historical client to use for the request (contains the API key).
        path : Path or str, optional
            The path to stream the data to on disk (will then return a `DBNStore`).

        Returns
        -------
        DBNStore

        Warnings
        --------
        Calling this method will incur a cost.

        """
        return client.timeseries.get_range(
            dataset=self.dataset,
            symbols=self.symbols,
            schema=Schema.DEFINITION,
            start=self.start,
            end=self.end,
            stype_in=self.stype_in,
            stype_out=self.stype_out,
            path=path,
        )

    def request_symbology(self, client: Historical) -> dict[str, Any]:
        """
        Request symbology resolution based on the metadata properties.

        Makes a `GET /symbology.resolve` HTTP request.

        Current symbology mappings from the metadata are also available by
        calling the `.symbology` or `.mappings` properties.

        Parameters
        ----------
        client : Historical
            The historical client to use for the request.

        Returns
        -------
        dict[str, Any]
            A result including a map of input symbol to output symbol across a
            date range.

        """
        return client.symbology.resolve(
            dataset=self.dataset,
            symbols=self.symbols,
            stype_in=self.stype_in,
            stype_out=self.stype_out,
            start_date=self.start.date(),
            end_date=self.end.date(),
        )

    def to_csv(
        self,
        path: Path | str,
        pretty_ts: bool = True,
        pretty_px: bool = True,
        map_symbols: bool = True,
        schema: Schema | str | None = None,
    ) -> None:
        """
        Write the data to a file in CSV format.

        Parameters
        ----------
        path : Path or str
            The file path to write to.
        pretty_ts : bool, default True
            If all timestamp columns should be converted from UNIX nanosecond
            `int` to `pd.Timestamp` tz-aware (UTC).
        pretty_px : bool, default True
            If all price columns should be converted from `int` to `float` at
            the correct scale (using the fixed precision scalar 1e-9).
        map_symbols : bool, default True
            If symbology mappings from the metadata should be used to create
            a 'symbol' column, mapping the instrument ID to its native symbol for
            every record.
        schema : Schema or str, optional
            The schema for the csv.
            This is only required when reading a DBN stream with mixed record types.

        Raises
        ------
        ValueError
            If the schema for the array cannot be determined.

        Notes
        -----
        Requires all the data to be brought up into memory to then be written.

        """
        self.to_df(
            pretty_ts=pretty_ts,
            pretty_px=pretty_px,
            map_symbols=map_symbols,
            schema=schema,
        ).to_csv(path)

    def to_df(
        self,
        pretty_ts: bool = True,
        pretty_px: bool = True,
        map_symbols: bool = True,
        schema: Schema | str | None = None,
    ) -> pd.DataFrame:
        """
        Return the data as a `pd.DataFrame`.

        Parameters
        ----------
        pretty_ts : bool, default True
            If all timestamp columns should be converted from UNIX nanosecond
            `int` to `pd.Timestamp` tz-aware (UTC).
        pretty_px : bool, default True
            If all price columns should be converted from `int` to `float` at
            the correct scale (using the fixed precision scalar 1e-9).
        map_symbols : bool, default True
            If symbology mappings from the metadata should be used to create
            a 'symbol' column, mapping the instrument ID to its native symbol for
            every record.
        schema : Schema or str, optional
            The schema for the dataframe.
            This is only required when reading a DBN stream with mixed record types.

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        ValueError
            If the schema for the array cannot be determined.

        """
        schema = validate_maybe_enum(schema, Schema, "schema")
        if schema is None:
            if self.schema is None:
                raise ValueError("a schema must be specified for mixed DBN data")
            schema = self.schema

        df = pd.DataFrame(
            self.to_ndarray(schema),
            columns=COLUMNS[schema],
        )
        df.set_index(self._get_index_column(schema), inplace=True)

        df = self._prepare_dataframe(df, schema)

        if pretty_ts:
            df = self._apply_pretty_ts(df)

        if pretty_px:
            df = self._apply_pretty_px(df)

        if map_symbols and self.schema != Schema.DEFINITION:
            df = self._map_symbols(df, pretty_ts)

        return df

    def to_file(self, path: Path | str) -> None:
        """
        Write the data to a DBN file at the given path.

        Parameters
        ----------
        path : str
            The file path to write to.

        Raises
        ------
        IsADirectoryError
            If path is a directory.
        FileExistsError
            If path exists.
        PermissionError
            If path is not writable.

        """
        file_path = validate_file_write_path(path, "path")
        with open(file_path, mode="xb") as f:
            f.write(self._data_source.reader.read())
        self._data_source = FileDataSource(file_path)

    def to_json(
        self,
        path: Path | str,
        pretty_ts: bool = True,
        pretty_px: bool = True,
        map_symbols: bool = True,
        schema: Schema | str | None = None,
    ) -> None:
        """
        Write the data to a file in JSON format.

        Parameters
        ----------
        path : Path or str
            The file path to write to.
        pretty_ts : bool, default True
            If all timestamp columns should be converted from UNIX nanosecond
            `int` to `pd.Timestamp` tz-aware (UTC).
        pretty_px : bool, default True
            If all price columns should be converted from `int` to `float` at
            the correct scale (using the fixed precision scalar 1e-9).
        map_symbols : bool, default True
            If symbology mappings from the metadata should be used to create
            a 'symbol' column, mapping the instrument ID to its native symbol for
            every record.
        schema : Schema or str, optional
            The schema for the json.
            This is only required when reading a DBN stream with mixed record types.

        Raises
        ------
        ValueError
            If the schema for the array cannot be determined.

        Notes
        -----
        Requires all the data to be brought up into memory to then be written.

        """
        self.to_df(
            pretty_ts=pretty_ts,
            pretty_px=pretty_px,
            map_symbols=map_symbols,
            schema=schema,
        ).to_json(path, orient="records", lines=True)

    def to_ndarray(
        self,
        schema: Schema | str | None = None,
    ) -> np.ndarray[Any, Any]:
        """
        Return the data as a numpy `ndarray`.

        Parameters
        ----------
        schema : Schema or str, optional
            The schema for the array.
            This is only required when reading a DBN stream with mixed record types.

        Returns
        -------
        np.ndarray

        Raises
        ------
        ValueError
            If the schema for the array cannot be determined.

        """
        schema = validate_maybe_enum(schema, Schema, "schema")
        if schema is None:
            if self.schema is None:
                raise ValueError("a schema must be specified for mixed DBN data")
            schema = self.schema

        schema_records = filter(
            lambda r: isinstance(r, SCHEMA_STRUCT_MAP[schema]),  # type: ignore
            self,
        )

        decoder = functools.partial(np.frombuffer, dtype=STRUCT_MAP[schema])
        result = tuple(map(decoder, map(bytes, schema_records)))

        if not result:
            return np.empty(shape=(0, 1), dtype=STRUCT_MAP[schema])

        return np.ravel(result)
