from __future__ import annotations

import abc
import datetime as dt
import logging
from io import BytesIO
from os import PathLike
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Union,
)

import numpy as np
import pandas as pd
import zstandard
from databento.common.data import (
    COLUMNS,
    DEFINITION_CHARARRAY_COLUMNS,
    DEFINITION_PRICE_COLUMNS,
    DEFINITION_TYPE_MAX_MAP,
    DERIV_SCHEMAS,
    STRUCT_MAP,
)
from databento.common.enums import Compression, Schema, SType
from databento.common.metadata import MetadataDecoder
from databento.common.symbology import ProductIdMappingInterval


logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from databento.historical.client import Historical


def is_zstandard(reader: IO[bytes]) -> bool:
    """
    Determine if an `IO[bytes]` reader contains zstandard compressed
    data.

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
    """Abstract base class for backing Bento classes with data."""

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
    A file-backed data source for a Bento object.

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

    def __init__(self, source: Union[PathLike[str], str]):
        self._path = Path(source)

        if not self._path.is_file() or not self._path.exists():
            raise FileNotFoundError(source)

        self._name = self._path.name
        self.__buffer: Optional[IO[bytes]] = None

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
    A memory-backed data source for a Bento object.

    Attributes
    ----------
    name : str
        The repr of the source object.
    nbytes : int
        The size of the data in bytes.
    reader : IO[bytes]
        A `BytesIO` for this in-memory buffer.

    """

    def __init__(self, source: Union[BytesIO, bytes]):
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
        Return the name of the source buffer.
        Equivelant to `repr` of the input.

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
        Return a reader for this buffer.
        The reader beings at the start of the buffer.

        Returns
        -------
        IO

        """
        self.__buffer.seek(0)
        return self.__buffer


class Bento:
    """
    A container for Databento Binary Encoding (DBN) data.

    Attributes
    ----------
    compression : Compression
        The data compression format (if any).
    dataset : str
        The dataset code.
    dtype : np.dtype
        The binary struct format for the data schema.
    end : pd.Timestamp
        The query end for the data.
    limit : int | None
        The query limit for the data.
    mappings : Dict[str, List[Dict[str, Any]]]:
        The symbology mappings for the data.
    metadata : Dict[str, Any]
        The metadata for the data.
    nbytes : int
        The size of the data in bytes.
    raw : bytes
        The raw compressed data in bytes.
    reader : IO[bytes]
        A zstd decompression stream.
    record_count : int
        The record count.
    schema : Schema
        The data record schema.
    start : pd.Timestamp
        The query start for the data.
    record_size : int
        The binary record size in bytes.
    stype_in : SType
        The query input symbology type for the data.
    stype_out : SType
        The query output symbology type for the data.
    symbology : Dict[str, Any]
        The symbology resolution mappings for the data.
    symbols : List[str]
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

    See Also
    --------
    https://docs.databento.com/knowledge-base/new-users/dbn-encoding

    """

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
            raise RuntimeError(
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
        self._metadata: Dict[str, Any] = MetadataDecoder().decode_to_json(
            metadata_bytes.getvalue(),
        )

        # This is populated when _map_symbols is called
        self._product_id_index: Dict[
            dt.date,
            Dict[int, str],
        ] = {}

    def __iter__(self) -> Generator[np.void, None, None]:
        reader = self.reader
        for _ in range(self.record_count):
            raw = reader.read(self.record_size)
            rec = np.frombuffer(raw, dtype=STRUCT_MAP[self.schema])
            yield rec[0]

    def _apply_pretty_ts(self, df: pd.DataFrame) -> pd.DataFrame:
        df.index = pd.to_datetime(df.index, utc=True)
        for column in df.columns:
            if column.startswith("ts_") and "delta" not in column:
                df[column] = pd.to_datetime(df[column], utc=True)

        if self.schema == Schema.DEFINITION:
            df["expiration"] = pd.to_datetime(df["expiration"], utc=True)
            df["activation"] = pd.to_datetime(df["activation"], utc=True)

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

    def _build_product_id_index(self) -> Dict[dt.date, Dict[int, str]]:
        intervals: List[ProductIdMappingInterval] = []
        for native, i in self.mappings.items():
            for row in i:
                symbol = row["symbol"]
                if symbol == "":
                    continue
                intervals.append(
                    ProductIdMappingInterval(
                        start_date=row["start_date"],
                        end_date=row["end_date"],
                        native=native,
                        product_id=int(row["symbol"]),
                    ),
                )

        product_id_index: Dict[dt.date, Dict[int, str]] = {}
        for interval in intervals:
            for ts in pd.date_range(
                start=interval.start_date,
                end=interval.end_date,
                # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.date_range.html
                **{"inclusive" if pd.__version__ >= "1.4.0" else "closed": "left"},
            ):
                d: dt.date = ts.date()
                date_map: Dict[int, str] = product_id_index.get(d, {})
                if not date_map:
                    product_id_index[d] = date_map
                date_map[interval.product_id] = interval.native

        return product_id_index

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df.set_index(self._get_index_column(), inplace=True)
        df.drop(["length", "rtype"], axis=1, inplace=True)
        if self.schema == Schema.MBO or self.schema in DERIV_SCHEMAS:
            df["flags"] = df["flags"] & 0xFF  # Apply bitmask
            df["side"] = df["side"].str.decode("utf-8")
            df["action"] = df["action"].str.decode("utf-8")
        elif self.schema == Schema.DEFINITION:
            for column in DEFINITION_CHARARRAY_COLUMNS:
                df[column] = df[column].str.decode("utf-8")
            for column, type_max in DEFINITION_TYPE_MAX_MAP.items():
                if column in df.columns:
                    df[column] = df[column].where(df[column] != type_max, np.nan)

        # Reorder columns
        df = df.reindex(columns=COLUMNS[self.schema])

        return df

    def _get_index_column(self) -> str:
        return (
            "ts_event"
            if self.schema
            in (
                Schema.OHLCV_1S,
                Schema.OHLCV_1M,
                Schema.OHLCV_1H,
                Schema.OHLCV_1D,
                Schema.GATEWAY_ERROR,
                Schema.SYMBOL_MAPPING,
            )
            else "ts_recv"
        )

    def _map_symbols(self, df: pd.DataFrame, pretty_ts: bool) -> pd.DataFrame:
        # Build product ID index
        if not self._product_id_index:
            self._product_id_index = self._build_product_id_index()

        # Map product IDs to native symbols
        if self._product_id_index:
            df_index = df.index if pretty_ts else pd.to_datetime(df.index, utc=True)
            dates = [ts.date() for ts in df_index]
            df["symbol"] = [
                self._product_id_index[dates[i]][p]
                for i, p in enumerate(df["product_id"])
            ]

        return df

    @property
    def compression(self) -> Compression:
        """
        Return the data compression format (if any).
        This is determined by inspecting the data.

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
        return str(self._metadata["dataset"])

    @property
    def dtype(self) -> np.dtype[Any]:
        """
        Return the binary struct format for the data schema.

        Returns
        -------
        np.dtype

        """
        return np.dtype(STRUCT_MAP[self.schema])

    @property
    def end(self) -> pd.Timestamp:
        """
        Return the query end for the data.

        Returns
        -------
        pd.Timestamp

        Notes
        -----
        The data timestamps will not occur after `end`.

        """
        return pd.Timestamp(self._metadata["end"], tz="UTC")

    @property
    def limit(self) -> Optional[int]:
        """
        Return the query limit for the data.

        Returns
        -------
        int or None

        """
        return self._metadata["limit"]

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
    def mappings(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Return the symbology mappings for the data.

        Returns
        -------
        Dict[str, List[Dict[str, Any]]]

        """
        return self._metadata["mappings"]

    @property
    def metadata(self) -> Dict[str, Any]:
        """
        Return the metadata for the data.

        Returns
        -------
        Dict[str, Any]

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
        Bento.reader

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
        Bento.raw

        """
        if self.compression == Compression.ZSTD:
            reader: IO[bytes] = zstandard.ZstdDecompressor().stream_reader(
                self._data_source.reader,
            )
        else:
            reader = self._data_source.reader

        # Seek past the metadata to read records
        reader.seek(self._metadata_length)
        return reader

    @property
    def record_count(self) -> int:
        """
        Return the record count.

        Returns
        -------
        int

        """
        return self._metadata["record_count"]

    @property
    def schema(self) -> Schema:
        """
        Return the data record schema.

        Returns
        -------
        Schema

        """
        return Schema(self._metadata["schema"])

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
        return pd.Timestamp(self._metadata["start"], tz="UTC")

    @property
    def record_size(self) -> int:
        """
        Return the binary record size in bytes.

        Returns
        -------
        int

        """
        return self.dtype.itemsize

    @property
    def stype_in(self) -> SType:
        """
        Return the query input symbology type for the data.

        Returns
        -------
        SType

        """
        return SType(self._metadata["stype_in"])

    @property
    def stype_out(self) -> SType:
        """
        Return the query output symbology type for the data.

        Returns
        -------
        SType

        """
        return SType(self._metadata["stype_out"])

    @property
    def symbology(self) -> Dict[str, Any]:
        """
        Return the symbology resolution mappings for the data.

        Returns
        -------
        Dict[str, Any]

        """
        return {
            "symbols": self.symbols,
            "stype_in": str(self.stype_in),
            "stype_out": str(self.stype_out),
            "start_date": str(self.start.date()),
            "end_date": str(self.end.date()),
            "partial": self._metadata["partial"],
            "not_found": self._metadata["not_found"],
            "mappings": self.mappings,
        }

    @property
    def symbols(self) -> List[str]:
        """
        Return the query symbols for the data.

        Returns
        -------
        List[str]

        """
        return self._metadata["symbols"]

    @classmethod
    def from_file(cls, path: Union[PathLike[str], str]) -> "Bento":
        """
        Load the data from a DBN file at the given path.

        Parameters
        ----------
        path : Path or str
            The path to read from.

        Returns
        -------
        Bento

        Raises
        ------
        FileNotFoundError
            If a empty or non-existant file is specified.

        """
        return cls(FileDataSource(path))

    @classmethod
    def from_bytes(cls, data: bytes) -> "Bento":
        """
        Load the data from a raw bytes.

        Parameters
        ----------
        data : bytes
            The bytes to read from.

        Returns
        -------
        Bento

        Raises
        ------
        FileNotFoundError
            If a empty or non-existant file is specified.

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
            except Exception as exc:
                logger.exception(
                    "exception while replaying to user callback",
                    exc_info=exc,
                )
                raise

    def request_full_definitions(
        self,
        client: "Historical",
        path: Optional[Union[Path, str]] = None,
    ) -> "Bento":
        """
        Request full instrument definitions based on the metadata properties.

        Makes a `GET /timeseries.stream` HTTP request.

        Parameters
        ----------
        client : Historical
            The historical client to use for the request (contains the API key).
        path : Path or str, optional
            The path to stream the data to on disk (will then return a `Bento`).

        Returns
        -------
        Bento

        Warnings
        --------
        Calling this method will incur a cost.

        """
        return client.timeseries.stream(
            dataset=self.dataset,
            symbols=self.symbols,
            schema=Schema.DEFINITION,
            start=self.start,
            end=self.end,
            stype_in=self.stype_in,
            stype_out=self.stype_out,
            path=path,
        )

    def request_symbology(self, client: "Historical") -> Dict[str, Any]:
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
        Dict[str, Any]
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
        path: Union[Path, str],
        pretty_ts: bool = True,
        pretty_px: bool = True,
        map_symbols: bool = True,
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
            a 'symbol' column, mapping the product ID to its native symbol for
            every record.

        Notes
        -----
        Requires all the data to be brought up into memory to then be written.

        """
        self.to_df(
            pretty_ts=pretty_ts,
            pretty_px=pretty_px,
            map_symbols=map_symbols,
        ).to_csv(path)

    def to_df(
        self,
        pretty_ts: bool = True,
        pretty_px: bool = True,
        map_symbols: bool = True,
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
            a 'symbol' column, mapping the product ID to its native symbol for
            every record.

        Returns
        -------
        pd.DataFrame

        """
        df = pd.DataFrame(self.to_ndarray())
        df = self._prepare_dataframe(df)

        if pretty_ts:
            df = self._apply_pretty_ts(df)

        if pretty_px:
            df = self._apply_pretty_px(df)

        if map_symbols and self.schema != Schema.DEFINITION:
            df = self._map_symbols(df, pretty_ts)

        return df

    def to_file(self, path: Union[Path, str]) -> None:
        """
        Write the data to a DBN file at the given path.

        Parameters
        ----------
        path : str
            The file path to write to.

        """
        with open(path, mode="xb") as f:
            f.write(self._data_source.reader.read())
        self._data_source = FileDataSource(path)

    def to_json(
        self,
        path: Union[Path, str],
        pretty_ts: bool = True,
        pretty_px: bool = True,
        map_symbols: bool = True,
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
            a 'symbol' column, mapping the product ID to its native symbol for
            every record.

        Notes
        -----
        Requires all the data to be brought up into memory to then be written.

        """
        self.to_df(
            pretty_ts=pretty_ts,
            pretty_px=pretty_px,
            map_symbols=map_symbols,
        ).to_json(path, orient="records", lines=True)

    def to_ndarray(self) -> np.ndarray[Any, Any]:
        """
        Return the data as a numpy `ndarray`.

        Returns
        -------
        np.ndarray

        """
        data: bytes = self.reader.read()
        return np.frombuffer(data, dtype=self.dtype)
