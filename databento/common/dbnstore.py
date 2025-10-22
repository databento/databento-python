from __future__ import annotations

import abc
import datetime
import decimal
import itertools
import logging
import warnings
import zoneinfo
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterator
from collections.abc import Mapping
from io import BufferedReader
from io import BytesIO
from os import PathLike
from pathlib import Path
from typing import IO
from typing import TYPE_CHECKING
from typing import Any
from typing import BinaryIO
from typing import Final
from typing import Literal
from typing import Protocol
from typing import TextIO
from typing import overload

import databento_dbn
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard
from databento_dbn import FIXED_PRICE_SCALE
from databento_dbn import UNDEF_PRICE
from databento_dbn import Compression
from databento_dbn import DBNDecoder
from databento_dbn import Encoding
from databento_dbn import InstrumentDefMsg
from databento_dbn import InstrumentDefMsgV1
from databento_dbn import InstrumentDefMsgV2
from databento_dbn import Metadata
from databento_dbn import RType
from databento_dbn import Schema
from databento_dbn import SType
from databento_dbn import Transcoder
from databento_dbn import VersionUpgradePolicy

from databento.common.constants import DEFINITION_TYPE_MAX_MAP
from databento.common.constants import SCHEMA_STRUCT_MAP
from databento.common.constants import SCHEMA_STRUCT_MAP_V1
from databento.common.constants import SCHEMA_STRUCT_MAP_V2
from databento.common.enums import PriceType
from databento.common.error import BentoError
from databento.common.error import BentoWarning
from databento.common.symbology import InstrumentMap
from databento.common.types import DBNRecord
from databento.common.types import Default
from databento.common.types import MappingIntervalDict
from databento.common.validation import validate_enum
from databento.common.validation import validate_file_write_path
from databento.common.validation import validate_maybe_enum


logger = logging.getLogger(__name__)

PARQUET_CHUNK_SIZE: Final = 2**16

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

    def __init__(self, source: object) -> None: ...

    @property
    def name(self) -> str: ...

    @property
    def nbytes(self) -> int: ...

    @property
    def reader(self) -> IO[bytes]: ...


class FileDataSource(DataSource):
    """
    A file-backed data source for a DBNStore object.

    Attributes
    ----------
    name : str
        The name of the file.
    nbytes : int
        The size of the data in bytes; equal to the file size.
    path : PathLike[str] or str
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
        self.__buffer: BufferedReader | None = None

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
    def reader(self) -> BufferedReader:
        """
        Return a reader for this file.

        Returns
        -------
        BufferedReader

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
        if isinstance(source, bytes):
            initial_data = source
        else:
            source.seek(0)
            initial_data = source.read()

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
    def reader(self) -> BytesIO:
        """
        Return a reader for this buffer. The reader beings at the start of the
        buffer.

        Returns
        -------
        BytesIO

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
    to_parquet
        Write the data to a parquet file.

    Raises
    ------
    BentoError
        When the data_source does not contain valid DBN data or is corrupted.

    See Also
    --------
    https://databento.com/docs/standards-and-conventions/databento-binary-encoding

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
            upgrade_policy=VersionUpgradePolicy.AS_IS,
        )

        self._instrument_map = InstrumentMap()

    def __iter__(self) -> Generator[DBNRecord, None, None]:
        reader = self.reader
        decoder = DBNDecoder(
            upgrade_policy=VersionUpgradePolicy.UPGRADE_TO_V3,
        )
        while True:
            raw = reader.read(DBNStore.DBN_READ_SIZE)
            if raw:
                decoder.write(raw)
                records = decoder.decode()
                for record in records:
                    if isinstance(record, databento_dbn.Metadata):
                        continue
                    if isinstance(record, databento_dbn.SymbolMappingMsg):
                        self._instrument_map.insert_symbol_mapping_msg(record)
                    yield record
            else:
                # This call to decode is required to seek past the decoded records
                # This behavior will be fixed in the next version of databento_dbn
                _ = decoder.decode()
                if len(decoder.buffer()) > 0:
                    warnings.warn(
                        BentoWarning("DBN file is truncated or contains an incomplete record"),
                    )
                break

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return f"<{name}(schema={self.schema})>"

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
    def mappings(self) -> dict[str, list[MappingIntervalDict]]:
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
        IO[bytes]

        See Also
        --------
        DBNStore.raw

        """
        if self.compression == Compression.ZSTD:
            return zstandard.ZstdDecompressor().stream_reader(
                self._data_source.reader,
                read_across_frames=True,
            )
        return self._data_source.reader

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
        return self._metadata.stype_in

    @property
    def stype_out(self) -> SType:
        """
        Return the query output symbology type for the data.

        Returns
        -------
        SType

        """
        return self._metadata.stype_out

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
            "end_date": str(self.end.date()) if self.end else None,
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
        path : PathLike[str] or str
            The path to read from.

        Returns
        -------
        DBNStore

        Raises
        ------
        FileNotFoundError
            If a non-existent file is specified.
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
        data : BytesIO or bytes or IO[bytes]
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

    def insert_symbology_json(
        self,
        json_data: str | Mapping[str, Any] | TextIO,
        clear_existing: bool = True,
    ) -> None:
        """
        Insert the given JSON data obtained from the `symbology.resolve`
        endpoint or a `symbology.json` file.

        Parameters
        ----------
        json_data : str | Mapping[str, Any] | TextIO
            The JSON data to insert.
        clear_existing : bool, default True
            If existing symbology data should be cleared from the internal mappings.

        """
        if clear_existing:
            self._instrument_map.clear()
        self._instrument_map.insert_json(json_data)

    def replay(self, callback: Callable[[DBNRecord], None]) -> None:
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
        path: PathLike[str] | str | None = None,
    ) -> DBNStore:
        """
        Request full instrument definitions based on the metadata properties.

        Makes a `GET /timeseries.get_range` HTTP request.

        Parameters
        ----------
        client : Historical
            The historical client to use for the request (contains the API key).
        path : PathLike[str] or str, optional
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
        if self.end is None:
            end_date = None
        else:
            end_date = self.end.ceil(freq="D").date()

        return client.symbology.resolve(
            dataset=self.dataset,
            symbols=self.symbols,
            stype_in=self.stype_in,
            stype_out=self.stype_out,
            start_date=self.start.date(),
            end_date=end_date,
        )

    def to_csv(
        self,
        path: PathLike[str] | str,
        pretty_px: bool = True,
        pretty_ts: bool = True,
        map_symbols: bool = True,
        compression: Compression | str = Compression.NONE,
        schema: Schema | str | None = None,
        mode: Literal["w", "x"] = "w",
    ) -> None:
        """
        Write the data to a file in CSV format.

        Parameters
        ----------
        path : PathLike[str] or str
            The file path to write to.
        pretty_px : bool, default True
            If all price columns should be converted from `int` to `float` at
            the correct scale (using the fixed-precision scalar 1e-9). Null
            prices are replaced with an empty string.
        pretty_ts : bool, default True
            If all timestamp columns should be converted from UNIX nanosecond
            `int` to tz-aware UTC `pd.Timestamp`.
        map_symbols : bool, default True
            If symbology mappings from the metadata should be used to create
            a 'symbol' column, mapping the instrument ID to its requested symbol for
            every record.
        compression : Compression or str, default `Compression.NONE`
            The output compression for writing.
        schema : Schema or str, optional
            The DBN schema for the csv.
            This is only required when reading a DBN stream with mixed record types.
        mode : str, default "w"
            The file write mode to use, either "x" or "w".

        Raises
        ------
        ValueError
            If the schema for the array cannot be determined.

        """
        compression = validate_enum(compression, Compression, "compression")
        schema = validate_maybe_enum(schema, Schema, "schema")
        file_path = validate_file_write_path(path, "path", exist_ok=mode == "w")
        if schema is None:
            if self.schema is None:
                raise ValueError("a schema must be specified for mixed DBN data")
            schema = self.schema

        with open(file_path, f"{mode}b") as output:
            self._transcode(
                output=output,  # type: ignore [arg-type]
                encoding=Encoding.CSV,
                pretty_px=pretty_px,
                pretty_ts=pretty_ts,
                map_symbols=map_symbols,
                compression=compression,
                schema=schema,
            )

    @overload
    def to_df(
        self,
        price_type: PriceType | str = ...,
        pretty_ts: bool = ...,
        map_symbols: bool = ...,
        schema: Schema | str | None = ...,
        tz: datetime.tzinfo | str = ...,
        count: None = ...,
    ) -> pd.DataFrame: ...

    @overload
    def to_df(
        self,
        price_type: PriceType | str = ...,
        pretty_ts: bool = ...,
        map_symbols: bool = ...,
        schema: Schema | str | None = ...,
        tz: datetime.tzinfo | str = ...,
        count: int = ...,
    ) -> DataFrameIterator: ...

    def to_df(
        self,
        price_type: PriceType | str = PriceType.FLOAT,
        pretty_ts: bool = True,
        map_symbols: bool = True,
        schema: Schema | str | None = None,
        tz: datetime.tzinfo | str | Default[datetime.tzinfo] = Default[datetime.tzinfo](
            datetime.timezone.utc,
        ),
        count: int | None = None,
    ) -> pd.DataFrame | DataFrameIterator:
        """
        Return the data as a `pd.DataFrame`.

        Notes
        -----
        The DataFrame index will be set to `ts_recv` if it exists in the schema, otherwise it will be set to `ts_event`.

        Parameters
        ----------
        price_type : PriceType or str, default "float"
            The price type to use for price fields.
            If "fixed", prices will have a type of `int` in fixed decimal format; each unit representing 1e-9 or 0.000000001.
            If "float", prices will have a type of `float`.
            If "decimal", prices will be instances of `decimal.Decimal`.
        pretty_ts : bool, default True
            If all timestamp columns should be converted from UNIX nanosecond
            `int` to tz-aware `pd.Timestamp`. The timezone can be specified using the `tz` parameter.
        map_symbols : bool, default True
            If symbology mappings from the metadata should be used to create
            a 'symbol' column, mapping the instrument ID to its requested symbol for
            every record.
        schema : Schema or str, optional
            The DBN schema for the dataframe.
            This is only required when reading a DBN stream with mixed record types.
        tz : datetime.tzinfo or str, default UTC
            If `pretty_ts` is `True`, all timestamps will be converted to the specified timezone.
        count : int, optional
            If set, instead of returning a single `DataFrame` a `DataFrameIterator`
            instance will be returned. When iterated, this object will yield
            a `DataFrame` with at most `count` elements until the entire contents
            of the `DBNStore` are exhausted. This can be used to process a large
            `DBNStore` in pieces instead of all at once.

        Returns
        -------
        pd.DataFrame
        DataFrameIterator

        Raises
        ------
        ValueError
            If the DBN schema is unspecified and cannot be determined.

        """
        price_type = validate_enum(price_type, PriceType, "price_type")
        schema = validate_maybe_enum(schema, Schema, "schema")

        if isinstance(tz, Default):
            tz = tz.value  # consume default
        elif not pretty_ts:
            raise ValueError(
                "A timezone was specified when `pretty_ts` is `False`. Did you mean to set `pretty_ts=True`?",
            )

        if isinstance(tz, str):
            tz = zoneinfo.ZoneInfo(tz)
        elif not isinstance(tz, datetime.tzinfo):
            raise ValueError(
                f"The value {tz!r} is not a valid datetime.tzinfo",
            )

        if schema is None:
            if self.schema is None:
                raise ValueError("a schema must be specified for mixed DBN data")
            schema = self.schema

        if count is None:
            records = iter([self.to_ndarray(schema)])
        else:
            records = self.to_ndarray(schema, count)

        if map_symbols:
            self._instrument_map.insert_metadata(self.metadata)

        df_iter = DataFrameIterator(
            records=records,
            count=count,
            struct_type=self._schema_struct_map[schema],
            instrument_map=self._instrument_map,
            tz=tz,
            price_type=price_type,
            pretty_ts=pretty_ts,
            map_symbols=map_symbols,
        )

        if count is None:
            return next(df_iter)

        return df_iter

    def to_parquet(
        self,
        path: PathLike[str] | str,
        price_type: PriceType | str = PriceType.FLOAT,
        pretty_ts: bool = True,
        map_symbols: bool = True,
        schema: Schema | str | None = None,
        mode: Literal["w", "x"] = "w",
        parquet_schema: pa.Schema | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Write the data to a parquet file at the given path.

        Parameters
        ----------
        path: PathLike[str] or str
            The file path to write the data to.
        price_type : str, default "float"
            The price type to use for price fields.
            If "fixed", prices will have a type of `int` in fixed decimal format; each unit representing 1e-9 or 0.000000001.
            If "float", prices will have a type of `float`.
            The "decimal" price type is not supported at this time.
        pretty_ts : bool, default True
            If all timestamp columns should be converted from UNIX nanosecond
            `int` to tz-aware UTC `pyarrow.TimestampType`.
        map_symbols : bool, default True
            If symbology mappings from the metadata should be used to create
            a 'symbol' column, mapping the instrument ID to its requested symbol for
            every record.
        schema : Schema or str, optional
            The DBN schema for the parquet file.
            This is only required when reading a DBN stream with mixed record types.
        mode : str, default "w"
            The file write mode to use, either "x" or "w".
        parquet_schema : pyarrow.Schema, optional
            The pyarrow parquet schema to use to write the parquet file.
            This defaults to a detected schema based on the DataFrame representation.
        **kwargs : Any
            Keyword arguments to pass to the `pyarrow.parquet.ParquetWriter`.
            These can be used to override the default behavior of the writer.

        Raises
        ------
        ValueError
            If an incorrect price type is specified.
            If the DBN schema is unspecified and cannot be determined.

        """
        file_path = validate_file_write_path(path, "path", exist_ok=mode == "w")
        price_type = validate_enum(price_type, PriceType, "price_type")

        if price_type == PriceType.DECIMAL:
            raise ValueError("the 'decimal' price type is not currently supported")

        schema = validate_maybe_enum(schema, Schema, "schema")
        if schema is None:
            if self.schema is None:
                raise ValueError("a schema must be specified for mixed DBN data")
            schema = self.schema

        dataframe_iter = self.to_df(
            price_type=price_type,
            pretty_ts=pretty_ts,
            map_symbols=map_symbols,
            schema=schema,
            count=PARQUET_CHUNK_SIZE,
        )

        writer = None
        try:
            for frame in dataframe_iter:
                if writer is None:
                    # Initialize the writer using the first DataFrame
                    if parquet_schema is None:
                        parquet_schema = pa.Schema.from_pandas(frame)
                    writer = pq.ParquetWriter(
                        where=kwargs.pop("where", file_path),
                        schema=parquet_schema,
                        **kwargs,
                    )
                writer.write_table(
                    pa.Table.from_pandas(
                        frame,
                        schema=parquet_schema,
                    ),
                )
        finally:
            if writer is not None:
                writer.close()

    def to_file(
        self,
        path: PathLike[str] | str,
        mode: Literal["w", "x"] = "w",
        compression: Compression | str | None = None,
    ) -> None:
        """
        Write the data to a DBN file at the given path.

        Parameters
        ----------
        path : PathLike[str] or str
            The file path to write to.
        mode : str, default "w"
            The file write mode to use, either "x" or "w".
        compression : Compression or str, optional
            The compression format to write. If `None`, uses the same compression as the underlying data.

        Raises
        ------
        IsADirectoryError
            If path is a directory.
        FileExistsError
            If path exists.
        PermissionError
            If path is not writable.

        """
        compression = validate_maybe_enum(compression, Compression, "compression")
        file_path = validate_file_write_path(path, "path", exist_ok=mode == "w")

        writer: IO[bytes] | zstandard.ZstdCompressionWriter
        if compression is None or compression == self.compression:
            # Handle trivial case
            with open(file_path, mode=f"{mode}b") as writer:
                reader = self._data_source.reader
                while chunk := reader.read(2**16):
                    writer.write(chunk)
            return

        if compression == Compression.ZSTD:
            writer = zstandard.ZstdCompressor(
                write_checksum=True,
            ).stream_writer(
                open(file_path, mode=f"{mode}b"),
                closefd=True,
            )
        else:
            writer = open(file_path, mode=f"{mode}b")

        try:
            reader = self.reader

            while chunk := reader.read(2**16):
                writer.write(chunk)
        finally:
            writer.close()

    def to_json(
        self,
        path: PathLike[str] | str,
        pretty_px: bool = True,
        pretty_ts: bool = True,
        map_symbols: bool = True,
        compression: Compression | str = Compression.NONE,
        schema: Schema | str | None = None,
        mode: Literal["w", "x"] = "w",
    ) -> None:
        """
        Write the data to a file in JSON format.

        Parameters
        ----------
        path : PathLike[str] or str
            The file path to write to.
        pretty_px : bool, default True
            If all price columns should be converted from `int` to `float` at
            the correct scale (using the fixed-precision scalar 1e-9).
        pretty_ts : bool, default True
            If all timestamp columns should be converted from UNIX nanosecond
            `int` to tz-aware UTC `pd.Timestamp`.
        map_symbols : bool, default True
            If symbology mappings from the metadata should be used to create
            a 'symbol' column, mapping the instrument ID to its requested symbol for
            every record.
        compression : Compression or str, default `Compression.NONE`
            The output compression for writing.
        schema : Schema or str, optional
            The DBN schema for the json.
            This is only required when reading a DBN stream with mixed record types.
        mode : str, default "w"
            The file write mode to use, either "x" or "w".

        Raises
        ------
        ValueError
            If the schema for the array cannot be determined.

        """
        compression = validate_enum(compression, Compression, "compression")
        schema = validate_maybe_enum(schema, Schema, "schema")
        file_path = validate_file_write_path(path, "path", exist_ok=mode == "w")

        if schema is None:
            if self.schema is None:
                raise ValueError("a schema must be specified for mixed DBN data")
            schema = self.schema

        with open(file_path, f"{mode}b") as output:
            self._transcode(
                output=output,  # type: ignore [arg-type]
                encoding=Encoding.JSON,
                pretty_px=pretty_px,
                pretty_ts=pretty_ts,
                map_symbols=map_symbols,
                compression=compression,
                schema=schema,
            )

    @overload
    def to_ndarray(  # type: ignore [misc]
        self,
        schema: Schema | str | None = ...,
        count: None = ...,
    ) -> np.ndarray[Any, Any]: ...

    @overload
    def to_ndarray(
        self,
        schema: Schema | str | None = ...,
        count: int = ...,
    ) -> NDArrayIterator: ...

    def to_ndarray(
        self,
        schema: Schema | str | None = None,
        count: int | None = None,
    ) -> np.ndarray[Any, Any] | NDArrayIterator:
        """
        Return the data as a numpy `ndarray`.

        Parameters
        ----------
        schema : Schema or str, optional
            The DBN schema for the array.
            This is only required when reading a DBN stream with mixed record types.
        count : int, optional
            If set, instead of returning a single `np.ndarray` a `NDArrayIterator`
            instance will be returned. When iterated, this object will yield
            a `np.ndarray` with at most `count` elements until the entire contents
            of the `DBNStore` are exhausted. This can be used to process a large
            `DBNStore` in pieces instead of all at once.

        Returns
        -------
        np.ndarray
        NDArrayIterator

        Raises
        ------
        ValueError
            If the DBN schema is unspecified and cannot be determined.

        """
        schema = validate_maybe_enum(schema, Schema, "schema")
        ndarray_iter: NDArrayIterator

        if self.schema is None:
            # If schema is None, we're handling heterogeneous data from the live client
            # This is less performant because the records of a given schema are not contiguous in memory
            if schema is None:
                raise ValueError("a schema must be specified for mixed DBN data")

            # Always use the latest since DBNStore iteration upgrades
            schema_struct = SCHEMA_STRUCT_MAP[schema]
            schema_dtype = schema_struct._dtypes
            schema_rtype = RType.from_schema(schema)
            schema_filter = filter(lambda r: r.rtype == schema_rtype, self)

            reader = self.reader
            reader.seek(self._metadata_length)
            ndarray_iter = NDArrayBytesIterator(
                records=map(bytes, schema_filter),
                dtype=schema_dtype,
                count=count,
            )
        else:
            # If schema is set, we're handling homogeneous historical data
            schema_struct = self._schema_struct_map[self.schema]
            schema_dtype = schema_struct._dtypes

            if self._metadata.ts_out:
                schema_dtype.append(("ts_out", "u8"))

            if schema is not None and schema != self.schema:
                # This is to maintain identical behavior with NDArrayBytesIterator
                ndarray_iter = iter([np.empty([0, 1], dtype=schema_dtype)])
            else:
                ndarray_iter = NDArrayStreamIterator(
                    reader=self.reader,
                    dtype=schema_dtype,
                    offset=self._metadata_length,
                    count=count,
                )

        if count is None:
            return next(ndarray_iter, np.empty([0, 1], dtype=schema_dtype))

        return ndarray_iter

    def _transcode(
        self,
        output: BinaryIO,
        encoding: Encoding,
        pretty_px: bool,
        pretty_ts: bool,
        map_symbols: bool,
        compression: Compression,
        schema: Schema,
    ) -> None:
        if map_symbols:
            self._instrument_map.insert_metadata(self.metadata)
            symbol_map = self._instrument_map._data
        else:
            symbol_map = None

        transcoder = Transcoder(
            file=output,
            encoding=encoding,
            compression=compression,
            pretty_px=pretty_px,
            pretty_ts=pretty_ts,
            has_metadata=True,
            map_symbols=map_symbols,
            symbol_interval_map=symbol_map,  # type: ignore [arg-type]
            schema=schema,
        )

        reader = self.reader
        transcoder.write(reader.read(self._metadata_length))
        while byte_chunk := reader.read(2**16):
            transcoder.write(byte_chunk)

        if transcoder.buffer():
            warnings.warn(
                BentoWarning(
                    "DBN file is truncated or contains an incomplete record",
                ),
            )

        transcoder.flush()

    @property
    def _schema_struct_map(self) -> dict[Schema, type[DBNRecord]]:
        """
        Return a mapping of Schema variants to DBNRecord types based on the DBN
        metadata version.

        Returns
        -------
        dict[Schema, type[DBNRecord]]

        """
        if self.metadata.version == 1:
            return SCHEMA_STRUCT_MAP_V1
        if self.metadata.version == 2:
            return SCHEMA_STRUCT_MAP_V2
        return SCHEMA_STRUCT_MAP


class NDArrayIterator(Protocol):
    @abc.abstractmethod
    def __iter__(self) -> NDArrayIterator: ...

    @abc.abstractmethod
    def __next__(self) -> np.ndarray[Any, Any]: ...


class NDArrayStreamIterator(NDArrayIterator):
    """
    Iterator for homogeneous byte streams of DBN records.
    """

    def __init__(
        self,
        reader: IO[bytes],
        dtype: list[tuple[str, str]],
        offset: int = 0,
        count: int | None = None,
    ) -> None:
        self._reader = reader
        self._dtype: np.dtype[Any] = np.dtype(dtype)
        self._offset = offset
        self._count = count
        self._close_on_next = False

        self._reader.seek(offset)

    def __iter__(self) -> NDArrayStreamIterator:
        return self

    def __next__(self) -> np.ndarray[Any, Any]:
        if self._close_on_next:
            raise StopIteration

        if self._count is None:
            read_size = -1
        else:
            read_size = self._dtype.itemsize * max(self._count, 1)

        if buffer := self._reader.read(read_size):
            loose_bytes = len(buffer) % self._dtype.itemsize
            if loose_bytes != 0:
                warnings.warn(
                    BentoWarning("DBN file is truncated or contains an incomplete record"),
                )
                buffer = buffer[:-loose_bytes]
                self._close_on_next = True  # decode one more buffer before stopping

            try:
                return np.frombuffer(
                    buffer=buffer,
                    dtype=self._dtype,
                )
            except ValueError as exc:
                raise BentoError("Cannot decode DBN stream") from exc

        raise StopIteration


class NDArrayBytesIterator(NDArrayIterator):
    """
    Iterator for heterogeneous streams of DBN records.
    """

    def __init__(
        self,
        records: Iterator[bytes],
        dtype: list[tuple[str, str]],
        count: int | None,
    ):
        self._records = records
        self._dtype = dtype
        self._count = count
        self._first_next = True

    def __iter__(self) -> NDArrayIterator:
        return self

    def __next__(self) -> np.ndarray[Any, Any]:
        record_bytes = BytesIO()
        num_records = 0
        for record in itertools.islice(self._records, self._count):
            num_records += 1
            record_bytes.write(record)

        if self._first_next:
            self._first_next = False
            if num_records == 0:
                return np.empty([0, 1], dtype=self._dtype)

        if num_records == 0:
            raise StopIteration

        try:
            return np.frombuffer(
                record_bytes.getbuffer(),
                dtype=self._dtype,
                count=num_records,
            )
        except ValueError as exc:
            raise BentoError("Cannot decode DBN stream") from exc


class DataFrameIterator:
    """
    Iterator for DataFrames that supports batching and column formatting for
    DBN records.
    """

    def __init__(
        self,
        records: Iterator[np.ndarray[Any, Any]],
        count: int | None,
        struct_type: type[DBNRecord],
        instrument_map: InstrumentMap,
        tz: datetime.tzinfo,
        price_type: PriceType = PriceType.FLOAT,
        pretty_ts: bool = True,
        map_symbols: bool = True,
    ):
        self._records = records
        self._count = count
        self._struct_type = struct_type
        self._price_type = price_type
        self._pretty_ts = pretty_ts
        self._map_symbols = map_symbols
        self._instrument_map = instrument_map
        self._tz = tz

    def __iter__(self) -> DataFrameIterator:
        return self

    def __next__(self) -> pd.DataFrame:
        df = pd.DataFrame(
            next(self._records),
            columns=self._struct_type._ordered_fields,
        )

        if self._struct_type in (InstrumentDefMsg, InstrumentDefMsgV1, InstrumentDefMsgV2):
            self._format_definition_fields(df)

        self._format_hidden_fields(df)

        if self._pretty_ts:
            self._format_pretty_ts(df)

        if self._map_symbols:
            self._format_map_symbols(df)

        if self._pretty_ts:
            self._format_timezone(df)

        self._format_px(df, self._price_type)

        self._format_set_index(df)

        return df

    def _format_definition_fields(self, df: pd.DataFrame) -> None:
        for column, type_max in DEFINITION_TYPE_MAX_MAP.items():
            if column in df.columns:
                df[column] = df[column].where(df[column] != type_max, np.nan)

    def _format_hidden_fields(self, df: pd.DataFrame) -> None:
        for column, dtype in self._struct_type._dtypes:
            hidden_fields = self._struct_type._hidden_fields
            if dtype.startswith("S") and column not in hidden_fields:
                df[column] = df[column].str.decode("utf-8")

    def _format_map_symbols(self, df: pd.DataFrame) -> None:
        # the first ordered field will be ts_recv or ts_event when appropriate
        ts_name = self._struct_type._ordered_fields[0]

        if df.empty:
            df["symbol"] = []
        else:
            df["symbol"] = df.apply(
                lambda r: self._instrument_map.resolve(
                    r["instrument_id"],
                    (
                        r[ts_name] if self._pretty_ts else pd.to_datetime(r[ts_name], utc=True)
                    ).date(),
                ),
                axis=1,
            )

    def _format_timezone(self, df: pd.DataFrame) -> None:
        for field in self._struct_type._timestamp_fields:
            df[field] = df[field].dt.tz_convert(self._tz)

    def _format_px(
        self,
        df: pd.DataFrame,
        price_type: PriceType,
    ) -> None:
        px_fields = self._struct_type._price_fields

        if price_type == PriceType.DECIMAL:
            df[px_fields] = (
                df[px_fields].replace(UNDEF_PRICE, np.nan).applymap(decimal.Decimal)
                / FIXED_PRICE_SCALE
            )
        elif price_type == PriceType.FLOAT:
            df[px_fields] = df[px_fields].replace(UNDEF_PRICE, np.nan) / FIXED_PRICE_SCALE
        else:
            return  # do nothing

    def _format_pretty_ts(self, df: pd.DataFrame) -> None:
        for field in self._struct_type._timestamp_fields:
            df[field] = pd.to_datetime(df[field], utc=True, errors="coerce")

    def _format_set_index(self, df: pd.DataFrame) -> None:
        # the first ordered field will be ts_recv or ts_event when appropriate
        index_column = self._struct_type._ordered_fields[0]
        df.set_index(index_column, inplace=True)
