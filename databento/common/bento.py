import io
import json
import os.path
from typing import Any, BinaryIO, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
import zstandard
from databento.common.data import DBZ_COLUMNS, DBZ_STRUCT_MAP, DERIV_SCHEMAS
from databento.common.enums import Compression, Encoding, Schema


class Bento:
    """The abstract base class for all Bento I/O classes."""

    def __init__(
        self,
        schema: Optional[Schema],
        encoding: Optional[Encoding],
        compression: Optional[Compression],
    ):
        self._schema = schema
        self._encoding = encoding
        self._compression = compression

        if self._schema is not None:
            self._struct_fmt = np.dtype(DBZ_STRUCT_MAP[self._schema])
            self._struct_size = self._struct_fmt.itemsize
        else:
            self._struct_fmt = None
            self._struct_size = None

        self._metadata: Optional[Dict[str, Any]] = None
        self._metadata_raw: Optional[bytes] = None

    @property
    def schema(self) -> Schema:
        """
        Return the output schema.

        Returns
        -------
        Schema

        """
        return self._schema

    @property
    def encoding(self) -> Encoding:
        """
        Return the output encoding.

        Returns
        -------
        Encoding

        """
        return self._encoding

    @property
    def compression(self) -> Compression:
        """
        Return the output compression.

        Returns
        -------
        Compression

        """
        return self._compression

    @property
    def struct_fmt(self) -> np.dtype:
        """
        Return the binary struct format for the schema.

        Returns
        -------
        np.dtype

        """
        return self._struct_fmt

    @property
    def struct_size(self) -> int:
        """
        Return the schemas binary struct size in bytes.

        Returns
        -------
        int

        """
        return self._struct_size

    @property
    def nbytes(self) -> int:
        raise NotImplementedError()  # pragma: no cover

    @property
    def raw(self) -> bytes:
        """
        Return the raw data from the I/O stream.

        Returns
        -------
        bytes

        """
        raise NotImplementedError()  # pragma: no cover

    def reader(self, decompress: bool = False) -> BinaryIO:
        """
        Return an I/O reader for the data.

        Parameters
        ----------
        decompress : bool
            If data should be decompressed (if compressed).

        Returns
        -------
        BinaryIO

        """
        raise NotImplementedError()  # pragma: no cover

    def writer(self) -> BinaryIO:
        """
        Return a raw I/O writer for the data.

        Returns
        -------
        BinaryIO

        """
        raise NotImplementedError()  # pragma: no cover

    def to_list(self) -> List[Any]:
        """
        Return the data as a list records.

        - ``DBZ`` encoding will return a list of `np.void` mixed dtypes.
        - ``CSV`` encoding will return a list of `str`.
        - ``JSON`` encoding will return a list of `Dict[str, Any]`.

        Returns
        -------
        List[Any]

        """
        if self._encoding == Encoding.DBZ:
            return self._prepare_list_dbz()
        elif self._encoding == Encoding.CSV:
            return self._prepare_list_csv()
        elif self._encoding == Encoding.JSON:
            return self._prepare_list_json()
        else:  # pragma: no cover (design-time error)
            raise ValueError(f"invalid encoding, was {self._encoding.value}")

    def to_df(self, pretty_ts: bool = False, pretty_px: bool = False) -> pd.DataFrame:
        """
        Return the data as a pd.DataFrame.

        Parameters
        ----------
        pretty_ts : bool, default False
            If the type of any timestamp columns should be converted from UNIX
            nanosecond `int` to `pd.Timestamp` (UTC).
        pretty_px : bool, default False
            If the type of any price columns should be converted from `int` to
            `float` at the correct scale (using the fixed precision scalar 1e-9).

        Returns
        -------
        pd.DataFrame

        """
        df: pd.DataFrame
        if self._encoding == Encoding.DBZ:
            df = self._prepare_df_dbz()
        elif self._encoding == Encoding.CSV:
            df = self._prepare_df_csv()
        elif self._encoding == Encoding.JSON:
            df = self._prepare_df_json()
        else:  # pragma: no cover (design-time error)
            raise ValueError(f"invalid encoding, was {self._encoding.value}")

        if pretty_ts:
            df.index = pd.to_datetime(df.index, utc=True)
            for column in list(df.columns):
                if column.startswith("ts_"):
                    df[column] = pd.to_datetime(df[column], utc=True)

        if pretty_px:
            for column in list(df.columns):
                if (
                    column in ("price", "open", "high", "low", "close")
                    or column.startswith("bid_px")  # MBP
                    or column.startswith("ask_px")  # MBP
                ):
                    df[column] = df[column] * 1e-9

        return df

    def replay(self, callback: Callable[[Any], None]) -> None:
        """
        Pass all data records sequentially to the given callback.

        Parameters
        ----------
        callback : callable
            The callback to the data handler.

        """
        if self._encoding == Encoding.DBZ:
            self._replay_dbz(callback)
        elif self._encoding in (Encoding.CSV, Encoding.JSON):
            self._replay_text(callback)
        else:  # pragma: no cover (design-time error)
            raise ValueError(f"invalid encoding, was {self._encoding.value}")

    def to_file(self, path: str) -> "FileBento":
        """
        Write the data to a file at the given path.

        Parameters
        ----------
        path : str
            The path to write to.

        Returns
        -------
        FileBento

        """
        with open(path, mode="wb") as f:
            if self._metadata_raw:
                f.write(self._metadata_raw)
            f.write(self.raw)

        return FileBento(
            path=path,
            schema=self._schema,
            encoding=self._encoding,
            compression=self._compression,
        )

    def set_metadata_json(self, metadata: Dict[str, Any]) -> None:
        """
        Set metadata from a JSON object.

        Parameters
        ----------
        metadata : Dict[str, Any]
            The metadata to set for the object.

        """
        self._metadata = metadata

        schema = Schema(metadata["schema"])
        encoding = Encoding(metadata["encoding"])
        compression = Compression(metadata["compression"])

        if self._schema is None:
            self._schema = schema
        else:
            assert self._schema == schema, (
                f"Metadata schema '{schema.value}' is not equal to "
                f"existing schema '{self._schema.value}'"
            )

        if self._encoding is None:
            self._encoding = encoding
        # TODO(cs): Improve metadata validation
        # else:
        #     assert self._encoding == encoding, (
        #         f"Metadata encoding '{encoding.value}' is not equal to "
        #         f"existing encoding '{self._encoding.value}'"
        #     )

        if self._compression is None:
            self._compression = compression
        # TODO(cs): Improve metadata validation
        # else:
        #     assert self._compression == compression, (
        #         f"Metadata compression '{compression.value}' is not equal to "
        #         f"existing compression '{self._compression.value}'"
        #     )

    def set_metadata_raw(self, raw: bytes) -> None:
        """
        Set metadata from raw bytes.

        Parameters
        ----------
        raw : bytes
            The raw metadata to set for the object.
        """
        # TODO(cs): Temporary method until consolidated encoder
        self._metadata_raw = raw

    def _should_decompress(self, decompress: bool) -> bool:
        if not decompress:
            return False
        return self._compression == Compression.ZSTD

    def _get_index_column(self) -> str:
        return (
            "ts_recv"
            if self._schema
            not in (
                Schema.OHLCV_1S,
                Schema.OHLCV_1M,
                Schema.OHLCV_1H,
                Schema.OHLCV_1D,
            )
            else "ts_event"
        )

    def _prepare_list_dbz(self) -> List[np.void]:
        data: bytes = self.reader(decompress=True).read()
        return np.frombuffer(data, dtype=DBZ_STRUCT_MAP[self._schema])

    def _prepare_list_csv(self) -> List[str]:
        data: bytes = self.reader(decompress=True).read()
        lines: List[str] = data.decode().splitlines(keepends=False)
        lines.pop(0)  # Remove header row
        return lines

    def _prepare_list_json(self) -> List[Dict]:
        data: bytes = self.reader(decompress=True).read()
        lines: List[str] = data.decode().splitlines(keepends=False)
        return list(map(json.loads, lines))

    def _prepare_df_dbz(self) -> pd.DataFrame:
        df = pd.DataFrame(self.to_list())
        df.set_index(self._get_index_column(), inplace=True)
        # Cleanup dataframe
        if self._schema == Schema.MBO:
            df.drop("chan_id", axis=1, inplace=True)
            df = df.reindex(columns=DBZ_COLUMNS[self._schema])
            df["flags"] = df["flags"] & 0xFF  # Apply bitmask
            df["side"] = df["side"].str.decode("utf-8")
            df["action"] = df["action"].str.decode("utf-8")
        elif self._schema in DERIV_SCHEMAS:
            df.drop(["nwords", "type", "depth"], axis=1, inplace=True)
            df = df.reindex(columns=DBZ_COLUMNS[self._schema])
            df["flags"] = df["flags"] & 0xFF  # Apply bitmask
            df["side"] = df["side"].str.decode("utf-8")
            df["action"] = df["action"].str.decode("utf-8")
        else:
            df.drop(["nwords", "type"], axis=1, inplace=True)

        return df

    def _prepare_df_csv(self) -> pd.DataFrame:
        data: bytes = self.reader(decompress=True).read()
        df = pd.read_csv(io.BytesIO(data), index_col=self._get_index_column())
        return df

    def _prepare_df_json(self) -> pd.DataFrame:
        jsons: List[Dict] = self.to_list()
        df = pd.DataFrame.from_dict(jsons, orient="columns")
        df.set_index(self._get_index_column(), inplace=True)
        return df

    def _replay_dbz(self, callback: Callable[[Any], None]) -> None:
        dtype = DBZ_STRUCT_MAP[self._schema]
        reader: BinaryIO = self.reader(decompress=True)
        while True:
            raw: bytes = reader.read(self.struct_size)
            record = np.frombuffer(raw, dtype=dtype)
            if record.size == 0:
                break
            callback(record[0])

    def _replay_text(self, callback: Callable[[Any], None]) -> None:
        if self._compression == Compression.NONE:
            reader: BinaryIO = self.reader(decompress=True)
            while True:
                raw: bytes = reader.readline()
                if not raw:
                    break
                record = raw.decode().rstrip("\n")
                callback(record)
        else:
            for record in self.to_list():
                callback(record)


class MemoryBento(Bento):
    """
    Provides a data container backed by in-memory buffer streaming I/O.

    Parameters
    ----------
    schema : Schema, optional
        The data record schema.
    encoding : Encoding, optional
        The data encoding.
    compression : Compression, optional
        The data compression mode.
    initial_bytes : bytes, optional
        The initial data for the memory buffer.
    """

    def __init__(
        self,
        schema: Optional[Schema] = None,
        encoding: Optional[Encoding] = None,
        compression: Optional[Compression] = None,
        initial_bytes: Optional[bytes] = None,
    ):
        self._raw = io.BytesIO(initial_bytes=initial_bytes)
        super().__init__(
            schema=schema,
            encoding=encoding,
            compression=compression,
        )

    @property
    def nbytes(self) -> int:
        """
        Return the amount of space in bytes that the data array would use in a
        contiguous representation.

        Returns
        -------
        int

        """
        return self._raw.getbuffer().nbytes

    @property
    def raw(self) -> bytes:
        return self._raw.getvalue()

    def reader(self, decompress: bool = False) -> BinaryIO:
        self._raw.seek(0)  # Ensure reader at start of stream
        if self._should_decompress(decompress):
            return zstandard.ZstdDecompressor().stream_reader(self._raw.getbuffer())
        else:
            return self._raw

    def writer(self) -> BinaryIO:
        return self._raw


class FileBento(Bento):
    """
    Provides a data container backed by DBZ file streaming I/O.

    Parameters
    ----------
    path : str
        The path to the data file.
    """

    def __init__(
        self,
        path: str,
        schema: Optional[Schema] = None,
        encoding: Optional[Encoding] = None,
        compression: Optional[Compression] = None,
    ):
        self._path = path
        super().__init__(
            schema=schema,
            encoding=encoding,
            compression=compression,
        )

    @property
    def path(self) -> str:
        """
        Return the path to the backing data file.

        Returns
        -------
        str

        """
        return self._path

    @property
    def nbytes(self) -> int:
        """
        Return the amount of space occupied by the data.

        Returns
        -------
        int

        """
        return os.path.getsize(self._path)

    @property
    def raw(self) -> bytes:
        return self.reader().read()

    def reader(self, decompress: bool = False) -> BinaryIO:
        f = open(self._path, mode="rb")
        if self._should_decompress(decompress):
            return zstandard.ZstdDecompressor().stream_reader(f)
        else:
            return f

    def writer(self) -> BinaryIO:
        return open(self._path, mode="wb")
