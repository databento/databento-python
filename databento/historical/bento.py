import io
import json
import os.path
from typing import Any, BinaryIO, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
import zstandard
from databento.common.data import BIN_COLUMNS, BIN_RECORD_MAP, DERIV_SCHEMAS
from databento.common.enums import Compression, Encoding, Schema


class BentoIOBase:
    """The abstract base class for all Bento I/O classes."""

    def __init__(
        self,
        schema: Optional[Schema],
        encoding: Optional[Encoding],
        compression: Optional[Compression],
    ):
        # Set compression
        self._compression = compression or self._infer_compression()

        # Set encoding
        self._encoding = encoding or self._infer_encoding()

        # Set schema
        self._schema = schema or self._infer_schema()
        self._struct_fmt = np.dtype(BIN_RECORD_MAP[self._schema])
        self._struct_size = self._struct_fmt.itemsize

    @property
    def nbytes(self) -> int:
        raise NotImplementedError()  # pragma: no cover

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

    def getvalue(self, decompress: bool = False) -> bytes:
        """
        Return the data from the I/O stream.

        Parameters
        ----------
        decompress : bool
            If data should be decompressed (if compressed).

        Returns
        -------
        bytes

        """
        raise NotImplementedError()  # pragma: no cover

    def to_disk(self, path: str, overwrite: bool = True) -> "BentoDiskIO":
        """
        Write the data to disk at the given path.

        Parameters
        ----------
        path : str
            The path to write to.
        overwrite : bool, default True
            If an existing file at the given path should be overwritten.

        Returns
        -------
        BentoDiskIO

        Raises
        ------
        FileExistsError
            If overwrite is False and a file already exists at the given path.

        """
        if os.path.isfile(path):
            if overwrite:
                os.remove(path)
            else:
                raise FileExistsError(f"file already exists at '{path}'")

        with open(path, mode="wb") as f:
            f.write(self.getvalue())

        return BentoDiskIO(
            path=path,
            schema=self._schema,
            encoding=self._encoding,
            compression=self._compression,
        )

    def to_list(self) -> List[Any]:
        """
        Return a list of data records.

        - BIN encoding will return a list of `np.void` mixed dtypes.
        - CSV encoding will return a list of `str`.
        - JSON encoding will return a list of `Dict[str, Any]`.

        Returns
        -------
        List[Any]

        """
        if self._encoding == Encoding.BIN:
            return self._prepare_list_bin()
        elif self._encoding == Encoding.CSV:
            return self._prepare_list_csv()
        elif self._encoding == Encoding.JSON:
            return self._prepare_list_json()
        else:  # pragma: no cover (design-time error)
            raise ValueError(f"invalid encoding, was {self._encoding.value}")

    def to_df(self) -> pd.DataFrame:
        """
        Return the data in a pd.DataFrame.

        Returns
        -------
        pd.DataFrame

        """
        if self._encoding == Encoding.BIN:
            return self._prepare_df_bin()
        elif self._encoding == Encoding.CSV:
            return self._prepare_df_csv()
        elif self._encoding == Encoding.JSON:
            return self._prepare_df_json()
        else:  # pragma: no cover (design-time error)
            raise ValueError(f"invalid encoding, was {self._encoding.value}")

    def replay(self, callback: Callable[[Any], None]) -> None:
        """
        Pass all data records sequentially to the given callback.

        Parameters
        ----------
        callback : callable
            The callback to the data handler.

        """
        if self._encoding == Encoding.BIN:
            self._replay_bin(callback)
        elif self._encoding in (Encoding.CSV, Encoding.JSON):
            self._replay_csv_or_json(callback)
        else:  # pragma: no cover (design-time error)
            raise ValueError(f"invalid encoding, was {self._encoding.value}")

    def _should_decompress(self, decompress: bool) -> bool:
        if not decompress:
            return False
        return self._compression == Compression.ZSTD

    def _infer_compression(self) -> Optional[Compression]:
        # Infer by checking for zstd header
        reader: BinaryIO = self.reader()
        header: bytes = reader.read(4)
        if header is None:
            return None
        elif header == b"(\xb5/\xfd":
            return Compression.ZSTD
        else:
            return Compression.NONE

    def _infer_encoding(self) -> Optional[Encoding]:
        # Infer by checking pattern of initial bytes
        reader: BinaryIO = self.reader(decompress=True)
        initial: bytes = reader.read(3)
        if initial is None:
            return None
        if initial == b"ts_":
            return Encoding.CSV
        elif initial == b'{"t':
            return Encoding.JSON
        else:
            return Encoding.BIN

    def _infer_schema(self) -> Optional[Schema]:
        if hasattr(self, "path"):
            path = self.path  # type: ignore  # (checked above)
        else:  # pragma: no cover (design-time error)
            raise RuntimeError("cannot infer schema without a path to read from")

        # Firstly, attempt to infer from file path
        extensions: List[str] = path.split(".")
        # Iterate schemas in reverse order as MBP-10 needs to be checked prior to MBP-1
        for schema in reversed([x for x in Schema]):
            if schema.value in extensions:
                return schema

        raise RuntimeError(
            f"unable to infer schema from `path` '{path}' "
            f"(add the schema value as an extension e.g. 'my_data.mbo', "
            f"or specify the schema explicitly)",
        )

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

    def _prepare_list_bin(self) -> List[np.void]:
        data: bytes = self.getvalue(decompress=True)
        return np.frombuffer(data, dtype=BIN_RECORD_MAP[self._schema])

    def _prepare_list_csv(self) -> List[str]:
        data: bytes = self.getvalue(decompress=True)
        return data.decode().splitlines(keepends=False)

    def _prepare_list_json(self) -> List[Dict]:
        lines: List[str] = self._prepare_list_csv()
        return list(map(json.loads, lines))

    def _prepare_df_bin(self) -> pd.DataFrame:
        df = pd.DataFrame(self.to_list())
        df.set_index(self._get_index_column(), inplace=True)
        # Cleanup dataframe
        if self._schema == Schema.MBO:
            df.drop("chan_id", axis=1, inplace=True)
            df = df.reindex(columns=BIN_COLUMNS[self._schema])
            df["flags"] = df["flags"] & 0xFF  # Apply bitmask
            df["side"] = df["side"].str.decode("utf-8")
            df["action"] = df["action"].str.decode("utf-8")
        elif self._schema in DERIV_SCHEMAS:
            df.drop(["nwords", "type", "depth"], axis=1, inplace=True)
            df = df.reindex(columns=BIN_COLUMNS[self._schema])
            df["flags"] = df["flags"] & 0xFF  # Apply bitmask
            df["side"] = df["side"].str.decode("utf-8")
            df["action"] = df["action"].str.decode("utf-8")
        else:
            df.drop(["nwords", "type"], axis=1, inplace=True)

        return df

    def _prepare_df_csv(self) -> pd.DataFrame:
        data: bytes = self.getvalue(decompress=True)
        df = pd.read_csv(io.BytesIO(data), index_col=self._get_index_column())
        return df

    def _prepare_df_json(self) -> pd.DataFrame:
        jsons: List[Dict] = self.to_list()
        df = pd.DataFrame.from_dict(jsons, orient="columns")
        df.set_index(self._get_index_column(), inplace=True)
        return df

    def _replay_bin(self, callback: Callable[[Any], None]) -> None:
        dtype = BIN_RECORD_MAP[self._schema]
        reader: BinaryIO = self.reader(decompress=True)
        while True:
            raw: bytes = reader.read(self.struct_size)
            record = np.frombuffer(raw, dtype=dtype)
            if not record:
                break
            callback(record[0])

    def _replay_csv_or_json(self, callback: Callable[[Any], None]) -> None:
        if self._compression == Compression.NONE:
            reader: BinaryIO = self.reader(decompress=True)
            while True:
                record: bytes = reader.readline()
                if not record:
                    break
                callback(record.decode().rstrip("\n"))
        else:
            for record in self.to_list():
                callback(record)


class BentoMemoryIO(BentoIOBase):
    """
    Provides a data container backed by in-memory buffer streaming I/O.

    Parameters
    ----------
    schema : Schema
        The data record schema.
    encoding : Encoding, optional
        The data encoding. If ``None`` then will be inferred.
    compression : Compression, optional
        The data compression mode. If ``None`` then will be inferred.
    initial_bytes : bytes, optional
        The initial data for the memory buffer.
    """

    def __init__(
        self,
        schema: Schema,
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

    def reader(self, decompress: bool = False) -> BinaryIO:
        self._raw.seek(0)  # Ensure reader at start of stream
        if self._should_decompress(decompress):
            return zstandard.ZstdDecompressor().stream_reader(self._raw.getbuffer())
        else:
            return self._raw

    def writer(self) -> BinaryIO:
        return self._raw

    def getvalue(self, decompress: bool = False) -> bytes:
        self._raw.seek(0)  # Ensure reader at start of stream
        if self._should_decompress(decompress):
            return self.reader(decompress=decompress).read()
        else:
            return self._raw.getvalue()


class BentoDiskIO(BentoIOBase):
    """
    Provides a data container backed by disk streaming I/O.

    Parameters
    ----------
    path : str
        The path to the data file.
    schema : Schema, optional
        The data record schema. If ``None`` then will be inferred.
    encoding : Encoding, optional
        The data encoding. If ``None`` then will be inferred.
    compression : Compression, optional
        The data compression mode. If ``None`` then will be inferred.
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
        Return the amount of space occupied by the data on disk.

        Returns
        -------
        int

        """
        return os.path.getsize(self._path)

    def reader(self, decompress: bool = False) -> BinaryIO:
        f = open(self._path, mode="rb")
        if self._should_decompress(decompress):
            return zstandard.ZstdDecompressor().stream_reader(f)
        else:
            return f

    def writer(self) -> BinaryIO:
        return open(self._path, mode="wb")

    def getvalue(self, decompress: bool = False) -> bytes:
        return self.reader(decompress=decompress).read()
