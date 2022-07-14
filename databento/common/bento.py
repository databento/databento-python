import io
import json
import os.path
from typing import Any, BinaryIO, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import zstandard
from databento.common.data import DBZ_COLUMNS, DBZ_STRUCT_MAP, DERIV_SCHEMAS
from databento.common.enums import Compression, Encoding, Schema, SType


class Bento:
    """The abstract base class for all Bento I/O classes."""

    def __init__(self):
        self._metadata: Dict[str, Any] = {}

        self._dataset: Optional[str] = None
        self._schema: Optional[Schema] = None
        self._symbols: Optional[List[str]] = None
        self._stype_in: Optional[SType] = None
        self._stype_out: Optional[SType] = None
        self._start: Optional[pd.Timestamp] = None
        self._end: Optional[pd.Timestamp] = None
        self._encoding: Optional[Encoding] = None
        self._compression: Optional[Compression] = None
        self._shape: Optional[Tuple[int, int]] = None
        self._rows: Optional[int] = None
        self._cols: Optional[int] = None

        self._dtype: Optional[np.dtype] = None

    def _check_metadata(self) -> None:
        if not self._metadata:
            raise ValueError("metadata has not been correctly initialized")

    @property
    def dataset(self) -> str:
        """
        Return the dataset ID.

        Returns
        -------
        str

        """
        if self._dataset is None:
            self._check_metadata()
            self._dataset = self._metadata["dataset"]

        return self._dataset

    @property
    def schema(self) -> Schema:
        """
        Return the data record schema.

        Returns
        -------
        Schema

        """
        if self._schema is None:
            self._check_metadata()
            self._schema = Schema(self._metadata["schema"])

        return self._schema

    @property
    def symbols(self) -> List[str]:
        """
        Return the query symbols for the data.

        Returns
        -------
        List[str]

        """
        if self._symbols is None:
            self._check_metadata()
            self._symbols = self._metadata["symbols"]

        return self._symbols

    @property
    def stype_in(self) -> SType:
        """
        Return the query input symbol type for the data.

        Returns
        -------
        SType

        """
        if self._stype_in is None:
            self._check_metadata()
            self._stype_in = SType(self._metadata["stype_in"])

        return self._stype_in

    @property
    def stype_out(self) -> SType:
        """
        Return the query output symbol type for the data.

        Returns
        -------
        SType

        """
        if self._stype_out is None:
            self._check_metadata()
            self._stype_out = SType(self._metadata["stype_out"])

        return self._stype_out

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
        if self._start is None:
            self._check_metadata()
            self._start = pd.Timestamp(self._metadata["start"], tz="UTC")

        return self._start

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
        if self._end is None:
            self._check_metadata()
            self._end = pd.Timestamp(self._metadata["end"], tz="UTC")

        return self._end

    @property
    def encoding(self) -> Encoding:
        """
        Return the encoding for the data.

        Returns
        -------
        Encoding

        """
        if self._encoding is None:
            self._check_metadata()
            self._encoding = Encoding(self._metadata["encoding"])

        return self._encoding

    @property
    def compression(self) -> Compression:
        """
        Return the compression mode for the data.

        Returns
        -------
        Compression

        """
        if self._compression is None:
            self._check_metadata()
            self._compression = Compression(self._metadata["compression"])

        return self._compression

    @property
    def shape(self) -> Tuple[int, int]:
        """
        Return the shape of the data.

        Returns
        -------
        Tuple[int, int]
            The rows and columns.

        """
        if self._shape is None:
            self._check_metadata()
            self._shape = (self._metadata["nrows"], self._metadata["ncols"])

        return self._shape

    @property
    def dtype(self) -> np.dtype:
        """
        Return the binary struct format for the schema.

        Returns
        -------
        np.dtype

        """
        if self._dtype is None:
            self._check_metadata()
            self._dtype = np.dtype(DBZ_STRUCT_MAP[self.schema])

        return self._dtype

    @property
    def struct_size(self) -> int:
        """
        Return the schemas binary struct size in bytes.

        Returns
        -------
        int

        """
        return self.dtype.itemsize

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

    @property
    def mappings(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        Return the symbology mappings for the data.

        Returns
        -------
        List[Dict[str, List[Dict[str, str]]]]

        """
        self._check_metadata()

        return self._metadata["mappings"]

    @property
    def symbology(self) -> Dict[str, Any]:
        """
        Return the symbology resolution response information for the query.

        This JSON representable object should exactly match a `symbology.resolve`
        request using the same query parameters.

        Returns
        -------
        Dict[str, Any]

        """
        self._check_metadata()

        status = self._metadata["status"]
        if status == 1:
            message = "Partially resolved"
        elif status == 2:
            message = "Not found"
        else:
            message = "OK"

        response: Dict[str, Any] = {
            "result": self.mappings,
            "symbols": self.symbols,
            "stype_in": self.stype_in.value,
            "stype_out": self.stype_out.value,
            "from_date": str(self.start.date()),
            "to_date": str(self.end.date()),
            "partial": self._metadata["partial"],
            "not_found": self._metadata["not_found"],
            "message": message,
            "status": status,
        }

        return response

    @property
    def definitions(self) -> List[Dict[str, Any]]:
        """
        Return the instrument 'mini-definitions' for the data.

        A 'mini-definition' is a subset of the full `definition` schema,
        which provides additional useful metadata for working with the data.

        Returns
        -------
        List[Dict[str, Any]]

        """
        self._check_metadata()

        return self._metadata["definitions"]

    def instrument(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Return the 'mini-definition' for the given native symbol.

        Parameters
        ----------
        symbol : str
            The native symbol for the instrument.

        Returns
        -------
        List[Dict[str, Any]]

        """
        self._check_metadata()

        return [d for d in self._metadata["definitions"] if d["symbol"] == symbol]

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
        if self.encoding == Encoding.DBZ:
            return self._prepare_list_dbz()
        elif self.encoding == Encoding.CSV:
            return self._prepare_list_csv()
        elif self.encoding == Encoding.JSON:
            return self._prepare_list_json()
        else:  # pragma: no cover (design-time error)
            raise ValueError(f"invalid encoding, was {self.encoding.value}")

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
        if self.encoding == Encoding.DBZ:
            df = self._prepare_df_dbz()
        elif self.encoding == Encoding.CSV:
            df = self._prepare_df_csv()
        elif self.encoding == Encoding.JSON:
            df = self._prepare_df_json()
        else:  # pragma: no cover (design-time error)
            raise ValueError(f"invalid encoding, was {self.encoding.value}")

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
        if self.encoding == Encoding.DBZ:
            self._replay_dbz(callback)
        elif self.encoding in (Encoding.CSV, Encoding.JSON):
            self._replay_text(callback)
        else:  # pragma: no cover (design-time error)
            raise ValueError(f"invalid encoding, was {self.encoding.value}")

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
            f.write(self.reader().read())

        bento = FileBento(path=path)
        bento.set_metadata(self._metadata)

        return bento

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        """
        Set metadata from a JSON object.

        Parameters
        ----------
        metadata : Dict[str, Any]
            The metadata to set.

        """
        self._metadata = metadata

    def _should_decompress(self, decompress: bool) -> bool:
        if not decompress:
            return False
        return self.compression == Compression.ZSTD

    def _get_index_column(self) -> str:
        return (
            "ts_recv"
            if self.schema
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
        return np.frombuffer(data, dtype=DBZ_STRUCT_MAP[self.schema])

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
        if self.schema == Schema.MBO:
            df.drop("chan_id", axis=1, inplace=True)
            df = df.reindex(columns=DBZ_COLUMNS[self.schema])
            df["flags"] = df["flags"] & 0xFF  # Apply bitmask
            df["side"] = df["side"].str.decode("utf-8")
            df["action"] = df["action"].str.decode("utf-8")
        elif self.schema in DERIV_SCHEMAS:
            df.drop(["nwords", "type", "depth"], axis=1, inplace=True)
            df = df.reindex(columns=DBZ_COLUMNS[self.schema])
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
        dtype = DBZ_STRUCT_MAP[self.schema]
        reader: BinaryIO = self.reader(decompress=True)
        while True:
            raw: bytes = reader.read(self.struct_size)
            record = np.frombuffer(raw, dtype=dtype)
            if record.size == 0:
                break
            callback(record[0])

    def _replay_text(self, callback: Callable[[Any], None]) -> None:
        if self.compression == Compression.NONE:
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
    initial_bytes : bytes, optional
        The initial data for the memory buffer.
    """

    def __init__(self, initial_bytes: Optional[bytes] = None):
        super().__init__()

        self._raw = io.BytesIO(initial_bytes=initial_bytes or b"")

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

    def __init__(self, path: str):
        super().__init__()

        self._path = path

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
