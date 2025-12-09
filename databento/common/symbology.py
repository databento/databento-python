from __future__ import annotations

import csv
import datetime as dt
import functools
import json
from collections.abc import Mapping
from io import TextIOWrapper
from os import PathLike
from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import NamedTuple
from typing import TextIO

import numpy as np
import pandas as pd
from databento_dbn import UNDEF_TIMESTAMP
from databento_dbn import Metadata
from databento_dbn import SType
from databento_dbn import SymbolMappingMsg
from databento_dbn import SymbolMappingMsgV1
from numpy.typing import NDArray

from databento.common.parsing import datetime_to_unix_nanoseconds
from databento.common.validation import validate_path


class MappingInterval(NamedTuple):
    """
    Interval inside which a symbol is defined.

    Attributes
    ----------
    start_date: dt.date
        The start time of the interval.
    end_date: dt.date
        The end time of the interval (exclusive).
    symbol: str
        The string symbol.

    """

    start_date: dt.date
    end_date: dt.date
    symbol: str


def _validate_path_pair(
    in_file: PathLike[str] | str,
    out_file: PathLike[str] | str | None,
) -> tuple[Path, Path]:
    in_file_valid = validate_path(in_file, "in_file")

    if not in_file_valid.exists():
        raise ValueError(f"{in_file_valid} does not exist")
    if not in_file_valid.is_file():
        raise ValueError(f"{in_file_valid} is not a file")

    if out_file is not None:
        out_file_valid = validate_path(out_file, "out_file")
    else:
        out_file_valid = in_file_valid.with_name(
            f"{in_file_valid.stem}_mapped{in_file_valid.suffix}",
        )

        i = 0
        while out_file_valid.exists():
            out_file_valid = in_file_valid.with_name(
                f"{in_file_valid.stem}_mapped_{i}{in_file_valid.suffix}",
            )
            i += 1

    if in_file_valid == out_file_valid:
        raise ValueError("The input file cannot be the same path as the output file.")

    return in_file_valid, out_file_valid


def map_symbols_csv(
    symbology_file: PathLike[str] | str,
    csv_file: PathLike[str] | str,
    out_file: PathLike[str] | str | None = None,
) -> Path:
    """
    Use a `symbology.json` file to map a symbols column onto an existing CSV
    file. The result is written to `out_file`.

    Parameters
    ----------
    symbology_file: PathLike[str] | str
        Path to a `symbology.json` file to use as a symbology source.
    csv_file: PathLike[str] | str
        Path to a CSV file that contains encoded DBN data; must contain
        a `ts_recv` or `ts_event` and `instrument_id` column.
    out_file: PathLike[str] | str (optional)
        Path to a file to write results to. If unspecified, `_mapped` will be
        appended to the `csv_file` name.

    Returns
    -------
    Path
        The path to the written file.

    Raises
    ------
    ValueError
        When the input or output paths are invalid.
        When the input CSV file does not contain a valid timestamp or instrument_id column.

    See Also
    --------
    map_symbols_json

    """
    instrument_map = InstrumentMap()
    with open(symbology_file) as input_symbology:
        instrument_map.insert_json(json.load(input_symbology))
    return instrument_map.map_symbols_csv(
        csv_file=csv_file,
        out_file=out_file,
    )


def map_symbols_json(
    symbology_file: PathLike[str] | str,
    json_file: PathLike[str] | str,
    out_file: PathLike[str] | str | None = None,
) -> Path:
    """
    Use a `symbology.json` file to insert a symbols key into records of an
    existing JSON file. The result is written to `out_file`.

    Parameters
    ----------
    symbology_file: PathLike[str] | str
        Path to a `symbology.json` file to use as a symbology source.
    json_file: PathLike[str] | str
        Path to a JSON file that contains encoded DBN data.
    out_file: PathLike[str] | str (optional)
        Path to a file to write results to. If unspecified, `_mapped` will be
        appended to the `json_file` name.

    Returns
    -------
    Path
        The path to the written file.

    Raises
    ------
    ValueError
        When the input or output paths are invalid.
        When the input JSON file does not contain a valid record.

    See Also
    --------
    map_symbols_csv

    """
    instrument_map = InstrumentMap()
    with open(symbology_file) as input_symbology:
        instrument_map.insert_json(json.load(input_symbology))
    return instrument_map.map_symbols_json(
        json_file=json_file,
        out_file=out_file,
    )


class InstrumentMap:
    SYMBOLOGY_RESOLVE_KEYS: ClassVar[tuple[str, ...]] = (
        "result",
        "symbols",
        "stype_in",
        "stype_out",
        "start_date",
        "end_date",
        "partial",
        "not_found",
        "message",
        "status",
    )

    SYMBOLOGY_RESULT_KEYS: ClassVar[tuple[str, ...]] = (
        "d0",
        "d1",
        "s",
    )

    def __init__(self) -> None:
        self._instrument_ids: NDArray[np.uint64]
        self._starts: NDArray[np.datetime64[dt.date]]
        self._ends: NDArray[np.datetime64[dt.date]]
        self._symbols: NDArray[np.datetime64[dt.date]]

        self.clear()  # initialize

    def clear(self) -> None:
        """
        Clear all mapping data.
        """
        self._instrument_ids = np.empty(shape=[0], dtype=np.uint64)
        self._starts = np.empty(shape=[0], dtype="datetime64[D]")
        self._ends = np.empty(shape=[0], dtype="datetime64[D]")
        self._symbols = np.empty(shape=[0], dtype="object")

    @functools.lru_cache
    def resolve(self, instrument_id: int, date: dt.date) -> str | None:
        """
        Resolve an instrument ID on a particular date to the mapped symbol, or
        `None` if there is not mapping on that date.

        Parameters
        ----------
        instrument_id : int
        date : dt.date

        Returns
        -------
        str | None

        Raises
        ------
        ValueError
            If the InstrumentMap is empty.
            If the InstrumentMap does not contain a mapping for the `instrument_id`.

        See Also
        --------
        InstrumentMap.resolve_many()

        """
        if self._instrument_ids.size == 0:
            return None

        np_date = np.datetime64(date)

        key_ids = self._instrument_ids
        key_starts = self._starts

        left = np.searchsorted(key_ids, instrument_id, side="left")
        right = np.searchsorted(key_ids, instrument_id, side="right")

        if left == right:
            return None

        pos = np.searchsorted(key_starts[left:right], np_date, side="right") - 1

        if pos < 0:
            return None

        pos += left

        if np_date < self._ends[pos]:
            return self._symbols[pos]

        return None

    def resolve_many(
        self,
        instrument_ids: NDArray[np.uint64],
        dates: NDArray[np.datetime64[dt.date]],
    ) -> NDArray[np.object_]:
        """
        Resolve several instrument ID and date pairs to their mapped symbols.
        This method is optimal for vectorizing the symbology resolution
        operation.

        Parameters
        ----------
        instrument_ids : NDArray[np.uint64]
            The collection of instrument IDs.
        dates : NDArray[np.datetime64[dt.date]]
            The collection of dates for each instrument ID.

        Returns
        -------
        NDArray[np.object_]
            The collection of resolved symbols.

        See Also
        --------
        InstrumentMap.resolve()

        """
        if instrument_ids.shape != dates.shape:
            raise ValueError("instrument_ids and dates must have the same shape")

        result = np.full(instrument_ids.shape, None, dtype=object)
        if instrument_ids.size == 0 or self._instrument_ids.size == 0:
            return result

        # Get unique instrument, date combinations and just resolve those
        query_array = np.stack([instrument_ids, dates.view("uint64")], axis=1)
        unique_queries, inverse = np.unique(query_array, axis=0, return_inverse=True)
        unique_inst = unique_queries[:, 0]
        unique_dates = unique_queries[:, 1].view("datetime64[D]")

        resolved_unique = np.full(unique_inst.shape, None, dtype=object)
        for inst_id in np.unique(unique_inst):
            # In this loop we resolve all dates for each instrument
            mask = unique_inst == inst_id
            idx = np.nonzero(mask)[0]

            # Slice the mapping arrays
            left = np.searchsorted(self._instrument_ids, inst_id, side="left")
            right = np.searchsorted(self._instrument_ids, inst_id, side="right")

            if left == right:
                continue  # no intervals for this instrument

            starts = self._starts[left:right]
            ends = self._ends[left:right]
            symbols = self._symbols[left:right]

            # Get all the dates to resolve
            dates_to_check = unique_dates[idx]

            resolved = np.searchsorted(starts, dates_to_check, side="right") - 1
            valid = (resolved >= 0) & (dates_to_check < ends[resolved])
            resolved_unique[idx[valid]] = symbols[resolved[valid]]

        # Map the resolved symbols back to the result
        result[:] = resolved_unique[inverse]

        return result

    def insert_metadata(self, metadata: Metadata) -> None:
        """
        Insert mappings from DBN Metadata.

        Parameters
        ----------
        metadata : Metadata
            The DBN metadata.

        See Also
        --------
        insert_symbol_mapping_msg
        insert_json

        """
        if len(metadata.mappings) == 0:
            # Nothing to do
            return

        instrument_ids: list[int] = []
        intervals: list[MappingInterval] = []
        for symbol_in, entries in metadata.mappings.items():
            for entry in entries:
                if not entry["symbol"]:
                    continue  # skip empty symbol mapping

                try:
                    start_date = pd.Timestamp(entry["start_date"], tz="utc").date()
                    end_date = pd.Timestamp(entry["end_date"], tz="utc").date()
                except TypeError:
                    raise ValueError(
                        f"failed to parse date range from start_date={entry['start_date']} end_date={entry['end_date']}",
                    )

                symbol, instrument_id = _resolve_mapping_tuple(
                    symbol_in=symbol_in,
                    stype_in=metadata.stype_in,
                    symbol_out=entry["symbol"],
                    stype_out=metadata.stype_out,
                )

                instrument_ids.append(instrument_id)
                intervals.append(
                    MappingInterval(
                        start_date=start_date,
                        end_date=end_date,
                        symbol=symbol,
                    ),
                )

        self._insert_intervals(instrument_ids=instrument_ids, intervals=intervals)

    def insert_symbol_mapping_msg(
        self,
        msg: SymbolMappingMsg | SymbolMappingMsgV1,
    ) -> None:
        """
        Insert mappings from a SymbolMappingMsg.

        Parameters
        ----------
        msg : SymbolMappingMsg
            The SymbolMappingMsg to insert a mapping from.

        See Also
        --------
        insert_metadata
        insert_json

        """
        if msg.start_ts == UNDEF_TIMESTAMP:
            start_ts = pd.Timestamp.min
        else:
            start_ts = msg.pretty_start_ts

        if msg.end_ts == UNDEF_TIMESTAMP:
            end_ts = pd.Timestamp.max
        else:
            end_ts = msg.pretty_end_ts

        # Need to decide if we care about the input or output symbol
        # For smart symbology, the output symbol is more useful
        if msg.stype_out_symbol.isdigit():
            symbol = msg.stype_in_symbol
        else:
            symbol = msg.stype_out_symbol

        self._insert_intervals(
            instrument_ids=[msg.instrument_id],
            intervals=[
                MappingInterval(
                    start_date=start_ts.date(),
                    end_date=end_ts.date(),
                    symbol=symbol,
                ),
            ],
        )

    def insert_json(
        self,
        json_data: str | Mapping[str, Any] | TextIO,
    ) -> None:
        """
        Insert JSON data from the `symbology.resolve` endpoint or a
        `symbology.json` file.

        Parameters
        ----------
        json_data : str | Mapping[str, Any] | TextIO
            The JSON data to insert.

        See Also
        --------
        insert_metadata
        insert_symbol_mapping_msg

        """
        if isinstance(json_data, str):
            mapping = json.loads(json_data)
        elif isinstance(
            json_data,
            (
                TextIO,
                TextIOWrapper,
            ),
        ):
            mapping = json.load(json_data)
        else:
            mapping = json_data

        if not all(k in mapping for k in self.SYMBOLOGY_RESOLVE_KEYS):
            raise ValueError("mapping must contain a complete symbology.resolve result")

        if not isinstance(mapping["result"], dict):
            raise ValueError("`result` is not a valid symbology mapping")

        stype_in = SType(mapping["stype_in"])
        stype_out = SType(mapping["stype_out"])

        instrument_ids: list[int] = []
        intervals: list[MappingInterval] = []
        for symbol_in, entries in mapping["result"].items():
            for entry in entries:
                if not all(k in entry for k in self.SYMBOLOGY_RESULT_KEYS):
                    raise ValueError(
                        "`result` contents must contain `d0`, `d1`, and `s` keys",
                    )

                if not entry["s"]:
                    continue  # skip empty symbol mapping

                try:
                    start_date = pd.Timestamp(entry["d0"], tz="utc").date()
                    end_date = pd.Timestamp(entry["d1"], tz="utc").date()
                except TypeError:
                    raise ValueError(
                        f"failed to parse date range from d0={entry['d0']} d1={entry['d1']}",
                    )

                symbol, instrument_id = _resolve_mapping_tuple(
                    symbol_in=symbol_in,
                    stype_in=stype_in,
                    symbol_out=entry["s"],
                    stype_out=stype_out,
                )

                instrument_ids.append(instrument_id)
                intervals.append(MappingInterval(start_date, end_date, symbol))

        self._insert_intervals(instrument_ids=instrument_ids, intervals=intervals)

    def map_symbols_csv(
        self,
        csv_file: PathLike[str] | str,
        out_file: PathLike[str] | str | None = None,
    ) -> Path:
        """
        Use the loaded symbology data to map a symbols column onto an existing
        CSV file. The result is written to `out_file`.

        Parameters
        ----------
        csv_file: PathLike[str] | str
            Path to a CSV file that contains encoded DBN data; must contain
            a `ts_recv` or `ts_event` and `instrument_id` column.
        out_file: PathLike[str] | str (optional)
            Path to a file to write results to. If unspecified, `_mapped` will be
            appended to the `csv_file` name.

        Returns
        -------
        Path
            The path to the written file.

        Raises
        ------
        ValueError
            When the input or output paths are invalid.
            When the input CSV file does not contain a valid timestamp or instrument_id column.

        See Also
        --------
        InstrumentMap.map_symbols_json

        """
        csv_file_valid, out_file_valid = _validate_path_pair(csv_file, out_file)

        with csv_file_valid.open() as input_:
            reader = csv.DictReader(input_)

            in_fields = reader.fieldnames

            if in_fields is None:
                raise ValueError(f"no CSV header in {csv_file}")

            if "ts_recv" in in_fields:
                ts_field = "ts_recv"
            elif "ts_event" in in_fields:
                ts_field = "ts_event"
            else:
                raise ValueError(
                    f"{csv_file} does not have a 'ts_recv' or 'ts_event' column",
                )

            if "instrument_id" not in in_fields:
                raise ValueError(f"{csv_file} does not have an 'instrument_id' column")

            out_fields = (*in_fields, "symbol")

            with out_file_valid.open("w") as output:
                writer = csv.DictWriter(
                    output,
                    fieldnames=out_fields,
                    lineterminator="\n",
                )
                writer.writeheader()

                for row in reader:
                    ts = datetime_to_unix_nanoseconds(row[ts_field])
                    date = pd.Timestamp(ts, unit="ns").date()
                    instrument_id = row["instrument_id"]
                    if instrument_id is None:
                        row["symbol"] = ""
                    else:
                        row["symbol"] = self.resolve(int(instrument_id), date)

                    writer.writerow(row)

        return out_file_valid

    def map_symbols_json(
        self,
        json_file: PathLike[str] | str,
        out_file: PathLike[str] | str | None = None,
    ) -> Path:
        """
        Use the loaded symbology data to insert a symbols key into records of
        an existing JSON file. The result is written to `out_file`.

        Parameters
        ----------
        json_file: PathLike[str] | str
            Path to a JSON file that contains encoded DBN data.
        out_file: PathLike[str] | str (optional)
            Path to a file to write results to. If unspecified, `_mapped` will be
            appended to the `json_file` name.

        Returns
        -------
        Path
            The path to the written file.

        Raises
        ------
        ValueError
            When the input or output paths are invalid.
            When the input JSON file does not contain a valid record.

        See Also
        --------
        InstrumentMap.map_symbols_csv

        """
        json_file_valid, out_file_valid = _validate_path_pair(json_file, out_file)

        with json_file_valid.open() as input_:
            with out_file_valid.open("w") as output:
                for i, record in enumerate(map(json.loads, input_)):
                    try:
                        header = record["hd"]
                        instrument_id = header["instrument_id"]
                    except KeyError:
                        raise ValueError(
                            f"{json_file}:{i} does not contain a valid JSON encoded record",
                        )

                    if "ts_recv" in record:
                        ts_field = record["ts_recv"]
                    elif "ts_event" in header:
                        ts_field = header["ts_event"]
                    else:
                        raise ValueError(
                            f"{json_file}:{i} does not have a 'ts_recv' or 'ts_event' key",
                        )

                    ts = datetime_to_unix_nanoseconds(ts_field)

                    date = pd.Timestamp(ts, unit="ns").date()
                    record["symbol"] = self.resolve(int(instrument_id), date)

                    json.dump(
                        record,
                        output,
                        separators=(",", ":"),
                    )
                    output.write("\n")

        return out_file_valid

    def build_symbol_map(self) -> dict[int, list[tuple[dt.date, dt.date, str]]]:
        """
        Build a simple symbol map. This is to interface with methods of
        databento-dbn.

        Returns
        -------
        dict[int, list[tuple[dt.date, dt.date, str]]]

        """
        symbol_map: dict[int, list[tuple[dt.date, dt.date, str]]] = {}

        if self._instrument_ids.size == 0:
            return symbol_map

        unique_ids, start_indices = np.unique(self._instrument_ids, return_index=True)
        end_indices = np.append(start_indices[1:], self._instrument_ids.size)

        for inst_id, start, end in zip(unique_ids, start_indices, end_indices):
            starts = self._starts[start:end]
            ends = self._ends[start:end]
            symbols = self._symbols[start:end]

            symbol_map[inst_id] = [
                (_to_date(s), _to_date(e), str(sym)) for s, e, sym in zip(starts, ends, symbols)
            ]

        return symbol_map

    def _insert_intervals(
        self,
        instrument_ids: list[int],
        intervals: list[MappingInterval],
    ) -> None:
        new_ids = np.array(instrument_ids, dtype=np.uint64)
        new_starts = np.array([i.start_date for i in intervals], dtype="datetime64[D]")
        new_ends = np.array([i.end_date for i in intervals], dtype="datetime64[D]")
        new_symbols = np.array([i.symbol for i in intervals], dtype=object)

        n = new_ids.size
        if not (n == new_starts.size == new_ends.size == new_symbols.size):
            raise ValueError("All input arrays must have the same length")

        if n == 0:
            return  # nothing to insert

        self._instrument_ids = np.concatenate([self._instrument_ids, new_ids])
        self._starts = np.concatenate([self._starts, new_starts])
        self._ends = np.concatenate([self._ends, new_ends])
        self._symbols = np.concatenate([self._symbols, new_symbols])

        order = np.lexsort((self._starts, self._instrument_ids))
        self._instrument_ids = self._instrument_ids[order]
        self._starts = self._starts[order]
        self._ends = self._ends[order]
        self._symbols = self._symbols[order]


def _to_date(value: np.datetime64[dt.date]) -> dt.date:
    py_dt = value.astype("O")  # yields datetime.datetime or datetime.date
    if isinstance(py_dt, dt.datetime):
        return py_dt.date()
    return py_dt


def _resolve_mapping_tuple(
    symbol_in: str | int,
    stype_in: SType,
    symbol_out: str | int,
    stype_out: SType,
) -> tuple[str, int]:
    if stype_in == SType.INSTRUMENT_ID:
        try:
            instrument_id = int(symbol_in)
        except (TypeError, ValueError):
            raise ValueError(
                f"failed to parse `{symbol_in}` as an instrument_id",
            )
        return str(symbol_out), instrument_id
    elif stype_out == SType.INSTRUMENT_ID:
        try:
            instrument_id = int(symbol_out)
        except (TypeError, ValueError):
            raise ValueError(
                f"failed to parse `{symbol_out}` as an instrument_id",
            )
        return str(symbol_in), instrument_id

    raise ValueError(
        "either `stype_out` or `stype_in` must be `instrument_id` to insert",
    )
