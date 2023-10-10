from __future__ import annotations

import bisect
import datetime as dt
import functools
import json
from collections import defaultdict
from collections.abc import Mapping
from io import TextIOWrapper
from typing import Any, ClassVar, NamedTuple, TextIO

import pandas as pd
from databento_dbn import UNDEF_TIMESTAMP
from databento_dbn import Metadata
from databento_dbn import SType
from databento_dbn import SymbolMappingMsg


ALL_SYMBOLS = "ALL_SYMBOLS"


class SymbolInterval(NamedTuple):
    """
    Interval inside which a symbol is defined.

    Attributes
    ----------
    start: dt.date
        The start time of the interval.
    end: dt.date
        The end time of the interval (exclusive).
    symbol: str
        The string symbol.

    """

    start: dt.date
    end: dt.date
    symbol: str


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
        self._data: dict[int, list[SymbolInterval]] = defaultdict(list)

    def clear(self) -> None:
        """
        Clear all mapping data.
        """
        self._data.clear()

    @functools.lru_cache
    def resolve(
        self,
        instrument_id: int,
        date: dt.date,
    ) -> str | None:
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

        """
        mappings = self._data[instrument_id]
        for entry in mappings:
            if entry.start <= date < entry.end:
                return entry.symbol
        return None

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

        if SType(metadata.stype_in) == SType.INSTRUMENT_ID:
            inverse = True
        elif SType(metadata.stype_out) == SType.INSTRUMENT_ID:
            inverse = False
        else:
            raise ValueError(
                "either `stype_out` or `stype_in` must be `instrument_id` to insert",
            )

        for in_symbol, entries in metadata.mappings.items():
            for entry in entries:
                try:
                    start_date = pd.Timestamp(entry["start_date"], tz="utc").date()
                    end_date = pd.Timestamp(entry["end_date"], tz="utc").date()
                except TypeError:
                    raise ValueError(
                        f"failed to parse date range from start_date={entry['start_date']} end_date={entry['end_date']}",
                    )

                if inverse:
                    try:
                        instrument_id = int(in_symbol)
                    except TypeError:
                        raise ValueError(
                            f"failed to parse `{in_symbol}` as an instrument_id",
                        )
                    symbol = entry["symbol"]
                else:
                    try:
                        instrument_id = int(entry["symbol"])
                    except TypeError:
                        raise ValueError(
                            f"failed to parse `{entry['symbol']}` as an instrument_id",
                        )
                    symbol = in_symbol

                self._insert_inverval(
                    instrument_id,
                    SymbolInterval(
                        start=start_date,
                        end=end_date,
                        symbol=symbol,
                    ),
                )

    def insert_symbol_mapping_msg(
        self,
        msg: SymbolMappingMsg,
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
            start_ts = msg.start_ts

        if msg.end_ts == UNDEF_TIMESTAMP:
            end_ts = pd.Timestamp.max
        else:
            end_ts = msg.end_ts

        # Need to decide if we care about the input or output symbol
        # For smart symbology, the output symbol is more useful
        if msg.stype_out_symbol.isdigit():
            symbol = msg.stype_in_symbol
        else:
            symbol = msg.stype_out_symbol

        self._insert_inverval(
            msg.hd.instrument_id,
            SymbolInterval(
                start=pd.Timestamp(start_ts, unit="ns", tz="utc").date(),
                end=pd.Timestamp(end_ts, unit="ns", tz="utc").date(),
                symbol=symbol,
            ),
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

        if SType(mapping["stype_in"]) == SType.INSTRUMENT_ID:
            inverse = True
        elif SType(mapping["stype_out"]) == SType.INSTRUMENT_ID:
            inverse = False
        else:
            raise ValueError(
                "either `stype_out` or `stype_in` must be `instrument_id` to insert",
            )

        if not isinstance(mapping["result"], dict):
            raise ValueError("`result` is not a valid symbology mapping")

        for in_symbol, entries in mapping["result"].items():
            for entry in entries:
                if not all(k in entry for k in self.SYMBOLOGY_RESULT_KEYS):
                    raise ValueError(
                        "`result` contents must contain `d0`, `d1`, and `s` keys",
                    )

                try:
                    start_date = pd.Timestamp(entry["d0"], tz="utc").date()
                    end_date = pd.Timestamp(entry["d1"], tz="utc").date()
                except TypeError:
                    raise ValueError(
                        f"failed to parse date range from d0={entry['d0']} d1={entry['d1']}",
                    )

                if inverse:
                    try:
                        instrument_id = int(in_symbol)
                    except TypeError:
                        raise ValueError(
                            f"failed to parse `{in_symbol}` as an instrument_id",
                        )
                    symbol = entry["s"]
                else:
                    try:
                        instrument_id = int(entry["s"])
                    except TypeError:
                        raise ValueError(
                            f"failed to parse `{entry['s']}` as an instrument_id",
                        )
                    symbol = in_symbol

                self._insert_inverval(
                    instrument_id,
                    SymbolInterval(
                        start=start_date,
                        end=end_date,
                        symbol=symbol,
                    ),
                )

    def _insert_inverval(self, instrument_id: int, interval: SymbolInterval) -> None:
        """
        Insert a SymbolInterval into the map.

        This ensures elements are inserted in order and prevents
        duplicate entries.

        """
        mappings = self._data[instrument_id]
        insert_position = bisect.bisect_left(
            self._data[instrument_id],
            interval,
        )

        if insert_position < len(mappings) and mappings[insert_position] == interval:
            return  # this mapping is already present

        mappings.insert(insert_position, interval)
