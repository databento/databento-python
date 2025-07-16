from __future__ import annotations

from enum import Enum
from enum import unique

from databento.common.enums import StringyMixin
from databento.common.enums import coercible


# ruff: noqa: C901


@unique
@coercible
class Venue(StringyMixin, str, Enum):
    """
    A trading execution venue.

    GLBX
        CME Globex.
    XNAS
        Nasdaq - All Markets.
    XBOS
        Nasdaq OMX BX.
    XPSX
        Nasdaq OMX PSX.
    BATS
        Cboe BZX U.S. Equities Exchange.
    BATY
        Cboe BYX U.S. Equities Exchange.
    EDGA
        Cboe EDGA U.S. Equities Exchange.
    EDGX
        Cboe EDGX U.S. Equities Exchange.
    XNYS
        New York Stock Exchange, Inc..
    XCIS
        NYSE National, Inc..
    XASE
        NYSE MKT LLC.
    ARCX
        NYSE Arca.
    XCHI
        NYSE Texas, Inc..
    IEXG
        Investors Exchange.
    FINN
        FINRA/Nasdaq TRF Carteret.
    FINC
        FINRA/Nasdaq TRF Chicago.
    FINY
        FINRA/NYSE TRF.
    MEMX
        MEMX LLC Equities.
    EPRL
        MIAX Pearl Equities.
    AMXO
        NYSE American Options.
    XBOX
        BOX Options.
    XCBO
        Cboe Options.
    EMLD
        MIAX Emerald.
    EDGO
        Cboe EDGX Options.
    GMNI
        Nasdaq GEMX.
    XISX
        Nasdaq ISE.
    MCRY
        Nasdaq MRX.
    XMIO
        MIAX Options.
    ARCO
        NYSE Arca Options.
    OPRA
        Options Price Reporting Authority.
    MPRL
        MIAX Pearl.
    XNDQ
        Nasdaq Options.
    XBXO
        Nasdaq BX Options.
    C2OX
        Cboe C2 Options.
    XPHL
        Nasdaq PHLX.
    BATO
        Cboe BZX Options.
    MXOP
        MEMX Options.
    IFEU
        ICE Europe Commodities.
    NDEX
        ICE Endex.
    DBEQ
        Databento US Equities - Consolidated.
    SPHR
        MIAX Sapphire.
    LTSE
        Long-Term Stock Exchange, Inc..
    XOFF
        Off-Exchange Transactions - Listed Instruments.
    ASPN
        IntelligentCross ASPEN Intelligent Bid/Offer.
    ASMT
        IntelligentCross ASPEN Maker/Taker.
    ASPI
        IntelligentCross ASPEN Inverted.
    EQUS
        Databento US Equities - Consolidated.
    IFUS
        ICE Futures US.
    IFLL
        ICE Europe Financials.
    XEUR
        Eurex Exchange.
    XEEE
        European Energy Exchange.

    """

    GLBX = "GLBX"
    XNAS = "XNAS"
    XBOS = "XBOS"
    XPSX = "XPSX"
    BATS = "BATS"
    BATY = "BATY"
    EDGA = "EDGA"
    EDGX = "EDGX"
    XNYS = "XNYS"
    XCIS = "XCIS"
    XASE = "XASE"
    ARCX = "ARCX"
    XCHI = "XCHI"
    IEXG = "IEXG"
    FINN = "FINN"
    FINC = "FINC"
    FINY = "FINY"
    MEMX = "MEMX"
    EPRL = "EPRL"
    AMXO = "AMXO"
    XBOX = "XBOX"
    XCBO = "XCBO"
    EMLD = "EMLD"
    EDGO = "EDGO"
    GMNI = "GMNI"
    XISX = "XISX"
    MCRY = "MCRY"
    XMIO = "XMIO"
    ARCO = "ARCO"
    OPRA = "OPRA"
    MPRL = "MPRL"
    XNDQ = "XNDQ"
    XBXO = "XBXO"
    C2OX = "C2OX"
    XPHL = "XPHL"
    BATO = "BATO"
    MXOP = "MXOP"
    IFEU = "IFEU"
    NDEX = "NDEX"
    DBEQ = "DBEQ"
    SPHR = "SPHR"
    LTSE = "LTSE"
    XOFF = "XOFF"
    ASPN = "ASPN"
    ASMT = "ASMT"
    ASPI = "ASPI"
    EQUS = "EQUS"
    IFUS = "IFUS"
    IFLL = "IFLL"
    XEUR = "XEUR"
    XEEE = "XEEE"

    @classmethod
    def from_int(cls, value: int) -> Venue:
        """
        Get a Venue from the given integer value.
        """
        if value == 1:
            return Venue.GLBX
        if value == 2:
            return Venue.XNAS
        if value == 3:
            return Venue.XBOS
        if value == 4:
            return Venue.XPSX
        if value == 5:
            return Venue.BATS
        if value == 6:
            return Venue.BATY
        if value == 7:
            return Venue.EDGA
        if value == 8:
            return Venue.EDGX
        if value == 9:
            return Venue.XNYS
        if value == 10:
            return Venue.XCIS
        if value == 11:
            return Venue.XASE
        if value == 12:
            return Venue.ARCX
        if value == 13:
            return Venue.XCHI
        if value == 14:
            return Venue.IEXG
        if value == 15:
            return Venue.FINN
        if value == 16:
            return Venue.FINC
        if value == 17:
            return Venue.FINY
        if value == 18:
            return Venue.MEMX
        if value == 19:
            return Venue.EPRL
        if value == 20:
            return Venue.AMXO
        if value == 21:
            return Venue.XBOX
        if value == 22:
            return Venue.XCBO
        if value == 23:
            return Venue.EMLD
        if value == 24:
            return Venue.EDGO
        if value == 25:
            return Venue.GMNI
        if value == 26:
            return Venue.XISX
        if value == 27:
            return Venue.MCRY
        if value == 28:
            return Venue.XMIO
        if value == 29:
            return Venue.ARCO
        if value == 30:
            return Venue.OPRA
        if value == 31:
            return Venue.MPRL
        if value == 32:
            return Venue.XNDQ
        if value == 33:
            return Venue.XBXO
        if value == 34:
            return Venue.C2OX
        if value == 35:
            return Venue.XPHL
        if value == 36:
            return Venue.BATO
        if value == 37:
            return Venue.MXOP
        if value == 38:
            return Venue.IFEU
        if value == 39:
            return Venue.NDEX
        if value == 40:
            return Venue.DBEQ
        if value == 41:
            return Venue.SPHR
        if value == 42:
            return Venue.LTSE
        if value == 43:
            return Venue.XOFF
        if value == 44:
            return Venue.ASPN
        if value == 45:
            return Venue.ASMT
        if value == 46:
            return Venue.ASPI
        if value == 47:
            return Venue.EQUS
        if value == 48:
            return Venue.IFUS
        if value == 49:
            return Venue.IFLL
        if value == 50:
            return Venue.XEUR
        if value == 51:
            return Venue.XEEE
        raise ValueError(f"Integer value {value} does not correspond with any Venue variant")

    def to_int(self) -> int:
        """
        Get a Venue's integer value.
        """
        if self == Venue.GLBX:
            return 1
        if self == Venue.XNAS:
            return 2
        if self == Venue.XBOS:
            return 3
        if self == Venue.XPSX:
            return 4
        if self == Venue.BATS:
            return 5
        if self == Venue.BATY:
            return 6
        if self == Venue.EDGA:
            return 7
        if self == Venue.EDGX:
            return 8
        if self == Venue.XNYS:
            return 9
        if self == Venue.XCIS:
            return 10
        if self == Venue.XASE:
            return 11
        if self == Venue.ARCX:
            return 12
        if self == Venue.XCHI:
            return 13
        if self == Venue.IEXG:
            return 14
        if self == Venue.FINN:
            return 15
        if self == Venue.FINC:
            return 16
        if self == Venue.FINY:
            return 17
        if self == Venue.MEMX:
            return 18
        if self == Venue.EPRL:
            return 19
        if self == Venue.AMXO:
            return 20
        if self == Venue.XBOX:
            return 21
        if self == Venue.XCBO:
            return 22
        if self == Venue.EMLD:
            return 23
        if self == Venue.EDGO:
            return 24
        if self == Venue.GMNI:
            return 25
        if self == Venue.XISX:
            return 26
        if self == Venue.MCRY:
            return 27
        if self == Venue.XMIO:
            return 28
        if self == Venue.ARCO:
            return 29
        if self == Venue.OPRA:
            return 30
        if self == Venue.MPRL:
            return 31
        if self == Venue.XNDQ:
            return 32
        if self == Venue.XBXO:
            return 33
        if self == Venue.C2OX:
            return 34
        if self == Venue.XPHL:
            return 35
        if self == Venue.BATO:
            return 36
        if self == Venue.MXOP:
            return 37
        if self == Venue.IFEU:
            return 38
        if self == Venue.NDEX:
            return 39
        if self == Venue.DBEQ:
            return 40
        if self == Venue.SPHR:
            return 41
        if self == Venue.LTSE:
            return 42
        if self == Venue.XOFF:
            return 43
        if self == Venue.ASPN:
            return 44
        if self == Venue.ASMT:
            return 45
        if self == Venue.ASPI:
            return 46
        if self == Venue.EQUS:
            return 47
        if self == Venue.IFUS:
            return 48
        if self == Venue.IFLL:
            return 49
        if self == Venue.XEUR:
            return 50
        if self == Venue.XEEE:
            return 51
        raise ValueError("Invalid Venue")

    @property
    def description(self) -> str:
        """
        Get a Venue's description.
        """
        if self == Venue.GLBX:
            return "CME Globex"
        if self == Venue.XNAS:
            return "Nasdaq - All Markets"
        if self == Venue.XBOS:
            return "Nasdaq OMX BX"
        if self == Venue.XPSX:
            return "Nasdaq OMX PSX"
        if self == Venue.BATS:
            return "Cboe BZX U.S. Equities Exchange"
        if self == Venue.BATY:
            return "Cboe BYX U.S. Equities Exchange"
        if self == Venue.EDGA:
            return "Cboe EDGA U.S. Equities Exchange"
        if self == Venue.EDGX:
            return "Cboe EDGX U.S. Equities Exchange"
        if self == Venue.XNYS:
            return "New York Stock Exchange, Inc."
        if self == Venue.XCIS:
            return "NYSE National, Inc."
        if self == Venue.XASE:
            return "NYSE MKT LLC"
        if self == Venue.ARCX:
            return "NYSE Arca"
        if self == Venue.XCHI:
            return "NYSE Texas, Inc."
        if self == Venue.IEXG:
            return "Investors Exchange"
        if self == Venue.FINN:
            return "FINRA/Nasdaq TRF Carteret"
        if self == Venue.FINC:
            return "FINRA/Nasdaq TRF Chicago"
        if self == Venue.FINY:
            return "FINRA/NYSE TRF"
        if self == Venue.MEMX:
            return "MEMX LLC Equities"
        if self == Venue.EPRL:
            return "MIAX Pearl Equities"
        if self == Venue.AMXO:
            return "NYSE American Options"
        if self == Venue.XBOX:
            return "BOX Options"
        if self == Venue.XCBO:
            return "Cboe Options"
        if self == Venue.EMLD:
            return "MIAX Emerald"
        if self == Venue.EDGO:
            return "Cboe EDGX Options"
        if self == Venue.GMNI:
            return "Nasdaq GEMX"
        if self == Venue.XISX:
            return "Nasdaq ISE"
        if self == Venue.MCRY:
            return "Nasdaq MRX"
        if self == Venue.XMIO:
            return "MIAX Options"
        if self == Venue.ARCO:
            return "NYSE Arca Options"
        if self == Venue.OPRA:
            return "Options Price Reporting Authority"
        if self == Venue.MPRL:
            return "MIAX Pearl"
        if self == Venue.XNDQ:
            return "Nasdaq Options"
        if self == Venue.XBXO:
            return "Nasdaq BX Options"
        if self == Venue.C2OX:
            return "Cboe C2 Options"
        if self == Venue.XPHL:
            return "Nasdaq PHLX"
        if self == Venue.BATO:
            return "Cboe BZX Options"
        if self == Venue.MXOP:
            return "MEMX Options"
        if self == Venue.IFEU:
            return "ICE Europe Commodities"
        if self == Venue.NDEX:
            return "ICE Endex"
        if self == Venue.DBEQ:
            return "Databento US Equities - Consolidated"
        if self == Venue.SPHR:
            return "MIAX Sapphire"
        if self == Venue.LTSE:
            return "Long-Term Stock Exchange, Inc."
        if self == Venue.XOFF:
            return "Off-Exchange Transactions - Listed Instruments"
        if self == Venue.ASPN:
            return "IntelligentCross ASPEN Intelligent Bid/Offer"
        if self == Venue.ASMT:
            return "IntelligentCross ASPEN Maker/Taker"
        if self == Venue.ASPI:
            return "IntelligentCross ASPEN Inverted"
        if self == Venue.EQUS:
            return "Databento US Equities - Consolidated"
        if self == Venue.IFUS:
            return "ICE Futures US"
        if self == Venue.IFLL:
            return "ICE Europe Financials"
        if self == Venue.XEUR:
            return "Eurex Exchange"
        if self == Venue.XEEE:
            return "European Energy Exchange"
        raise ValueError("Unexpected Venue value")


@unique
@coercible
class Dataset(StringyMixin, str, Enum):
    """
    A source of data.

    GLBX_MDP3
        CME MDP 3.0 Market Data.
    XNAS_ITCH
        Nasdaq TotalView-ITCH.
    XBOS_ITCH
        Nasdaq BX TotalView-ITCH.
    XPSX_ITCH
        Nasdaq PSX TotalView-ITCH.
    BATS_PITCH
        Cboe BZX Depth.
    BATY_PITCH
        Cboe BYX Depth.
    EDGA_PITCH
        Cboe EDGA Depth.
    EDGX_PITCH
        Cboe EDGX Depth.
    XNYS_PILLAR
        NYSE Integrated.
    XCIS_PILLAR
        NYSE National Integrated.
    XASE_PILLAR
        NYSE American Integrated.
    XCHI_PILLAR
        NYSE Texas Integrated.
    XCIS_BBO
        NYSE National BBO.
    XCIS_TRADES
        NYSE National Trades.
    MEMX_MEMOIR
        MEMX Memoir Depth.
    EPRL_DOM
        MIAX Pearl Depth.
    FINN_NLS
        FINRA/Nasdaq TRF (DEPRECATED).
    FINY_TRADES
        FINRA/NYSE TRF (DEPRECATED).
    OPRA_PILLAR
        OPRA Binary.
    DBEQ_BASIC
        Databento US Equities Basic.
    ARCX_PILLAR
        NYSE Arca Integrated.
    IEXG_TOPS
        IEX TOPS.
    EQUS_PLUS
        Databento US Equities Plus.
    XNYS_BBO
        NYSE BBO.
    XNYS_TRADES
        NYSE Trades.
    XNAS_QBBO
        Nasdaq QBBO.
    XNAS_NLS
        Nasdaq NLS.
    IFEU_IMPACT
        ICE Europe Commodities iMpact.
    NDEX_IMPACT
        ICE Endex iMpact.
    EQUS_ALL
        Databento US Equities (All Feeds).
    XNAS_BASIC
        Nasdaq Basic (NLS and QBBO).
    EQUS_SUMMARY
        Databento US Equities Summary.
    XCIS_TRADESBBO
        NYSE National Trades and BBO.
    XNYS_TRADESBBO
        NYSE Trades and BBO.
    EQUS_MINI
        Databento US Equities Mini.
    IFUS_IMPACT
        ICE Futures US iMpact.
    IFLL_IMPACT
        ICE Europe Financials iMpact.
    XEUR_EOBI
        Eurex EOBI.
    XEEE_EOBI
        European Energy Exchange EOBI.

    """

    GLBX_MDP3 = "GLBX.MDP3"
    XNAS_ITCH = "XNAS.ITCH"
    XBOS_ITCH = "XBOS.ITCH"
    XPSX_ITCH = "XPSX.ITCH"
    BATS_PITCH = "BATS.PITCH"
    BATY_PITCH = "BATY.PITCH"
    EDGA_PITCH = "EDGA.PITCH"
    EDGX_PITCH = "EDGX.PITCH"
    XNYS_PILLAR = "XNYS.PILLAR"
    XCIS_PILLAR = "XCIS.PILLAR"
    XASE_PILLAR = "XASE.PILLAR"
    XCHI_PILLAR = "XCHI.PILLAR"
    XCIS_BBO = "XCIS.BBO"
    XCIS_TRADES = "XCIS.TRADES"
    MEMX_MEMOIR = "MEMX.MEMOIR"
    EPRL_DOM = "EPRL.DOM"
    FINN_NLS = "FINN.NLS"
    FINY_TRADES = "FINY.TRADES"
    OPRA_PILLAR = "OPRA.PILLAR"
    DBEQ_BASIC = "DBEQ.BASIC"
    ARCX_PILLAR = "ARCX.PILLAR"
    IEXG_TOPS = "IEXG.TOPS"
    EQUS_PLUS = "EQUS.PLUS"
    XNYS_BBO = "XNYS.BBO"
    XNYS_TRADES = "XNYS.TRADES"
    XNAS_QBBO = "XNAS.QBBO"
    XNAS_NLS = "XNAS.NLS"
    IFEU_IMPACT = "IFEU.IMPACT"
    NDEX_IMPACT = "NDEX.IMPACT"
    EQUS_ALL = "EQUS.ALL"
    XNAS_BASIC = "XNAS.BASIC"
    EQUS_SUMMARY = "EQUS.SUMMARY"
    XCIS_TRADESBBO = "XCIS.TRADESBBO"
    XNYS_TRADESBBO = "XNYS.TRADESBBO"
    EQUS_MINI = "EQUS.MINI"
    IFUS_IMPACT = "IFUS.IMPACT"
    IFLL_IMPACT = "IFLL.IMPACT"
    XEUR_EOBI = "XEUR.EOBI"
    XEEE_EOBI = "XEEE.EOBI"

    @classmethod
    def from_int(cls, value: int) -> Dataset:
        """
        Get a Dataset from the given integer value.
        """
        if value == 1:
            return Dataset.GLBX_MDP3
        if value == 2:
            return Dataset.XNAS_ITCH
        if value == 3:
            return Dataset.XBOS_ITCH
        if value == 4:
            return Dataset.XPSX_ITCH
        if value == 5:
            return Dataset.BATS_PITCH
        if value == 6:
            return Dataset.BATY_PITCH
        if value == 7:
            return Dataset.EDGA_PITCH
        if value == 8:
            return Dataset.EDGX_PITCH
        if value == 9:
            return Dataset.XNYS_PILLAR
        if value == 10:
            return Dataset.XCIS_PILLAR
        if value == 11:
            return Dataset.XASE_PILLAR
        if value == 12:
            return Dataset.XCHI_PILLAR
        if value == 13:
            return Dataset.XCIS_BBO
        if value == 14:
            return Dataset.XCIS_TRADES
        if value == 15:
            return Dataset.MEMX_MEMOIR
        if value == 16:
            return Dataset.EPRL_DOM
        if value == 17:
            return Dataset.FINN_NLS
        if value == 18:
            return Dataset.FINY_TRADES
        if value == 19:
            return Dataset.OPRA_PILLAR
        if value == 20:
            return Dataset.DBEQ_BASIC
        if value == 21:
            return Dataset.ARCX_PILLAR
        if value == 22:
            return Dataset.IEXG_TOPS
        if value == 23:
            return Dataset.EQUS_PLUS
        if value == 24:
            return Dataset.XNYS_BBO
        if value == 25:
            return Dataset.XNYS_TRADES
        if value == 26:
            return Dataset.XNAS_QBBO
        if value == 27:
            return Dataset.XNAS_NLS
        if value == 28:
            return Dataset.IFEU_IMPACT
        if value == 29:
            return Dataset.NDEX_IMPACT
        if value == 30:
            return Dataset.EQUS_ALL
        if value == 31:
            return Dataset.XNAS_BASIC
        if value == 32:
            return Dataset.EQUS_SUMMARY
        if value == 33:
            return Dataset.XCIS_TRADESBBO
        if value == 34:
            return Dataset.XNYS_TRADESBBO
        if value == 35:
            return Dataset.EQUS_MINI
        if value == 36:
            return Dataset.IFUS_IMPACT
        if value == 37:
            return Dataset.IFLL_IMPACT
        if value == 38:
            return Dataset.XEUR_EOBI
        if value == 39:
            return Dataset.XEEE_EOBI
        raise ValueError(f"Integer value {value} does not correspond with any Dataset variant")

    def to_int(self) -> int:
        """
        Get a Dataset's integer value.
        """
        if self == Dataset.GLBX_MDP3:
            return 1
        if self == Dataset.XNAS_ITCH:
            return 2
        if self == Dataset.XBOS_ITCH:
            return 3
        if self == Dataset.XPSX_ITCH:
            return 4
        if self == Dataset.BATS_PITCH:
            return 5
        if self == Dataset.BATY_PITCH:
            return 6
        if self == Dataset.EDGA_PITCH:
            return 7
        if self == Dataset.EDGX_PITCH:
            return 8
        if self == Dataset.XNYS_PILLAR:
            return 9
        if self == Dataset.XCIS_PILLAR:
            return 10
        if self == Dataset.XASE_PILLAR:
            return 11
        if self == Dataset.XCHI_PILLAR:
            return 12
        if self == Dataset.XCIS_BBO:
            return 13
        if self == Dataset.XCIS_TRADES:
            return 14
        if self == Dataset.MEMX_MEMOIR:
            return 15
        if self == Dataset.EPRL_DOM:
            return 16
        if self == Dataset.FINN_NLS:
            return 17
        if self == Dataset.FINY_TRADES:
            return 18
        if self == Dataset.OPRA_PILLAR:
            return 19
        if self == Dataset.DBEQ_BASIC:
            return 20
        if self == Dataset.ARCX_PILLAR:
            return 21
        if self == Dataset.IEXG_TOPS:
            return 22
        if self == Dataset.EQUS_PLUS:
            return 23
        if self == Dataset.XNYS_BBO:
            return 24
        if self == Dataset.XNYS_TRADES:
            return 25
        if self == Dataset.XNAS_QBBO:
            return 26
        if self == Dataset.XNAS_NLS:
            return 27
        if self == Dataset.IFEU_IMPACT:
            return 28
        if self == Dataset.NDEX_IMPACT:
            return 29
        if self == Dataset.EQUS_ALL:
            return 30
        if self == Dataset.XNAS_BASIC:
            return 31
        if self == Dataset.EQUS_SUMMARY:
            return 32
        if self == Dataset.XCIS_TRADESBBO:
            return 33
        if self == Dataset.XNYS_TRADESBBO:
            return 34
        if self == Dataset.EQUS_MINI:
            return 35
        if self == Dataset.IFUS_IMPACT:
            return 36
        if self == Dataset.IFLL_IMPACT:
            return 37
        if self == Dataset.XEUR_EOBI:
            return 38
        if self == Dataset.XEEE_EOBI:
            return 39
        raise ValueError("Invalid Dataset")

    @property
    def description(self) -> str:
        """
        Get a Dataset's description.
        """
        if self == Dataset.GLBX_MDP3:
            return "CME MDP 3.0 Market Data"
        if self == Dataset.XNAS_ITCH:
            return "Nasdaq TotalView-ITCH"
        if self == Dataset.XBOS_ITCH:
            return "Nasdaq BX TotalView-ITCH"
        if self == Dataset.XPSX_ITCH:
            return "Nasdaq PSX TotalView-ITCH"
        if self == Dataset.BATS_PITCH:
            return "Cboe BZX Depth"
        if self == Dataset.BATY_PITCH:
            return "Cboe BYX Depth"
        if self == Dataset.EDGA_PITCH:
            return "Cboe EDGA Depth"
        if self == Dataset.EDGX_PITCH:
            return "Cboe EDGX Depth"
        if self == Dataset.XNYS_PILLAR:
            return "NYSE Integrated"
        if self == Dataset.XCIS_PILLAR:
            return "NYSE National Integrated"
        if self == Dataset.XASE_PILLAR:
            return "NYSE American Integrated"
        if self == Dataset.XCHI_PILLAR:
            return "NYSE Texas Integrated"
        if self == Dataset.XCIS_BBO:
            return "NYSE National BBO"
        if self == Dataset.XCIS_TRADES:
            return "NYSE National Trades"
        if self == Dataset.MEMX_MEMOIR:
            return "MEMX Memoir Depth"
        if self == Dataset.EPRL_DOM:
            return "MIAX Pearl Depth"
        if self == Dataset.FINN_NLS:
            return "FINRA/Nasdaq TRF (DEPRECATED)"
        if self == Dataset.FINY_TRADES:
            return "FINRA/NYSE TRF (DEPRECATED)"
        if self == Dataset.OPRA_PILLAR:
            return "OPRA Binary"
        if self == Dataset.DBEQ_BASIC:
            return "Databento US Equities Basic"
        if self == Dataset.ARCX_PILLAR:
            return "NYSE Arca Integrated"
        if self == Dataset.IEXG_TOPS:
            return "IEX TOPS"
        if self == Dataset.EQUS_PLUS:
            return "Databento US Equities Plus"
        if self == Dataset.XNYS_BBO:
            return "NYSE BBO"
        if self == Dataset.XNYS_TRADES:
            return "NYSE Trades"
        if self == Dataset.XNAS_QBBO:
            return "Nasdaq QBBO"
        if self == Dataset.XNAS_NLS:
            return "Nasdaq NLS"
        if self == Dataset.IFEU_IMPACT:
            return "ICE Europe Commodities iMpact"
        if self == Dataset.NDEX_IMPACT:
            return "ICE Endex iMpact"
        if self == Dataset.EQUS_ALL:
            return "Databento US Equities (All Feeds)"
        if self == Dataset.XNAS_BASIC:
            return "Nasdaq Basic (NLS and QBBO)"
        if self == Dataset.EQUS_SUMMARY:
            return "Databento US Equities Summary"
        if self == Dataset.XCIS_TRADESBBO:
            return "NYSE National Trades and BBO"
        if self == Dataset.XNYS_TRADESBBO:
            return "NYSE Trades and BBO"
        if self == Dataset.EQUS_MINI:
            return "Databento US Equities Mini"
        if self == Dataset.IFUS_IMPACT:
            return "ICE Futures US iMpact"
        if self == Dataset.IFLL_IMPACT:
            return "ICE Europe Financials iMpact"
        if self == Dataset.XEUR_EOBI:
            return "Eurex EOBI"
        if self == Dataset.XEEE_EOBI:
            return "European Energy Exchange EOBI"
        raise ValueError("Unexpected Dataset value")


@unique
@coercible
class Publisher(StringyMixin, str, Enum):
    """
    A specific Venue from a specific data source.

    GLBX_MDP3_GLBX
        CME Globex MDP 3.0.
    XNAS_ITCH_XNAS
        Nasdaq TotalView-ITCH.
    XBOS_ITCH_XBOS
        Nasdaq BX TotalView-ITCH.
    XPSX_ITCH_XPSX
        Nasdaq PSX TotalView-ITCH.
    BATS_PITCH_BATS
        Cboe BZX Depth.
    BATY_PITCH_BATY
        Cboe BYX Depth.
    EDGA_PITCH_EDGA
        Cboe EDGA Depth.
    EDGX_PITCH_EDGX
        Cboe EDGX Depth.
    XNYS_PILLAR_XNYS
        NYSE Integrated.
    XCIS_PILLAR_XCIS
        NYSE National Integrated.
    XASE_PILLAR_XASE
        NYSE American Integrated.
    XCHI_PILLAR_XCHI
        NYSE Texas Integrated.
    XCIS_BBO_XCIS
        NYSE National BBO.
    XCIS_TRADES_XCIS
        NYSE National Trades.
    MEMX_MEMOIR_MEMX
        MEMX Memoir Depth.
    EPRL_DOM_EPRL
        MIAX Pearl Depth.
    XNAS_NLS_FINN
        FINRA/Nasdaq TRF Carteret.
    XNAS_NLS_FINC
        FINRA/Nasdaq TRF Chicago.
    XNYS_TRADES_FINY
        FINRA/NYSE TRF.
    OPRA_PILLAR_AMXO
        OPRA - NYSE American Options.
    OPRA_PILLAR_XBOX
        OPRA - BOX Options.
    OPRA_PILLAR_XCBO
        OPRA - Cboe Options.
    OPRA_PILLAR_EMLD
        OPRA - MIAX Emerald.
    OPRA_PILLAR_EDGO
        OPRA - Cboe EDGX Options.
    OPRA_PILLAR_GMNI
        OPRA - Nasdaq GEMX.
    OPRA_PILLAR_XISX
        OPRA - Nasdaq ISE.
    OPRA_PILLAR_MCRY
        OPRA - Nasdaq MRX.
    OPRA_PILLAR_XMIO
        OPRA - MIAX Options.
    OPRA_PILLAR_ARCO
        OPRA - NYSE Arca Options.
    OPRA_PILLAR_OPRA
        OPRA - Options Price Reporting Authority.
    OPRA_PILLAR_MPRL
        OPRA - MIAX Pearl.
    OPRA_PILLAR_XNDQ
        OPRA - Nasdaq Options.
    OPRA_PILLAR_XBXO
        OPRA - Nasdaq BX Options.
    OPRA_PILLAR_C2OX
        OPRA - Cboe C2 Options.
    OPRA_PILLAR_XPHL
        OPRA - Nasdaq PHLX.
    OPRA_PILLAR_BATO
        OPRA - Cboe BZX Options.
    OPRA_PILLAR_MXOP
        OPRA - MEMX Options.
    IEXG_TOPS_IEXG
        IEX TOPS.
    DBEQ_BASIC_XCHI
        DBEQ Basic - NYSE Texas.
    DBEQ_BASIC_XCIS
        DBEQ Basic - NYSE National.
    DBEQ_BASIC_IEXG
        DBEQ Basic - IEX.
    DBEQ_BASIC_EPRL
        DBEQ Basic - MIAX Pearl.
    ARCX_PILLAR_ARCX
        NYSE Arca Integrated.
    XNYS_BBO_XNYS
        NYSE BBO.
    XNYS_TRADES_XNYS
        NYSE Trades.
    XNAS_QBBO_XNAS
        Nasdaq QBBO.
    XNAS_NLS_XNAS
        Nasdaq Trades.
    EQUS_PLUS_XCHI
        Databento US Equities Plus - NYSE Texas.
    EQUS_PLUS_XCIS
        Databento US Equities Plus - NYSE National.
    EQUS_PLUS_IEXG
        Databento US Equities Plus - IEX.
    EQUS_PLUS_EPRL
        Databento US Equities Plus - MIAX Pearl.
    EQUS_PLUS_XNAS
        Databento US Equities Plus - Nasdaq.
    EQUS_PLUS_XNYS
        Databento US Equities Plus - NYSE.
    EQUS_PLUS_FINN
        Databento US Equities Plus - FINRA/Nasdaq TRF Carteret.
    EQUS_PLUS_FINY
        Databento US Equities Plus - FINRA/NYSE TRF.
    EQUS_PLUS_FINC
        Databento US Equities Plus - FINRA/Nasdaq TRF Chicago.
    IFEU_IMPACT_IFEU
        ICE Europe Commodities.
    NDEX_IMPACT_NDEX
        ICE Endex.
    DBEQ_BASIC_DBEQ
        Databento US Equities Basic - Consolidated.
    EQUS_PLUS_EQUS
        EQUS Plus - Consolidated.
    OPRA_PILLAR_SPHR
        OPRA - MIAX Sapphire.
    EQUS_ALL_XCHI
        Databento US Equities (All Feeds) - NYSE Texas.
    EQUS_ALL_XCIS
        Databento US Equities (All Feeds) - NYSE National.
    EQUS_ALL_IEXG
        Databento US Equities (All Feeds) - IEX.
    EQUS_ALL_EPRL
        Databento US Equities (All Feeds) - MIAX Pearl.
    EQUS_ALL_XNAS
        Databento US Equities (All Feeds) - Nasdaq.
    EQUS_ALL_XNYS
        Databento US Equities (All Feeds) - NYSE.
    EQUS_ALL_FINN
        Databento US Equities (All Feeds) - FINRA/Nasdaq TRF Carteret.
    EQUS_ALL_FINY
        Databento US Equities (All Feeds) - FINRA/NYSE TRF.
    EQUS_ALL_FINC
        Databento US Equities (All Feeds) - FINRA/Nasdaq TRF Chicago.
    EQUS_ALL_BATS
        Databento US Equities (All Feeds) - Cboe BZX.
    EQUS_ALL_BATY
        Databento US Equities (All Feeds) - Cboe BYX.
    EQUS_ALL_EDGA
        Databento US Equities (All Feeds) - Cboe EDGA.
    EQUS_ALL_EDGX
        Databento US Equities (All Feeds) - Cboe EDGX.
    EQUS_ALL_XBOS
        Databento US Equities (All Feeds) - Nasdaq BX.
    EQUS_ALL_XPSX
        Databento US Equities (All Feeds) - Nasdaq PSX.
    EQUS_ALL_MEMX
        Databento US Equities (All Feeds) - MEMX.
    EQUS_ALL_XASE
        Databento US Equities (All Feeds) - NYSE American.
    EQUS_ALL_ARCX
        Databento US Equities (All Feeds) - NYSE Arca.
    EQUS_ALL_LTSE
        Databento US Equities (All Feeds) - Long-Term Stock Exchange.
    XNAS_BASIC_XNAS
        Nasdaq Basic - Nasdaq.
    XNAS_BASIC_FINN
        Nasdaq Basic - FINRA/Nasdaq TRF Carteret.
    XNAS_BASIC_FINC
        Nasdaq Basic - FINRA/Nasdaq TRF Chicago.
    IFEU_IMPACT_XOFF
        ICE Europe - Off-Market Trades.
    NDEX_IMPACT_XOFF
        ICE Endex - Off-Market Trades.
    XNAS_NLS_XBOS
        Nasdaq NLS - Nasdaq BX.
    XNAS_NLS_XPSX
        Nasdaq NLS - Nasdaq PSX.
    XNAS_BASIC_XBOS
        Nasdaq Basic - Nasdaq BX.
    XNAS_BASIC_XPSX
        Nasdaq Basic - Nasdaq PSX.
    EQUS_SUMMARY_EQUS
        Databento Equities Summary.
    XCIS_TRADESBBO_XCIS
        NYSE National Trades and BBO.
    XNYS_TRADESBBO_XNYS
        NYSE Trades and BBO.
    XNAS_BASIC_EQUS
        Nasdaq Basic - Consolidated.
    EQUS_ALL_EQUS
        Databento US Equities (All Feeds) - Consolidated.
    EQUS_MINI_EQUS
        Databento US Equities Mini.
    XNYS_TRADES_EQUS
        NYSE Trades - Consolidated.
    IFUS_IMPACT_IFUS
        ICE Futures US.
    IFUS_IMPACT_XOFF
        ICE Futures US - Off-Market Trades.
    IFLL_IMPACT_IFLL
        ICE Europe Financials.
    IFLL_IMPACT_XOFF
        ICE Europe Financials - Off-Market Trades.
    XEUR_EOBI_XEUR
        Eurex EOBI.
    XEEE_EOBI_XEEE
        European Energy Exchange EOBI.
    XEUR_EOBI_XOFF
        Eurex EOBI - Off-Market Trades.
    XEEE_EOBI_XOFF
        European Energy Exchange EOBI - Off-Market Trades.

    """

    GLBX_MDP3_GLBX = "GLBX.MDP3.GLBX"
    XNAS_ITCH_XNAS = "XNAS.ITCH.XNAS"
    XBOS_ITCH_XBOS = "XBOS.ITCH.XBOS"
    XPSX_ITCH_XPSX = "XPSX.ITCH.XPSX"
    BATS_PITCH_BATS = "BATS.PITCH.BATS"
    BATY_PITCH_BATY = "BATY.PITCH.BATY"
    EDGA_PITCH_EDGA = "EDGA.PITCH.EDGA"
    EDGX_PITCH_EDGX = "EDGX.PITCH.EDGX"
    XNYS_PILLAR_XNYS = "XNYS.PILLAR.XNYS"
    XCIS_PILLAR_XCIS = "XCIS.PILLAR.XCIS"
    XASE_PILLAR_XASE = "XASE.PILLAR.XASE"
    XCHI_PILLAR_XCHI = "XCHI.PILLAR.XCHI"
    XCIS_BBO_XCIS = "XCIS.BBO.XCIS"
    XCIS_TRADES_XCIS = "XCIS.TRADES.XCIS"
    MEMX_MEMOIR_MEMX = "MEMX.MEMOIR.MEMX"
    EPRL_DOM_EPRL = "EPRL.DOM.EPRL"
    XNAS_NLS_FINN = "XNAS.NLS.FINN"
    XNAS_NLS_FINC = "XNAS.NLS.FINC"
    XNYS_TRADES_FINY = "XNYS.TRADES.FINY"
    OPRA_PILLAR_AMXO = "OPRA.PILLAR.AMXO"
    OPRA_PILLAR_XBOX = "OPRA.PILLAR.XBOX"
    OPRA_PILLAR_XCBO = "OPRA.PILLAR.XCBO"
    OPRA_PILLAR_EMLD = "OPRA.PILLAR.EMLD"
    OPRA_PILLAR_EDGO = "OPRA.PILLAR.EDGO"
    OPRA_PILLAR_GMNI = "OPRA.PILLAR.GMNI"
    OPRA_PILLAR_XISX = "OPRA.PILLAR.XISX"
    OPRA_PILLAR_MCRY = "OPRA.PILLAR.MCRY"
    OPRA_PILLAR_XMIO = "OPRA.PILLAR.XMIO"
    OPRA_PILLAR_ARCO = "OPRA.PILLAR.ARCO"
    OPRA_PILLAR_OPRA = "OPRA.PILLAR.OPRA"
    OPRA_PILLAR_MPRL = "OPRA.PILLAR.MPRL"
    OPRA_PILLAR_XNDQ = "OPRA.PILLAR.XNDQ"
    OPRA_PILLAR_XBXO = "OPRA.PILLAR.XBXO"
    OPRA_PILLAR_C2OX = "OPRA.PILLAR.C2OX"
    OPRA_PILLAR_XPHL = "OPRA.PILLAR.XPHL"
    OPRA_PILLAR_BATO = "OPRA.PILLAR.BATO"
    OPRA_PILLAR_MXOP = "OPRA.PILLAR.MXOP"
    IEXG_TOPS_IEXG = "IEXG.TOPS.IEXG"
    DBEQ_BASIC_XCHI = "DBEQ.BASIC.XCHI"
    DBEQ_BASIC_XCIS = "DBEQ.BASIC.XCIS"
    DBEQ_BASIC_IEXG = "DBEQ.BASIC.IEXG"
    DBEQ_BASIC_EPRL = "DBEQ.BASIC.EPRL"
    ARCX_PILLAR_ARCX = "ARCX.PILLAR.ARCX"
    XNYS_BBO_XNYS = "XNYS.BBO.XNYS"
    XNYS_TRADES_XNYS = "XNYS.TRADES.XNYS"
    XNAS_QBBO_XNAS = "XNAS.QBBO.XNAS"
    XNAS_NLS_XNAS = "XNAS.NLS.XNAS"
    EQUS_PLUS_XCHI = "EQUS.PLUS.XCHI"
    EQUS_PLUS_XCIS = "EQUS.PLUS.XCIS"
    EQUS_PLUS_IEXG = "EQUS.PLUS.IEXG"
    EQUS_PLUS_EPRL = "EQUS.PLUS.EPRL"
    EQUS_PLUS_XNAS = "EQUS.PLUS.XNAS"
    EQUS_PLUS_XNYS = "EQUS.PLUS.XNYS"
    EQUS_PLUS_FINN = "EQUS.PLUS.FINN"
    EQUS_PLUS_FINY = "EQUS.PLUS.FINY"
    EQUS_PLUS_FINC = "EQUS.PLUS.FINC"
    IFEU_IMPACT_IFEU = "IFEU.IMPACT.IFEU"
    NDEX_IMPACT_NDEX = "NDEX.IMPACT.NDEX"
    DBEQ_BASIC_DBEQ = "DBEQ.BASIC.DBEQ"
    EQUS_PLUS_EQUS = "EQUS.PLUS.EQUS"
    OPRA_PILLAR_SPHR = "OPRA.PILLAR.SPHR"
    EQUS_ALL_XCHI = "EQUS.ALL.XCHI"
    EQUS_ALL_XCIS = "EQUS.ALL.XCIS"
    EQUS_ALL_IEXG = "EQUS.ALL.IEXG"
    EQUS_ALL_EPRL = "EQUS.ALL.EPRL"
    EQUS_ALL_XNAS = "EQUS.ALL.XNAS"
    EQUS_ALL_XNYS = "EQUS.ALL.XNYS"
    EQUS_ALL_FINN = "EQUS.ALL.FINN"
    EQUS_ALL_FINY = "EQUS.ALL.FINY"
    EQUS_ALL_FINC = "EQUS.ALL.FINC"
    EQUS_ALL_BATS = "EQUS.ALL.BATS"
    EQUS_ALL_BATY = "EQUS.ALL.BATY"
    EQUS_ALL_EDGA = "EQUS.ALL.EDGA"
    EQUS_ALL_EDGX = "EQUS.ALL.EDGX"
    EQUS_ALL_XBOS = "EQUS.ALL.XBOS"
    EQUS_ALL_XPSX = "EQUS.ALL.XPSX"
    EQUS_ALL_MEMX = "EQUS.ALL.MEMX"
    EQUS_ALL_XASE = "EQUS.ALL.XASE"
    EQUS_ALL_ARCX = "EQUS.ALL.ARCX"
    EQUS_ALL_LTSE = "EQUS.ALL.LTSE"
    XNAS_BASIC_XNAS = "XNAS.BASIC.XNAS"
    XNAS_BASIC_FINN = "XNAS.BASIC.FINN"
    XNAS_BASIC_FINC = "XNAS.BASIC.FINC"
    IFEU_IMPACT_XOFF = "IFEU.IMPACT.XOFF"
    NDEX_IMPACT_XOFF = "NDEX.IMPACT.XOFF"
    XNAS_NLS_XBOS = "XNAS.NLS.XBOS"
    XNAS_NLS_XPSX = "XNAS.NLS.XPSX"
    XNAS_BASIC_XBOS = "XNAS.BASIC.XBOS"
    XNAS_BASIC_XPSX = "XNAS.BASIC.XPSX"
    EQUS_SUMMARY_EQUS = "EQUS.SUMMARY.EQUS"
    XCIS_TRADESBBO_XCIS = "XCIS.TRADESBBO.XCIS"
    XNYS_TRADESBBO_XNYS = "XNYS.TRADESBBO.XNYS"
    XNAS_BASIC_EQUS = "XNAS.BASIC.EQUS"
    EQUS_ALL_EQUS = "EQUS.ALL.EQUS"
    EQUS_MINI_EQUS = "EQUS.MINI.EQUS"
    XNYS_TRADES_EQUS = "XNYS.TRADES.EQUS"
    IFUS_IMPACT_IFUS = "IFUS.IMPACT.IFUS"
    IFUS_IMPACT_XOFF = "IFUS.IMPACT.XOFF"
    IFLL_IMPACT_IFLL = "IFLL.IMPACT.IFLL"
    IFLL_IMPACT_XOFF = "IFLL.IMPACT.XOFF"
    XEUR_EOBI_XEUR = "XEUR.EOBI.XEUR"
    XEEE_EOBI_XEEE = "XEEE.EOBI.XEEE"
    XEUR_EOBI_XOFF = "XEUR.EOBI.XOFF"
    XEEE_EOBI_XOFF = "XEEE.EOBI.XOFF"

    @classmethod
    def from_int(cls, value: int) -> Publisher:
        """
        Get a Publisher from the given integer value.
        """
        if value == 1:
            return Publisher.GLBX_MDP3_GLBX
        if value == 2:
            return Publisher.XNAS_ITCH_XNAS
        if value == 3:
            return Publisher.XBOS_ITCH_XBOS
        if value == 4:
            return Publisher.XPSX_ITCH_XPSX
        if value == 5:
            return Publisher.BATS_PITCH_BATS
        if value == 6:
            return Publisher.BATY_PITCH_BATY
        if value == 7:
            return Publisher.EDGA_PITCH_EDGA
        if value == 8:
            return Publisher.EDGX_PITCH_EDGX
        if value == 9:
            return Publisher.XNYS_PILLAR_XNYS
        if value == 10:
            return Publisher.XCIS_PILLAR_XCIS
        if value == 11:
            return Publisher.XASE_PILLAR_XASE
        if value == 12:
            return Publisher.XCHI_PILLAR_XCHI
        if value == 13:
            return Publisher.XCIS_BBO_XCIS
        if value == 14:
            return Publisher.XCIS_TRADES_XCIS
        if value == 15:
            return Publisher.MEMX_MEMOIR_MEMX
        if value == 16:
            return Publisher.EPRL_DOM_EPRL
        if value == 17:
            return Publisher.XNAS_NLS_FINN
        if value == 18:
            return Publisher.XNAS_NLS_FINC
        if value == 19:
            return Publisher.XNYS_TRADES_FINY
        if value == 20:
            return Publisher.OPRA_PILLAR_AMXO
        if value == 21:
            return Publisher.OPRA_PILLAR_XBOX
        if value == 22:
            return Publisher.OPRA_PILLAR_XCBO
        if value == 23:
            return Publisher.OPRA_PILLAR_EMLD
        if value == 24:
            return Publisher.OPRA_PILLAR_EDGO
        if value == 25:
            return Publisher.OPRA_PILLAR_GMNI
        if value == 26:
            return Publisher.OPRA_PILLAR_XISX
        if value == 27:
            return Publisher.OPRA_PILLAR_MCRY
        if value == 28:
            return Publisher.OPRA_PILLAR_XMIO
        if value == 29:
            return Publisher.OPRA_PILLAR_ARCO
        if value == 30:
            return Publisher.OPRA_PILLAR_OPRA
        if value == 31:
            return Publisher.OPRA_PILLAR_MPRL
        if value == 32:
            return Publisher.OPRA_PILLAR_XNDQ
        if value == 33:
            return Publisher.OPRA_PILLAR_XBXO
        if value == 34:
            return Publisher.OPRA_PILLAR_C2OX
        if value == 35:
            return Publisher.OPRA_PILLAR_XPHL
        if value == 36:
            return Publisher.OPRA_PILLAR_BATO
        if value == 37:
            return Publisher.OPRA_PILLAR_MXOP
        if value == 38:
            return Publisher.IEXG_TOPS_IEXG
        if value == 39:
            return Publisher.DBEQ_BASIC_XCHI
        if value == 40:
            return Publisher.DBEQ_BASIC_XCIS
        if value == 41:
            return Publisher.DBEQ_BASIC_IEXG
        if value == 42:
            return Publisher.DBEQ_BASIC_EPRL
        if value == 43:
            return Publisher.ARCX_PILLAR_ARCX
        if value == 44:
            return Publisher.XNYS_BBO_XNYS
        if value == 45:
            return Publisher.XNYS_TRADES_XNYS
        if value == 46:
            return Publisher.XNAS_QBBO_XNAS
        if value == 47:
            return Publisher.XNAS_NLS_XNAS
        if value == 48:
            return Publisher.EQUS_PLUS_XCHI
        if value == 49:
            return Publisher.EQUS_PLUS_XCIS
        if value == 50:
            return Publisher.EQUS_PLUS_IEXG
        if value == 51:
            return Publisher.EQUS_PLUS_EPRL
        if value == 52:
            return Publisher.EQUS_PLUS_XNAS
        if value == 53:
            return Publisher.EQUS_PLUS_XNYS
        if value == 54:
            return Publisher.EQUS_PLUS_FINN
        if value == 55:
            return Publisher.EQUS_PLUS_FINY
        if value == 56:
            return Publisher.EQUS_PLUS_FINC
        if value == 57:
            return Publisher.IFEU_IMPACT_IFEU
        if value == 58:
            return Publisher.NDEX_IMPACT_NDEX
        if value == 59:
            return Publisher.DBEQ_BASIC_DBEQ
        if value == 60:
            return Publisher.EQUS_PLUS_EQUS
        if value == 61:
            return Publisher.OPRA_PILLAR_SPHR
        if value == 62:
            return Publisher.EQUS_ALL_XCHI
        if value == 63:
            return Publisher.EQUS_ALL_XCIS
        if value == 64:
            return Publisher.EQUS_ALL_IEXG
        if value == 65:
            return Publisher.EQUS_ALL_EPRL
        if value == 66:
            return Publisher.EQUS_ALL_XNAS
        if value == 67:
            return Publisher.EQUS_ALL_XNYS
        if value == 68:
            return Publisher.EQUS_ALL_FINN
        if value == 69:
            return Publisher.EQUS_ALL_FINY
        if value == 70:
            return Publisher.EQUS_ALL_FINC
        if value == 71:
            return Publisher.EQUS_ALL_BATS
        if value == 72:
            return Publisher.EQUS_ALL_BATY
        if value == 73:
            return Publisher.EQUS_ALL_EDGA
        if value == 74:
            return Publisher.EQUS_ALL_EDGX
        if value == 75:
            return Publisher.EQUS_ALL_XBOS
        if value == 76:
            return Publisher.EQUS_ALL_XPSX
        if value == 77:
            return Publisher.EQUS_ALL_MEMX
        if value == 78:
            return Publisher.EQUS_ALL_XASE
        if value == 79:
            return Publisher.EQUS_ALL_ARCX
        if value == 80:
            return Publisher.EQUS_ALL_LTSE
        if value == 81:
            return Publisher.XNAS_BASIC_XNAS
        if value == 82:
            return Publisher.XNAS_BASIC_FINN
        if value == 83:
            return Publisher.XNAS_BASIC_FINC
        if value == 84:
            return Publisher.IFEU_IMPACT_XOFF
        if value == 85:
            return Publisher.NDEX_IMPACT_XOFF
        if value == 86:
            return Publisher.XNAS_NLS_XBOS
        if value == 87:
            return Publisher.XNAS_NLS_XPSX
        if value == 88:
            return Publisher.XNAS_BASIC_XBOS
        if value == 89:
            return Publisher.XNAS_BASIC_XPSX
        if value == 90:
            return Publisher.EQUS_SUMMARY_EQUS
        if value == 91:
            return Publisher.XCIS_TRADESBBO_XCIS
        if value == 92:
            return Publisher.XNYS_TRADESBBO_XNYS
        if value == 93:
            return Publisher.XNAS_BASIC_EQUS
        if value == 94:
            return Publisher.EQUS_ALL_EQUS
        if value == 95:
            return Publisher.EQUS_MINI_EQUS
        if value == 96:
            return Publisher.XNYS_TRADES_EQUS
        if value == 97:
            return Publisher.IFUS_IMPACT_IFUS
        if value == 98:
            return Publisher.IFUS_IMPACT_XOFF
        if value == 99:
            return Publisher.IFLL_IMPACT_IFLL
        if value == 100:
            return Publisher.IFLL_IMPACT_XOFF
        if value == 101:
            return Publisher.XEUR_EOBI_XEUR
        if value == 102:
            return Publisher.XEEE_EOBI_XEEE
        if value == 103:
            return Publisher.XEUR_EOBI_XOFF
        if value == 104:
            return Publisher.XEEE_EOBI_XOFF
        raise ValueError(f"Integer value {value} does not correspond with any Publisher variant")

    def to_int(self) -> int:
        """
        Get a Publisher's integer value.
        """
        if self == Publisher.GLBX_MDP3_GLBX:
            return 1
        if self == Publisher.XNAS_ITCH_XNAS:
            return 2
        if self == Publisher.XBOS_ITCH_XBOS:
            return 3
        if self == Publisher.XPSX_ITCH_XPSX:
            return 4
        if self == Publisher.BATS_PITCH_BATS:
            return 5
        if self == Publisher.BATY_PITCH_BATY:
            return 6
        if self == Publisher.EDGA_PITCH_EDGA:
            return 7
        if self == Publisher.EDGX_PITCH_EDGX:
            return 8
        if self == Publisher.XNYS_PILLAR_XNYS:
            return 9
        if self == Publisher.XCIS_PILLAR_XCIS:
            return 10
        if self == Publisher.XASE_PILLAR_XASE:
            return 11
        if self == Publisher.XCHI_PILLAR_XCHI:
            return 12
        if self == Publisher.XCIS_BBO_XCIS:
            return 13
        if self == Publisher.XCIS_TRADES_XCIS:
            return 14
        if self == Publisher.MEMX_MEMOIR_MEMX:
            return 15
        if self == Publisher.EPRL_DOM_EPRL:
            return 16
        if self == Publisher.XNAS_NLS_FINN:
            return 17
        if self == Publisher.XNAS_NLS_FINC:
            return 18
        if self == Publisher.XNYS_TRADES_FINY:
            return 19
        if self == Publisher.OPRA_PILLAR_AMXO:
            return 20
        if self == Publisher.OPRA_PILLAR_XBOX:
            return 21
        if self == Publisher.OPRA_PILLAR_XCBO:
            return 22
        if self == Publisher.OPRA_PILLAR_EMLD:
            return 23
        if self == Publisher.OPRA_PILLAR_EDGO:
            return 24
        if self == Publisher.OPRA_PILLAR_GMNI:
            return 25
        if self == Publisher.OPRA_PILLAR_XISX:
            return 26
        if self == Publisher.OPRA_PILLAR_MCRY:
            return 27
        if self == Publisher.OPRA_PILLAR_XMIO:
            return 28
        if self == Publisher.OPRA_PILLAR_ARCO:
            return 29
        if self == Publisher.OPRA_PILLAR_OPRA:
            return 30
        if self == Publisher.OPRA_PILLAR_MPRL:
            return 31
        if self == Publisher.OPRA_PILLAR_XNDQ:
            return 32
        if self == Publisher.OPRA_PILLAR_XBXO:
            return 33
        if self == Publisher.OPRA_PILLAR_C2OX:
            return 34
        if self == Publisher.OPRA_PILLAR_XPHL:
            return 35
        if self == Publisher.OPRA_PILLAR_BATO:
            return 36
        if self == Publisher.OPRA_PILLAR_MXOP:
            return 37
        if self == Publisher.IEXG_TOPS_IEXG:
            return 38
        if self == Publisher.DBEQ_BASIC_XCHI:
            return 39
        if self == Publisher.DBEQ_BASIC_XCIS:
            return 40
        if self == Publisher.DBEQ_BASIC_IEXG:
            return 41
        if self == Publisher.DBEQ_BASIC_EPRL:
            return 42
        if self == Publisher.ARCX_PILLAR_ARCX:
            return 43
        if self == Publisher.XNYS_BBO_XNYS:
            return 44
        if self == Publisher.XNYS_TRADES_XNYS:
            return 45
        if self == Publisher.XNAS_QBBO_XNAS:
            return 46
        if self == Publisher.XNAS_NLS_XNAS:
            return 47
        if self == Publisher.EQUS_PLUS_XCHI:
            return 48
        if self == Publisher.EQUS_PLUS_XCIS:
            return 49
        if self == Publisher.EQUS_PLUS_IEXG:
            return 50
        if self == Publisher.EQUS_PLUS_EPRL:
            return 51
        if self == Publisher.EQUS_PLUS_XNAS:
            return 52
        if self == Publisher.EQUS_PLUS_XNYS:
            return 53
        if self == Publisher.EQUS_PLUS_FINN:
            return 54
        if self == Publisher.EQUS_PLUS_FINY:
            return 55
        if self == Publisher.EQUS_PLUS_FINC:
            return 56
        if self == Publisher.IFEU_IMPACT_IFEU:
            return 57
        if self == Publisher.NDEX_IMPACT_NDEX:
            return 58
        if self == Publisher.DBEQ_BASIC_DBEQ:
            return 59
        if self == Publisher.EQUS_PLUS_EQUS:
            return 60
        if self == Publisher.OPRA_PILLAR_SPHR:
            return 61
        if self == Publisher.EQUS_ALL_XCHI:
            return 62
        if self == Publisher.EQUS_ALL_XCIS:
            return 63
        if self == Publisher.EQUS_ALL_IEXG:
            return 64
        if self == Publisher.EQUS_ALL_EPRL:
            return 65
        if self == Publisher.EQUS_ALL_XNAS:
            return 66
        if self == Publisher.EQUS_ALL_XNYS:
            return 67
        if self == Publisher.EQUS_ALL_FINN:
            return 68
        if self == Publisher.EQUS_ALL_FINY:
            return 69
        if self == Publisher.EQUS_ALL_FINC:
            return 70
        if self == Publisher.EQUS_ALL_BATS:
            return 71
        if self == Publisher.EQUS_ALL_BATY:
            return 72
        if self == Publisher.EQUS_ALL_EDGA:
            return 73
        if self == Publisher.EQUS_ALL_EDGX:
            return 74
        if self == Publisher.EQUS_ALL_XBOS:
            return 75
        if self == Publisher.EQUS_ALL_XPSX:
            return 76
        if self == Publisher.EQUS_ALL_MEMX:
            return 77
        if self == Publisher.EQUS_ALL_XASE:
            return 78
        if self == Publisher.EQUS_ALL_ARCX:
            return 79
        if self == Publisher.EQUS_ALL_LTSE:
            return 80
        if self == Publisher.XNAS_BASIC_XNAS:
            return 81
        if self == Publisher.XNAS_BASIC_FINN:
            return 82
        if self == Publisher.XNAS_BASIC_FINC:
            return 83
        if self == Publisher.IFEU_IMPACT_XOFF:
            return 84
        if self == Publisher.NDEX_IMPACT_XOFF:
            return 85
        if self == Publisher.XNAS_NLS_XBOS:
            return 86
        if self == Publisher.XNAS_NLS_XPSX:
            return 87
        if self == Publisher.XNAS_BASIC_XBOS:
            return 88
        if self == Publisher.XNAS_BASIC_XPSX:
            return 89
        if self == Publisher.EQUS_SUMMARY_EQUS:
            return 90
        if self == Publisher.XCIS_TRADESBBO_XCIS:
            return 91
        if self == Publisher.XNYS_TRADESBBO_XNYS:
            return 92
        if self == Publisher.XNAS_BASIC_EQUS:
            return 93
        if self == Publisher.EQUS_ALL_EQUS:
            return 94
        if self == Publisher.EQUS_MINI_EQUS:
            return 95
        if self == Publisher.XNYS_TRADES_EQUS:
            return 96
        if self == Publisher.IFUS_IMPACT_IFUS:
            return 97
        if self == Publisher.IFUS_IMPACT_XOFF:
            return 98
        if self == Publisher.IFLL_IMPACT_IFLL:
            return 99
        if self == Publisher.IFLL_IMPACT_XOFF:
            return 100
        if self == Publisher.XEUR_EOBI_XEUR:
            return 101
        if self == Publisher.XEEE_EOBI_XEEE:
            return 102
        if self == Publisher.XEUR_EOBI_XOFF:
            return 103
        if self == Publisher.XEEE_EOBI_XOFF:
            return 104
        raise ValueError("Invalid Publisher")

    @property
    def venue(self) -> Venue:
        """
        Get a Publisher's Venue.
        """
        if self == Publisher.GLBX_MDP3_GLBX:
            return Venue.GLBX
        if self == Publisher.XNAS_ITCH_XNAS:
            return Venue.XNAS
        if self == Publisher.XBOS_ITCH_XBOS:
            return Venue.XBOS
        if self == Publisher.XPSX_ITCH_XPSX:
            return Venue.XPSX
        if self == Publisher.BATS_PITCH_BATS:
            return Venue.BATS
        if self == Publisher.BATY_PITCH_BATY:
            return Venue.BATY
        if self == Publisher.EDGA_PITCH_EDGA:
            return Venue.EDGA
        if self == Publisher.EDGX_PITCH_EDGX:
            return Venue.EDGX
        if self == Publisher.XNYS_PILLAR_XNYS:
            return Venue.XNYS
        if self == Publisher.XCIS_PILLAR_XCIS:
            return Venue.XCIS
        if self == Publisher.XASE_PILLAR_XASE:
            return Venue.XASE
        if self == Publisher.XCHI_PILLAR_XCHI:
            return Venue.XCHI
        if self == Publisher.XCIS_BBO_XCIS:
            return Venue.XCIS
        if self == Publisher.XCIS_TRADES_XCIS:
            return Venue.XCIS
        if self == Publisher.MEMX_MEMOIR_MEMX:
            return Venue.MEMX
        if self == Publisher.EPRL_DOM_EPRL:
            return Venue.EPRL
        if self == Publisher.XNAS_NLS_FINN:
            return Venue.FINN
        if self == Publisher.XNAS_NLS_FINC:
            return Venue.FINC
        if self == Publisher.XNYS_TRADES_FINY:
            return Venue.FINY
        if self == Publisher.OPRA_PILLAR_AMXO:
            return Venue.AMXO
        if self == Publisher.OPRA_PILLAR_XBOX:
            return Venue.XBOX
        if self == Publisher.OPRA_PILLAR_XCBO:
            return Venue.XCBO
        if self == Publisher.OPRA_PILLAR_EMLD:
            return Venue.EMLD
        if self == Publisher.OPRA_PILLAR_EDGO:
            return Venue.EDGO
        if self == Publisher.OPRA_PILLAR_GMNI:
            return Venue.GMNI
        if self == Publisher.OPRA_PILLAR_XISX:
            return Venue.XISX
        if self == Publisher.OPRA_PILLAR_MCRY:
            return Venue.MCRY
        if self == Publisher.OPRA_PILLAR_XMIO:
            return Venue.XMIO
        if self == Publisher.OPRA_PILLAR_ARCO:
            return Venue.ARCO
        if self == Publisher.OPRA_PILLAR_OPRA:
            return Venue.OPRA
        if self == Publisher.OPRA_PILLAR_MPRL:
            return Venue.MPRL
        if self == Publisher.OPRA_PILLAR_XNDQ:
            return Venue.XNDQ
        if self == Publisher.OPRA_PILLAR_XBXO:
            return Venue.XBXO
        if self == Publisher.OPRA_PILLAR_C2OX:
            return Venue.C2OX
        if self == Publisher.OPRA_PILLAR_XPHL:
            return Venue.XPHL
        if self == Publisher.OPRA_PILLAR_BATO:
            return Venue.BATO
        if self == Publisher.OPRA_PILLAR_MXOP:
            return Venue.MXOP
        if self == Publisher.IEXG_TOPS_IEXG:
            return Venue.IEXG
        if self == Publisher.DBEQ_BASIC_XCHI:
            return Venue.XCHI
        if self == Publisher.DBEQ_BASIC_XCIS:
            return Venue.XCIS
        if self == Publisher.DBEQ_BASIC_IEXG:
            return Venue.IEXG
        if self == Publisher.DBEQ_BASIC_EPRL:
            return Venue.EPRL
        if self == Publisher.ARCX_PILLAR_ARCX:
            return Venue.ARCX
        if self == Publisher.XNYS_BBO_XNYS:
            return Venue.XNYS
        if self == Publisher.XNYS_TRADES_XNYS:
            return Venue.XNYS
        if self == Publisher.XNAS_QBBO_XNAS:
            return Venue.XNAS
        if self == Publisher.XNAS_NLS_XNAS:
            return Venue.XNAS
        if self == Publisher.EQUS_PLUS_XCHI:
            return Venue.XCHI
        if self == Publisher.EQUS_PLUS_XCIS:
            return Venue.XCIS
        if self == Publisher.EQUS_PLUS_IEXG:
            return Venue.IEXG
        if self == Publisher.EQUS_PLUS_EPRL:
            return Venue.EPRL
        if self == Publisher.EQUS_PLUS_XNAS:
            return Venue.XNAS
        if self == Publisher.EQUS_PLUS_XNYS:
            return Venue.XNYS
        if self == Publisher.EQUS_PLUS_FINN:
            return Venue.FINN
        if self == Publisher.EQUS_PLUS_FINY:
            return Venue.FINY
        if self == Publisher.EQUS_PLUS_FINC:
            return Venue.FINC
        if self == Publisher.IFEU_IMPACT_IFEU:
            return Venue.IFEU
        if self == Publisher.NDEX_IMPACT_NDEX:
            return Venue.NDEX
        if self == Publisher.DBEQ_BASIC_DBEQ:
            return Venue.DBEQ
        if self == Publisher.EQUS_PLUS_EQUS:
            return Venue.EQUS
        if self == Publisher.OPRA_PILLAR_SPHR:
            return Venue.SPHR
        if self == Publisher.EQUS_ALL_XCHI:
            return Venue.XCHI
        if self == Publisher.EQUS_ALL_XCIS:
            return Venue.XCIS
        if self == Publisher.EQUS_ALL_IEXG:
            return Venue.IEXG
        if self == Publisher.EQUS_ALL_EPRL:
            return Venue.EPRL
        if self == Publisher.EQUS_ALL_XNAS:
            return Venue.XNAS
        if self == Publisher.EQUS_ALL_XNYS:
            return Venue.XNYS
        if self == Publisher.EQUS_ALL_FINN:
            return Venue.FINN
        if self == Publisher.EQUS_ALL_FINY:
            return Venue.FINY
        if self == Publisher.EQUS_ALL_FINC:
            return Venue.FINC
        if self == Publisher.EQUS_ALL_BATS:
            return Venue.BATS
        if self == Publisher.EQUS_ALL_BATY:
            return Venue.BATY
        if self == Publisher.EQUS_ALL_EDGA:
            return Venue.EDGA
        if self == Publisher.EQUS_ALL_EDGX:
            return Venue.EDGX
        if self == Publisher.EQUS_ALL_XBOS:
            return Venue.XBOS
        if self == Publisher.EQUS_ALL_XPSX:
            return Venue.XPSX
        if self == Publisher.EQUS_ALL_MEMX:
            return Venue.MEMX
        if self == Publisher.EQUS_ALL_XASE:
            return Venue.XASE
        if self == Publisher.EQUS_ALL_ARCX:
            return Venue.ARCX
        if self == Publisher.EQUS_ALL_LTSE:
            return Venue.LTSE
        if self == Publisher.XNAS_BASIC_XNAS:
            return Venue.XNAS
        if self == Publisher.XNAS_BASIC_FINN:
            return Venue.FINN
        if self == Publisher.XNAS_BASIC_FINC:
            return Venue.FINC
        if self == Publisher.IFEU_IMPACT_XOFF:
            return Venue.XOFF
        if self == Publisher.NDEX_IMPACT_XOFF:
            return Venue.XOFF
        if self == Publisher.XNAS_NLS_XBOS:
            return Venue.XBOS
        if self == Publisher.XNAS_NLS_XPSX:
            return Venue.XPSX
        if self == Publisher.XNAS_BASIC_XBOS:
            return Venue.XBOS
        if self == Publisher.XNAS_BASIC_XPSX:
            return Venue.XPSX
        if self == Publisher.EQUS_SUMMARY_EQUS:
            return Venue.EQUS
        if self == Publisher.XCIS_TRADESBBO_XCIS:
            return Venue.XCIS
        if self == Publisher.XNYS_TRADESBBO_XNYS:
            return Venue.XNYS
        if self == Publisher.XNAS_BASIC_EQUS:
            return Venue.EQUS
        if self == Publisher.EQUS_ALL_EQUS:
            return Venue.EQUS
        if self == Publisher.EQUS_MINI_EQUS:
            return Venue.EQUS
        if self == Publisher.XNYS_TRADES_EQUS:
            return Venue.EQUS
        if self == Publisher.IFUS_IMPACT_IFUS:
            return Venue.IFUS
        if self == Publisher.IFUS_IMPACT_XOFF:
            return Venue.XOFF
        if self == Publisher.IFLL_IMPACT_IFLL:
            return Venue.IFLL
        if self == Publisher.IFLL_IMPACT_XOFF:
            return Venue.XOFF
        if self == Publisher.XEUR_EOBI_XEUR:
            return Venue.XEUR
        if self == Publisher.XEEE_EOBI_XEEE:
            return Venue.XEEE
        if self == Publisher.XEUR_EOBI_XOFF:
            return Venue.XOFF
        if self == Publisher.XEEE_EOBI_XOFF:
            return Venue.XOFF
        raise ValueError("Unexpected Publisher value")

    @property
    def dataset(self) -> Dataset:
        """
        Get a Publisher's Dataset.
        """
        if self == Publisher.GLBX_MDP3_GLBX:
            return Dataset.GLBX_MDP3
        if self == Publisher.XNAS_ITCH_XNAS:
            return Dataset.XNAS_ITCH
        if self == Publisher.XBOS_ITCH_XBOS:
            return Dataset.XBOS_ITCH
        if self == Publisher.XPSX_ITCH_XPSX:
            return Dataset.XPSX_ITCH
        if self == Publisher.BATS_PITCH_BATS:
            return Dataset.BATS_PITCH
        if self == Publisher.BATY_PITCH_BATY:
            return Dataset.BATY_PITCH
        if self == Publisher.EDGA_PITCH_EDGA:
            return Dataset.EDGA_PITCH
        if self == Publisher.EDGX_PITCH_EDGX:
            return Dataset.EDGX_PITCH
        if self == Publisher.XNYS_PILLAR_XNYS:
            return Dataset.XNYS_PILLAR
        if self == Publisher.XCIS_PILLAR_XCIS:
            return Dataset.XCIS_PILLAR
        if self == Publisher.XASE_PILLAR_XASE:
            return Dataset.XASE_PILLAR
        if self == Publisher.XCHI_PILLAR_XCHI:
            return Dataset.XCHI_PILLAR
        if self == Publisher.XCIS_BBO_XCIS:
            return Dataset.XCIS_BBO
        if self == Publisher.XCIS_TRADES_XCIS:
            return Dataset.XCIS_TRADES
        if self == Publisher.MEMX_MEMOIR_MEMX:
            return Dataset.MEMX_MEMOIR
        if self == Publisher.EPRL_DOM_EPRL:
            return Dataset.EPRL_DOM
        if self == Publisher.XNAS_NLS_FINN:
            return Dataset.XNAS_NLS
        if self == Publisher.XNAS_NLS_FINC:
            return Dataset.XNAS_NLS
        if self == Publisher.XNYS_TRADES_FINY:
            return Dataset.XNYS_TRADES
        if self == Publisher.OPRA_PILLAR_AMXO:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_XBOX:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_XCBO:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_EMLD:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_EDGO:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_GMNI:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_XISX:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_MCRY:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_XMIO:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_ARCO:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_OPRA:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_MPRL:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_XNDQ:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_XBXO:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_C2OX:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_XPHL:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_BATO:
            return Dataset.OPRA_PILLAR
        if self == Publisher.OPRA_PILLAR_MXOP:
            return Dataset.OPRA_PILLAR
        if self == Publisher.IEXG_TOPS_IEXG:
            return Dataset.IEXG_TOPS
        if self == Publisher.DBEQ_BASIC_XCHI:
            return Dataset.DBEQ_BASIC
        if self == Publisher.DBEQ_BASIC_XCIS:
            return Dataset.DBEQ_BASIC
        if self == Publisher.DBEQ_BASIC_IEXG:
            return Dataset.DBEQ_BASIC
        if self == Publisher.DBEQ_BASIC_EPRL:
            return Dataset.DBEQ_BASIC
        if self == Publisher.ARCX_PILLAR_ARCX:
            return Dataset.ARCX_PILLAR
        if self == Publisher.XNYS_BBO_XNYS:
            return Dataset.XNYS_BBO
        if self == Publisher.XNYS_TRADES_XNYS:
            return Dataset.XNYS_TRADES
        if self == Publisher.XNAS_QBBO_XNAS:
            return Dataset.XNAS_QBBO
        if self == Publisher.XNAS_NLS_XNAS:
            return Dataset.XNAS_NLS
        if self == Publisher.EQUS_PLUS_XCHI:
            return Dataset.EQUS_PLUS
        if self == Publisher.EQUS_PLUS_XCIS:
            return Dataset.EQUS_PLUS
        if self == Publisher.EQUS_PLUS_IEXG:
            return Dataset.EQUS_PLUS
        if self == Publisher.EQUS_PLUS_EPRL:
            return Dataset.EQUS_PLUS
        if self == Publisher.EQUS_PLUS_XNAS:
            return Dataset.EQUS_PLUS
        if self == Publisher.EQUS_PLUS_XNYS:
            return Dataset.EQUS_PLUS
        if self == Publisher.EQUS_PLUS_FINN:
            return Dataset.EQUS_PLUS
        if self == Publisher.EQUS_PLUS_FINY:
            return Dataset.EQUS_PLUS
        if self == Publisher.EQUS_PLUS_FINC:
            return Dataset.EQUS_PLUS
        if self == Publisher.IFEU_IMPACT_IFEU:
            return Dataset.IFEU_IMPACT
        if self == Publisher.NDEX_IMPACT_NDEX:
            return Dataset.NDEX_IMPACT
        if self == Publisher.DBEQ_BASIC_DBEQ:
            return Dataset.DBEQ_BASIC
        if self == Publisher.EQUS_PLUS_EQUS:
            return Dataset.EQUS_PLUS
        if self == Publisher.OPRA_PILLAR_SPHR:
            return Dataset.OPRA_PILLAR
        if self == Publisher.EQUS_ALL_XCHI:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_XCIS:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_IEXG:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_EPRL:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_XNAS:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_XNYS:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_FINN:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_FINY:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_FINC:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_BATS:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_BATY:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_EDGA:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_EDGX:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_XBOS:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_XPSX:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_MEMX:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_XASE:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_ARCX:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_ALL_LTSE:
            return Dataset.EQUS_ALL
        if self == Publisher.XNAS_BASIC_XNAS:
            return Dataset.XNAS_BASIC
        if self == Publisher.XNAS_BASIC_FINN:
            return Dataset.XNAS_BASIC
        if self == Publisher.XNAS_BASIC_FINC:
            return Dataset.XNAS_BASIC
        if self == Publisher.IFEU_IMPACT_XOFF:
            return Dataset.IFEU_IMPACT
        if self == Publisher.NDEX_IMPACT_XOFF:
            return Dataset.NDEX_IMPACT
        if self == Publisher.XNAS_NLS_XBOS:
            return Dataset.XNAS_NLS
        if self == Publisher.XNAS_NLS_XPSX:
            return Dataset.XNAS_NLS
        if self == Publisher.XNAS_BASIC_XBOS:
            return Dataset.XNAS_BASIC
        if self == Publisher.XNAS_BASIC_XPSX:
            return Dataset.XNAS_BASIC
        if self == Publisher.EQUS_SUMMARY_EQUS:
            return Dataset.EQUS_SUMMARY
        if self == Publisher.XCIS_TRADESBBO_XCIS:
            return Dataset.XCIS_TRADESBBO
        if self == Publisher.XNYS_TRADESBBO_XNYS:
            return Dataset.XNYS_TRADESBBO
        if self == Publisher.XNAS_BASIC_EQUS:
            return Dataset.XNAS_BASIC
        if self == Publisher.EQUS_ALL_EQUS:
            return Dataset.EQUS_ALL
        if self == Publisher.EQUS_MINI_EQUS:
            return Dataset.EQUS_MINI
        if self == Publisher.XNYS_TRADES_EQUS:
            return Dataset.XNYS_TRADES
        if self == Publisher.IFUS_IMPACT_IFUS:
            return Dataset.IFUS_IMPACT
        if self == Publisher.IFUS_IMPACT_XOFF:
            return Dataset.IFUS_IMPACT
        if self == Publisher.IFLL_IMPACT_IFLL:
            return Dataset.IFLL_IMPACT
        if self == Publisher.IFLL_IMPACT_XOFF:
            return Dataset.IFLL_IMPACT
        if self == Publisher.XEUR_EOBI_XEUR:
            return Dataset.XEUR_EOBI
        if self == Publisher.XEEE_EOBI_XEEE:
            return Dataset.XEEE_EOBI
        if self == Publisher.XEUR_EOBI_XOFF:
            return Dataset.XEUR_EOBI
        if self == Publisher.XEEE_EOBI_XOFF:
            return Dataset.XEEE_EOBI
        raise ValueError("Unexpected Publisher value")

    @property
    def description(self) -> str:
        """
        Get a Publisher's description.
        """
        if self == Publisher.GLBX_MDP3_GLBX:
            return "CME Globex MDP 3.0"
        if self == Publisher.XNAS_ITCH_XNAS:
            return "Nasdaq TotalView-ITCH"
        if self == Publisher.XBOS_ITCH_XBOS:
            return "Nasdaq BX TotalView-ITCH"
        if self == Publisher.XPSX_ITCH_XPSX:
            return "Nasdaq PSX TotalView-ITCH"
        if self == Publisher.BATS_PITCH_BATS:
            return "Cboe BZX Depth"
        if self == Publisher.BATY_PITCH_BATY:
            return "Cboe BYX Depth"
        if self == Publisher.EDGA_PITCH_EDGA:
            return "Cboe EDGA Depth"
        if self == Publisher.EDGX_PITCH_EDGX:
            return "Cboe EDGX Depth"
        if self == Publisher.XNYS_PILLAR_XNYS:
            return "NYSE Integrated"
        if self == Publisher.XCIS_PILLAR_XCIS:
            return "NYSE National Integrated"
        if self == Publisher.XASE_PILLAR_XASE:
            return "NYSE American Integrated"
        if self == Publisher.XCHI_PILLAR_XCHI:
            return "NYSE Texas Integrated"
        if self == Publisher.XCIS_BBO_XCIS:
            return "NYSE National BBO"
        if self == Publisher.XCIS_TRADES_XCIS:
            return "NYSE National Trades"
        if self == Publisher.MEMX_MEMOIR_MEMX:
            return "MEMX Memoir Depth"
        if self == Publisher.EPRL_DOM_EPRL:
            return "MIAX Pearl Depth"
        if self == Publisher.XNAS_NLS_FINN:
            return "FINRA/Nasdaq TRF Carteret"
        if self == Publisher.XNAS_NLS_FINC:
            return "FINRA/Nasdaq TRF Chicago"
        if self == Publisher.XNYS_TRADES_FINY:
            return "FINRA/NYSE TRF"
        if self == Publisher.OPRA_PILLAR_AMXO:
            return "OPRA - NYSE American Options"
        if self == Publisher.OPRA_PILLAR_XBOX:
            return "OPRA - BOX Options"
        if self == Publisher.OPRA_PILLAR_XCBO:
            return "OPRA - Cboe Options"
        if self == Publisher.OPRA_PILLAR_EMLD:
            return "OPRA - MIAX Emerald"
        if self == Publisher.OPRA_PILLAR_EDGO:
            return "OPRA - Cboe EDGX Options"
        if self == Publisher.OPRA_PILLAR_GMNI:
            return "OPRA - Nasdaq GEMX"
        if self == Publisher.OPRA_PILLAR_XISX:
            return "OPRA - Nasdaq ISE"
        if self == Publisher.OPRA_PILLAR_MCRY:
            return "OPRA - Nasdaq MRX"
        if self == Publisher.OPRA_PILLAR_XMIO:
            return "OPRA - MIAX Options"
        if self == Publisher.OPRA_PILLAR_ARCO:
            return "OPRA - NYSE Arca Options"
        if self == Publisher.OPRA_PILLAR_OPRA:
            return "OPRA - Options Price Reporting Authority"
        if self == Publisher.OPRA_PILLAR_MPRL:
            return "OPRA - MIAX Pearl"
        if self == Publisher.OPRA_PILLAR_XNDQ:
            return "OPRA - Nasdaq Options"
        if self == Publisher.OPRA_PILLAR_XBXO:
            return "OPRA - Nasdaq BX Options"
        if self == Publisher.OPRA_PILLAR_C2OX:
            return "OPRA - Cboe C2 Options"
        if self == Publisher.OPRA_PILLAR_XPHL:
            return "OPRA - Nasdaq PHLX"
        if self == Publisher.OPRA_PILLAR_BATO:
            return "OPRA - Cboe BZX Options"
        if self == Publisher.OPRA_PILLAR_MXOP:
            return "OPRA - MEMX Options"
        if self == Publisher.IEXG_TOPS_IEXG:
            return "IEX TOPS"
        if self == Publisher.DBEQ_BASIC_XCHI:
            return "DBEQ Basic - NYSE Texas"
        if self == Publisher.DBEQ_BASIC_XCIS:
            return "DBEQ Basic - NYSE National"
        if self == Publisher.DBEQ_BASIC_IEXG:
            return "DBEQ Basic - IEX"
        if self == Publisher.DBEQ_BASIC_EPRL:
            return "DBEQ Basic - MIAX Pearl"
        if self == Publisher.ARCX_PILLAR_ARCX:
            return "NYSE Arca Integrated"
        if self == Publisher.XNYS_BBO_XNYS:
            return "NYSE BBO"
        if self == Publisher.XNYS_TRADES_XNYS:
            return "NYSE Trades"
        if self == Publisher.XNAS_QBBO_XNAS:
            return "Nasdaq QBBO"
        if self == Publisher.XNAS_NLS_XNAS:
            return "Nasdaq Trades"
        if self == Publisher.EQUS_PLUS_XCHI:
            return "Databento US Equities Plus - NYSE Texas"
        if self == Publisher.EQUS_PLUS_XCIS:
            return "Databento US Equities Plus - NYSE National"
        if self == Publisher.EQUS_PLUS_IEXG:
            return "Databento US Equities Plus - IEX"
        if self == Publisher.EQUS_PLUS_EPRL:
            return "Databento US Equities Plus - MIAX Pearl"
        if self == Publisher.EQUS_PLUS_XNAS:
            return "Databento US Equities Plus - Nasdaq"
        if self == Publisher.EQUS_PLUS_XNYS:
            return "Databento US Equities Plus - NYSE"
        if self == Publisher.EQUS_PLUS_FINN:
            return "Databento US Equities Plus - FINRA/Nasdaq TRF Carteret"
        if self == Publisher.EQUS_PLUS_FINY:
            return "Databento US Equities Plus - FINRA/NYSE TRF"
        if self == Publisher.EQUS_PLUS_FINC:
            return "Databento US Equities Plus - FINRA/Nasdaq TRF Chicago"
        if self == Publisher.IFEU_IMPACT_IFEU:
            return "ICE Europe Commodities"
        if self == Publisher.NDEX_IMPACT_NDEX:
            return "ICE Endex"
        if self == Publisher.DBEQ_BASIC_DBEQ:
            return "Databento US Equities Basic - Consolidated"
        if self == Publisher.EQUS_PLUS_EQUS:
            return "EQUS Plus - Consolidated"
        if self == Publisher.OPRA_PILLAR_SPHR:
            return "OPRA - MIAX Sapphire"
        if self == Publisher.EQUS_ALL_XCHI:
            return "Databento US Equities (All Feeds) - NYSE Texas"
        if self == Publisher.EQUS_ALL_XCIS:
            return "Databento US Equities (All Feeds) - NYSE National"
        if self == Publisher.EQUS_ALL_IEXG:
            return "Databento US Equities (All Feeds) - IEX"
        if self == Publisher.EQUS_ALL_EPRL:
            return "Databento US Equities (All Feeds) - MIAX Pearl"
        if self == Publisher.EQUS_ALL_XNAS:
            return "Databento US Equities (All Feeds) - Nasdaq"
        if self == Publisher.EQUS_ALL_XNYS:
            return "Databento US Equities (All Feeds) - NYSE"
        if self == Publisher.EQUS_ALL_FINN:
            return "Databento US Equities (All Feeds) - FINRA/Nasdaq TRF Carteret"
        if self == Publisher.EQUS_ALL_FINY:
            return "Databento US Equities (All Feeds) - FINRA/NYSE TRF"
        if self == Publisher.EQUS_ALL_FINC:
            return "Databento US Equities (All Feeds) - FINRA/Nasdaq TRF Chicago"
        if self == Publisher.EQUS_ALL_BATS:
            return "Databento US Equities (All Feeds) - Cboe BZX"
        if self == Publisher.EQUS_ALL_BATY:
            return "Databento US Equities (All Feeds) - Cboe BYX"
        if self == Publisher.EQUS_ALL_EDGA:
            return "Databento US Equities (All Feeds) - Cboe EDGA"
        if self == Publisher.EQUS_ALL_EDGX:
            return "Databento US Equities (All Feeds) - Cboe EDGX"
        if self == Publisher.EQUS_ALL_XBOS:
            return "Databento US Equities (All Feeds) - Nasdaq BX"
        if self == Publisher.EQUS_ALL_XPSX:
            return "Databento US Equities (All Feeds) - Nasdaq PSX"
        if self == Publisher.EQUS_ALL_MEMX:
            return "Databento US Equities (All Feeds) - MEMX"
        if self == Publisher.EQUS_ALL_XASE:
            return "Databento US Equities (All Feeds) - NYSE American"
        if self == Publisher.EQUS_ALL_ARCX:
            return "Databento US Equities (All Feeds) - NYSE Arca"
        if self == Publisher.EQUS_ALL_LTSE:
            return "Databento US Equities (All Feeds) - Long-Term Stock Exchange"
        if self == Publisher.XNAS_BASIC_XNAS:
            return "Nasdaq Basic - Nasdaq"
        if self == Publisher.XNAS_BASIC_FINN:
            return "Nasdaq Basic - FINRA/Nasdaq TRF Carteret"
        if self == Publisher.XNAS_BASIC_FINC:
            return "Nasdaq Basic - FINRA/Nasdaq TRF Chicago"
        if self == Publisher.IFEU_IMPACT_XOFF:
            return "ICE Europe - Off-Market Trades"
        if self == Publisher.NDEX_IMPACT_XOFF:
            return "ICE Endex - Off-Market Trades"
        if self == Publisher.XNAS_NLS_XBOS:
            return "Nasdaq NLS - Nasdaq BX"
        if self == Publisher.XNAS_NLS_XPSX:
            return "Nasdaq NLS - Nasdaq PSX"
        if self == Publisher.XNAS_BASIC_XBOS:
            return "Nasdaq Basic - Nasdaq BX"
        if self == Publisher.XNAS_BASIC_XPSX:
            return "Nasdaq Basic - Nasdaq PSX"
        if self == Publisher.EQUS_SUMMARY_EQUS:
            return "Databento Equities Summary"
        if self == Publisher.XCIS_TRADESBBO_XCIS:
            return "NYSE National Trades and BBO"
        if self == Publisher.XNYS_TRADESBBO_XNYS:
            return "NYSE Trades and BBO"
        if self == Publisher.XNAS_BASIC_EQUS:
            return "Nasdaq Basic - Consolidated"
        if self == Publisher.EQUS_ALL_EQUS:
            return "Databento US Equities (All Feeds) - Consolidated"
        if self == Publisher.EQUS_MINI_EQUS:
            return "Databento US Equities Mini"
        if self == Publisher.XNYS_TRADES_EQUS:
            return "NYSE Trades - Consolidated"
        if self == Publisher.IFUS_IMPACT_IFUS:
            return "ICE Futures US"
        if self == Publisher.IFUS_IMPACT_XOFF:
            return "ICE Futures US - Off-Market Trades"
        if self == Publisher.IFLL_IMPACT_IFLL:
            return "ICE Europe Financials"
        if self == Publisher.IFLL_IMPACT_XOFF:
            return "ICE Europe Financials - Off-Market Trades"
        if self == Publisher.XEUR_EOBI_XEUR:
            return "Eurex EOBI"
        if self == Publisher.XEEE_EOBI_XEEE:
            return "European Energy Exchange EOBI"
        if self == Publisher.XEUR_EOBI_XOFF:
            return "Eurex EOBI - Off-Market Trades"
        if self == Publisher.XEEE_EOBI_XOFF:
            return "European Energy Exchange EOBI - Off-Market Trades"
        raise ValueError("Unexpected Publisher value")
