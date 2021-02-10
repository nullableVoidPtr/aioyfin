from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client

from typing import ClassVar, Optional, List
from bs4 import BeautifulSoup, SoupStrainer
from functools import cached_property
from urllib.parse import urlencode, quote_plus
import pandas
import re
import json


class Ticker:
    _all_modules: ClassVar[List[str]] = [
        "summaryProfile",
        "quoteType",
        "financialData",
        "recommendationTrend",
        "upgradeDowngradeHistory",
        "earnings",
        "price",
        "summaryDetail",
        "defaultKeyStatistics",
        "calendarEvents",
        "assetProfile",
        "secFilings",
        "esgScores",
        "details",
        "insiderHolders",
        "earningsHistory",
        "earningsTrend",
        "industryTrend",
        "indexTrend",
        "sectorTrend",
        "financialsTemplate",
        "incomeStatementHistory",
        "cashflowStatementHistory",
        "balanceSheetHistory",
        "incomeStatementHistoryQuarterly",
        "cashflowStatementHistoryQuarterly",
        "balanceSheetHistoryQuarterly",
        "institutionOwnership",
        "fundOwnership",
        "majorDirectHolders",
        "majorHoldersBreakdown",
        "insiderTransactions",
        "netSharePurchaseActivity",
    ]

    symbol: str
    client: Client
    _data: dict
    _history: pandas.DataFrame

    def __init__(self, client: Client, symbol: str):
        self.client = client
        self.symbol = symbol

    @staticmethod
    def extract_raw(value):
        if isinstance(value, dict):
            if all(key in value for key in ["raw", "fmt"]):
                return value["raw"]
            else:
                return {k: Ticker.extract_raw(v) for k, v in value.items() if v != {}}
        elif isinstance(value, list):
            return [Ticker.extract_raw(v) for v in value]

        return value

    async def _scrape(self):
        async with self.client.session.get(
            f"https://query1.finance.yahoo.com/"
            f"v10/finance/quoteSummary/{quote_plus(self.symbol)}?"
            + urlencode({"modules": ",".join(Ticker._all_modules)})
        ) as resp:
            self._data = Ticker.extract_raw(await resp.json())

        async with self.client.session.get(
            f"https://query1.finance.yahoo.com/"
            f"v8/finance/chart/{quote_plus(self.symbol)}"
        ) as resp:
            self._history = await resp.json()

    async def refresh(self):
        for prop in [
            name
            for name in dir(Ticker)
            if isinstance(getattr(self, name), cached_property)
        ]:
            if prop in self.__dict__:
                delattr(self, prop)

        await self._scrape()
