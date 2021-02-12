from __future__ import annotations
from typing import ClassVar, Optional, List, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client

from bs4 import BeautifulSoup, SoupStrainer
from functools import cached_property
from urllib.parse import urlencode, quote_plus
import pandas as pd
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
    _history: pd.DataFrame

    def __init__(self, client: Client, symbol: str):
        self.client = client
        self.symbol = symbol

    @staticmethod
    def extract_raw(value):
        if isinstance(value, dict):
            if 'maxAge' in value:
                del value['maxAge']
            if all(key in value for key in ["raw", "fmt"]):
                return value["raw"]
            else:
                return {k: Ticker.extract_raw(v) for k, v in value.items() if v != {}}
        elif isinstance(value, list):
            return [Ticker.extract_raw(v) for v in value]

        return value

    async def history(self, **kwargs):
        async with self.client.session.get(
            f"https://query1.finance.yahoo.com/"
            f"v8/finance/chart/{quote_plus(self.symbol)}"
        ) as resp:
            return await resp.json()

    async def _scrape(self):
        async with self.client.session.get(
            f"https://query1.finance.yahoo.com/"
            f"v10/finance/quoteSummary/{quote_plus(self.symbol)}?"
            + urlencode({"modules": ",".join(Ticker._all_modules)})
        ) as resp:
            self._data = Ticker.extract_raw((await resp.json())['quoteSummary'])

        if self._data['error'] is not None:
            raise self._data['error']

        self._data = self._data['result'][0]

        self._history = await self.history()


    async def refresh(self):
        for prop in [
            name
            for name in dir(Ticker)
            if isinstance(getattr(Ticker, name), cached_property) and name in self.__dict__
        ]:
            delattr(self, prop)

        await self._scrape()

    @property
    def major_holders_breakdown(self) -> Dict[str, float]:
        return self._data['major_holders_breakdown']

    @cached_property
    def top_institutional_holders(self) -> pd.DataFrame:
        institutions = self._data['institutionOwnership']['ownershipList']
        institutions = pd.DataFrame(institutions)
        institutions['reportDate'] = institutions['reportDate'].astype('datetime64[s]')
        return institutions

    @cached_property
    def top_mutual_fund_holders(self) -> pd.DataFrame:
        funds = self._data['fundOwnership']['ownershipList']
        funds = pd.DataFrame(funds)
        funds['reportDate'] = funds['reportDate'].astype('datetime64[s]')
        return funds

    @cached_property
    def insider_holders(self) -> pd.DataFrame:
        holders = self._data['insiderHolders']['holders']
        holders = pd.DataFrame(holders)
        holders['latestTransDate'] = holders['latestTransDate'].astype('datetime64[s]')
        holders['positionDirectDate'] = holders['positionDirectDate'].astype('datetime64[s]')
        holders['positionIndirectDate'] = holders['positionIndirectDate'].astype('datetime64[s]')
        holders.loc[~(holders['positionDirectDate'].isnull()) & holders['positionDirect'].isnull(), 'positionDirect'] = 0
        holders.loc[~(holders['positionIndirectDate'].isnull()) & holders['positionIndirect'].isnull(), 'positionIndirect'] = 0
        return holders[['name', 'transactionDescription', 'positionDirectDate', 'positionDirect', 'positionIndirectDate', 'positionIndirect']]

    @cached_property
    def insider_transactions(self) -> pd.DataFrame:
        transactions = self._data['insiderTransactions']['transactions']
        transactions = pd.DataFrame(transactions)
        transactions['startDate'] = transactions['startDate'].astype('datetime64[s]')
        transactions['ownership'].replace({'I': 'Indirect', 'D': 'Direct'}, inplace=True)
        transactions.rename(columns={'transactionText': 'transaction', 'startDate': 'date'}, inplace=True)
        return transactions[['filerName', 'filerRelation', 'filerUrl', 'transaction', 'ownership', 'value', 'date', 'shares']]
