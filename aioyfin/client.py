import asyncio
import aiohttp

from .ticker import Ticker


class Client:
    loop: asyncio.AbstractEventLoop
    session: aiohttp.ClientSession

    def __init__(
        self,
        session: aiohttp.ClientSession = None,
        timeout: int = 10,
        loop: asyncio.AbstractEventLoop = None,
    ):
        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop

        self.managed_session = False
        if session is None:
            session = aiohttp.ClientSession(loop=self.loop)
            self.managed_session = True

        self.session = session

    async def get_ticker(self, symbol: str) -> Ticker:
        await (ticker := Ticker(self, symbol)).refresh()
        return ticker

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        if self.managed_session:
            await self.session.close()
