"""Tibber RT connection."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import random
from typing import TYPE_CHECKING, Any

from gql import Client

from .exceptions import InvalidLoginError, SubscriptionEndpointMissingError
from .websocket_transport import TibberWebsocketsTransport

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from ssl import SSLContext

    from .home import TibberHome

LOCK_CONNECT = asyncio.Lock()

_LOGGER = logging.getLogger(__name__)

_MAX_BACKOFF_SECONDS = 5 * 60


def _backoff_delay(retry_count: int) -> int:
    """Calculate exponential backoff delay with jitter."""
    return min(random.SystemRandom().randint(1, 30) + retry_count**2, _MAX_BACKOFF_SECONDS)


class TibberRT:
    """Class to handle real time connection with the Tibber api."""

    def __init__(self, access_token: str, timeout: int, user_agent: str, ssl: SSLContext | bool) -> None:
        """Initialize the Tibber connection.

        :param access_token: The access token to access the Tibber API with.
        :param timeout: The timeout in seconds to use when communicating with the Tibber API.
        :param user_agent: User agent identifier for the platform running this. Required if websession is None.
        """
        self._access_token: str = access_token
        self._timeout: int = timeout
        self._user_agent: str = user_agent
        self._ssl_context = ssl

        self._sub_endpoint: str | None = None
        self._homes: list[TibberHome] = []
        self._watchdog_runner: None | asyncio.Task[Any] = None
        self._watchdog_running: bool = False
        self._watchdog_retry_count: int = 0
        self._watchdog_next_test_time: dt.datetime = dt.datetime.min.replace(tzinfo=dt.UTC)

        self.sub_manager: Client | None = None
        self.session: Any | None = None
        self.on_reconnect_cb: Callable[[], Awaitable[None]] | None = None

    async def disconnect(self) -> None:
        """Stop subscription manager.
        This method simply calls the stop method of the SubscriptionManager if it is defined.
        """
        _LOGGER.debug("Stopping subscription manager")
        if self._watchdog_runner is not None:
            _LOGGER.debug("Stopping watchdog")
            self._watchdog_running = False
            self._watchdog_runner.cancel()
            self._watchdog_runner = None
        for home in self._homes:
            home.rt_unsubscribe()
        if self.session is not None:
            await self.sub_manager.close_async()
            self.session = None
        self.sub_manager = None

    async def connect(self) -> None:
        """Start subscription manager."""
        self._create_sub_manager()

        assert self.sub_manager is not None

        async with LOCK_CONNECT:
            if self.subscription_running:
                return
            if self._watchdog_runner is None:
                _LOGGER.debug("Starting watchdog")
                self._watchdog_running = True
                self._watchdog_runner = asyncio.create_task(self._watchdog())
            self.session = await self.sub_manager.connect_async()

    def set_access_token(self, access_token: str) -> None:
        """Set access token."""
        self._access_token = access_token

    def _create_sub_manager(self) -> None:
        if self.sub_endpoint is None:
            raise SubscriptionEndpointMissingError("Subscription endpoint not initialized")
        if self.sub_manager is not None:
            return
        self.sub_manager = Client(
            transport=TibberWebsocketsTransport(
                self.sub_endpoint,
                self._access_token,
                self._user_agent,
                ssl=self._ssl_context,
            ),
        )

    def _check_all_homes_alive(self) -> bool:
        """Check if all homes have active real-time subscriptions."""
        for home in self._homes:
            _LOGGER.debug(
                "Watchdog: Checking if home %s is alive, %s, %s",
                home.home_id,
                home.has_real_time_consumption,
                home.rt_subscription_running,
            )
            if not home.rt_subscription_running:
                return False
            _LOGGER.debug("Watchdog: Home %s is alive", home.home_id)
        return True

    async def _close_sub_manager(self) -> None:
        """Close the subscription manager and reset state."""
        try:
            if self.session is not None and self.sub_manager is not None:
                await self.sub_manager.close_async()
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Error in watchdog close")
        self.session = None
        self.sub_manager = None

    async def _watchdog(self) -> None:
        """Watchdog to keep connection alive."""
        assert self.sub_manager is not None
        assert isinstance(self.sub_manager.transport, TibberWebsocketsTransport)

        await asyncio.sleep(60)

        self._watchdog_retry_count = 0
        self._watchdog_next_test_time = dt.datetime.now(tz=dt.UTC)
        while self._watchdog_running:
            await asyncio.sleep(5)

            if self.sub_manager is not None:
                now = dt.datetime.now(tz=dt.UTC)

                if not self.sub_manager.transport.running or self.sub_manager.transport.reconnect_at <= now:
                    # Transport is dead: log and tear down so the reconnect block below runs.
                    _LOGGER.error("Watchdog: Connection is down, %s", now)
                    await self._close_sub_manager()
                    # sub_manager is now None; falls through to the reconnect block.
                else:
                    # Transport is alive. Skip home-liveness check during the grace period.
                    if now <= self._watchdog_next_test_time:
                        continue

                    if self._check_all_homes_alive():
                        self._watchdog_retry_count = 0
                        _LOGGER.debug("Watchdog: Connection is alive")
                        continue

                    # Transport is alive but some homes stopped receiving data.
                    _LOGGER.warning("Watchdog: Some homes not receiving data, resubscribing stale homes")
                    self._watchdog_next_test_time = now + dt.timedelta(seconds=60)
                    try:
                        await self._resubscribe_stale_homes()
                    except Exception:  # noqa: BLE001
                        _LOGGER.exception("Error resubscribing stale homes")
                    continue

            if not await self._watchdog_reconnect():
                return

    async def _watchdog_reconnect(self) -> bool:
        """Attempt reconnection after transport failure.

        :return: True to continue the watchdog loop, False to stop.
        """
        if not self._watchdog_running:
            _LOGGER.debug("Watchdog: Stopping")
            return False

        if self.on_reconnect_cb is not None:
            try:
                await self.on_reconnect_cb()
            except InvalidLoginError:
                delay_seconds = _backoff_delay(self._watchdog_retry_count)
                self._watchdog_retry_count += 1
                _LOGGER.error(
                    "Access token expired (attempt %s), retrying in %s seconds",
                    self._watchdog_retry_count,
                    delay_seconds,
                )
                await asyncio.sleep(delay_seconds)
                return True
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Error in watchdog reconnect callback")

        try:
            await self._resubscribe_homes()
        except Exception as err:
            delay_seconds = _backoff_delay(self._watchdog_retry_count)
            self._watchdog_retry_count += 1
            _LOGGER.error(
                "Error in watchdog connect, retrying in %s seconds, %s: %s",
                delay_seconds,
                self._watchdog_retry_count,
                err,
                exc_info=self._watchdog_retry_count > 1,
            )
            await self._close_sub_manager()
            await asyncio.sleep(delay_seconds)
        else:
            self._watchdog_retry_count = 0
            _LOGGER.debug("Watchdog: Reconnected successfully")
            # Grace period: give homes 90 s to start receiving data before the
            # liveness check runs.  Sleep only 30 s here; the remaining time is
            # covered by _watchdog_next_test_time.
            now = dt.datetime.now(tz=dt.UTC)
            self._watchdog_next_test_time = now + dt.timedelta(seconds=90)
            await asyncio.sleep(30)
        return True

    async def _resubscribe_homes(self) -> None:
        """Resubscribe to all homes."""
        _LOGGER.debug("Resubscribing to homes")
        await asyncio.gather(*[home.rt_resubscribe(skip_tibber_update=True) for home in self._homes])

    async def _resubscribe_stale_homes(self) -> None:
        """Resubscribe only homes that are not receiving real-time data."""
        stale_homes = [home for home in self._homes if not home.rt_subscription_running]
        if not stale_homes:
            return
        _LOGGER.debug("Resubscribing %d stale home(s)", len(stale_homes))
        await asyncio.gather(*[home.rt_resubscribe(skip_tibber_update=True) for home in stale_homes])

    def add_home(self, home: TibberHome) -> bool:
        """Add home to real time subscription."""
        if home.has_real_time_consumption is False:
            return False
        if home in self._homes:
            return False
        self._homes.append(home)
        return True

    @property
    def subscription_running(self) -> bool:
        """Is real time subscription running."""
        return (
            self.sub_manager is not None
            and isinstance(self.sub_manager.transport, TibberWebsocketsTransport)
            and self.sub_manager.transport.running
            and self.session is not None
        )

    @property
    def sub_endpoint(self) -> str | None:
        """Get subscription endpoint."""
        return self._sub_endpoint

    @sub_endpoint.setter
    def sub_endpoint(self, sub_endpoint: str) -> None:
        """Set subscription endpoint."""
        if self._sub_endpoint == sub_endpoint:
            return
        self._sub_endpoint = sub_endpoint
        if self.sub_manager is not None and isinstance(self.sub_manager.transport, TibberWebsocketsTransport):
            self.sub_manager = Client(
                transport=TibberWebsocketsTransport(
                    sub_endpoint,
                    self._access_token,
                    self._user_agent,
                    ssl=self._ssl_context,
                ),
            )
