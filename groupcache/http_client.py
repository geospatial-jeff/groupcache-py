"""
HTTP client for GroupCache peer-to-peer communication.

This client makes requests to other peers in the cluster.
"""

import logging
from typing import Any
import aiohttp

logger = logging.getLogger(__name__)


class GroupCacheHTTPClient:
    """HTTP client for making requests to peer nodes"""

    def __init__(
        self,
        base_path: str = "/_groupcache/",
        timeout: float = 5.0,
        max_retries: int = 1,
    ):
        self.base_path = base_path
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self._session = None

    async def _ensure_session(self):
        """Ensure we have an active session"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def get(self, peer_url: str, group: str, key: str) -> Any | None:
        """
        Request a value from a peer node.

        Args:
            peer_url: Base URL of the peer (e.g., "http://localhost:8080")
            group: Cache group name
            key: Cache key to retrieve

        Returns:
            The cached value if found, None otherwise
        """
        session = await self._ensure_session()
        url = f"{peer_url}{self.base_path}{group}/{key}"

        for attempt in range(self.max_retries + 1):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("value")
                    elif response.status == 404:
                        # Key not found is not an error
                        logger.debug(f"Key not found on peer: {url}")
                        return None
                    else:
                        # Other status codes are errors
                        text = await response.text()
                        logger.warning(
                            f"Peer request failed (attempt {attempt + 1}): "
                            f"{response.status} from {url} - {text}"
                        )

            except aiohttp.ClientError as e:
                logger.warning(
                    f"Peer connection error (attempt {attempt + 1}): "
                    f"{url} - {type(e).__name__}: {e}"
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error requesting from peer: "
                    f"{url} - {type(e).__name__}: {e}"
                )
                break  # Don't retry on unexpected errors

        # All attempts failed
        return None

    async def close(self):
        """Close the HTTP session"""
        if self._session and not self._session.closed:
            await self._session.close()
