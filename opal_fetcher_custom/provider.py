# imports 
from pydantic import List, Any, Field
from opal_common.fetcher.fetch_provider import BaseFetchProvider
from opal_common.fetcher.events import FetcherConfig, FetchEvent


class CustomFetcherConfig(FetcherConfig):
    """
    Config for CustomFetchProvider, inherits from `FetcherConfig`.
    """
    fetcher: str = "CustomFetchProvider"
    query: str = Field(..., description="the query to search in order to fetch the data")


class CustomFetchEvent(FetchEvent):
    """
    When writing a custom provider, you must create a custom FetchEvent subclass.
    """
    fetcher: str = "CustomFetchProvider"
    config: CustomFetcherConfig = None

class CustomFetchProvider(BaseFetchProvider):
    """
    The fetch-provider logic should be logically here.
    """

    def __init__(self, event: CustomFetchEvent) -> None:
        """
        Initialize your fetch provider
        """
        super().__init__(event)

    def parse_event(self, event: FetchEvent) -> CustomFetchEvent:
        """
        Deserializes fetch event type from the general `FetchEvent` to your derived fetch event.
        """
        return CustomFetchEvent(**event.dict(exclude={"config"}), config=event.config)

    # if you require context to cleanup or guard resources, you can use __aenter__() and __aexit__()
    async def __aenter__(self): ...
    async def __aexit__(self, exc_type=None, exc_val=None, tb=None): ...

    async def _fetch_(self):
        """
        The actual logic that you need to implement to fetch.
        """
        user_query: str = self._event.config.query
        self._event: CustomFetchEvent # type cast

        return True # returning fetched results

    async def _process_(self, data: List[Any]):
        """
        Optional processing of the data returned by _fetch_().
        must return json object
        (e.g: a list or a dict that contains only serializable objects).
        """
        self._event: CustomFetchEvent # type cast

        if data:
                # we changing the datato dict that we can be later serialized to json.
            return [dict(d) for d in data]
        else:
            return {}