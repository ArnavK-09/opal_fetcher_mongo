# Imports
from typing import Optional, List
from pydantic import BaseModel, Field
import motor.motor_asyncio
from opal_common.fetcher.fetch_provider import BaseFetchProvider
from opal_common.fetcher.events import FetcherConfig, FetchEvent
from opal_common.logger import logger


# Configuration for MongoDB connection
class MongoConnectionParams(BaseModel):
    """
    Args:
        BaseModel (_type_): _description_

    MongoDB connection parameters....
    """
    uri: str = Field(..., description="MongoDB connection string")
    database: str = Field(..., description="Database name")
    collection: str = Field(..., description="Collection name")
    query: dict = Field(default={}, description="Query to fetch data")
    fetch_one: bool = Field(False, description="Fetch only one document")


# Configuration for the MongoDB Fetcher
class MongoFetcherConfig(FetcherConfig):
    """
    Config for MongoFetchProvider, instance of `FetcherConfig`.
    """
    fetcher: str = "MongoFetchProvider"
    connection_params: Optional[MongoConnectionParams] = Field(
        ..., description="MongoDB connection parameters for fetcher...")


# Event shape for the MongoDB Fetch Provider
class MongoFetchEvent(FetchEvent):
    """
    A FetchEvent shape for the MongoDB Fetch Provider.
    """
    fetcher: str = "MongoFetchProvider"
    config: MongoFetcherConfig = None


# MongoDB Fetch Provider implementation
class MongoFetchProvider(BaseFetchProvider):

    def __init__(self, event: MongoFetchEvent) -> None:
        """
        Args:
            event (MongoFetchEvent): _description_
        """
        if event.config is None:
            event.config = MongoFetcherConfig()
        super().__init__(event)
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            event.config.connection_params.uri)
        self.collection = self.client[event.config.connection_params.database][
            event.config.connection_params.collection]

    def parse_event(self, event: FetchEvent) -> MongoFetchEvent:
        """
        Args:
            event (FetchEvent): _description_

        Returns:
            MongoFetchEvent: _description_
        """
        return MongoFetchEvent(**event.dict(exclude={"config"}),
                               config=event.config)

    async def __aenter__(self):
        self._event: MongoFetchEvent  # type casting
        return self

    async def __aexit__(self, exc_type=None, exc_val=None, tb=None):
        """
        Args:
            exc_type (_type_, optional): _description_. Defaults to None.
            exc_val (_type_, optional): _description_. Defaults to None.
            tb (_type_, optional): _description_. Defaults to None.
        """
        self.client.close()

    async def _fetch_(self):
        self._event: MongoFetchEvent  # type casting

        if self._event.config is None:
            logger.warning(
                "incomplete fetcher config: MongoDB data entries require a query to specify what data to fetch!"
            )
            return

        logger.debug(
            f"{self.__class__.__name__} fetching from {self._event.config.connection_params.uri}"
        )

        if self._event.config.connection_params.fetch_one:
            document = await self.collection.find_one(
                self._event.config.connection_params.query)
            return [document] if document else []
        else:
            cursor = self.collection.find(
                self._event.config.connection_params.query)
            documents = await cursor.to_list(length=None)
            return documents

    async def _process_(self, records: List[dict]):
        """
        Args:
            records (List[dict]): _description_
        """
        self._event: MongoFetchEvent  # type casting

        # when fetch_one is true, we want to return a dict (and not a list)
        if self._event.config.connection_params.fetch_one:
            if records and len(records) > 0:
                return records[0]
            else:
                return {}
        else:
            return records
