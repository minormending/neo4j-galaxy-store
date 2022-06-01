from typing import Any, Dict, Iterable, List
from pymongo.cursor import Cursor

from connection import MongoConnection, Neo4jConnection


class GalaxyStoreMongoDB:
    def __init__(self, uri: str) -> None:
        self.uri = uri

    def categories(self) -> Iterable[Dict[str, Any]]:
        yield from self._get_all_collection("categories")

    def apps(self) -> Iterable[Dict[str, Any]]:
        yield from self._get_all_collection("apps", batch_size=1000)

    def reviews(self) -> Iterable[Dict[str, Any]]:
        yield from self._get_all_collection("reviews")

    def _get_all_collection(
        self, collection: str, batch_size: int = 5000
    ) -> Iterable[Dict[str, Any]]:
        with MongoConnection(self.uri, "samsung-galaxy-store") as database:
            cursor: Cursor = database[collection].find({}, batch_size=batch_size)
            yield from cursor


