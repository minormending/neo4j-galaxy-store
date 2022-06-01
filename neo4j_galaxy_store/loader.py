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


class GalaxyStoreNeo4j:
    def __init__(self, uri: str, user: str, passwd: str) -> None:
        self.uri: str = uri
        self.user: str = user
        self.passwd: str = passwd
        self.db: str = None  # "samsung_galaxy_store"

    def _get_connection(self) -> Neo4jConnection:
        return Neo4jConnection(uri=self.uri, user=self.user, passwd=self.passwd)

    def setup_constraints(self) -> None:
        with self._get_connection() as conn:
            conn.query(
                "CREATE CONSTRAINT categories IF NOT EXISTS ON (c:Category) ASSERT c._id IS UNIQUE",
                parameters=None,
                db=self.db,
            )
            conn.query(
                "CREATE CONSTRAINT apps IF NOT EXISTS ON (a:App) ASSERT a._id IS UNIQUE",
                parameters=None,
                db=self.db,
            )
            conn.query(
                "CREATE CONSTRAINT reviews IF NOT EXISTS ON (r:Review) ASSERT r._id IS UNIQUE",
                parameters=None,
                db=self.db,
            )

    def populate_categories(self, categories: List[Dict[str, Any]]) -> None:
        with self._get_connection() as conn:
            query: str = """
                UNWIND $rows AS row
                MERGE (c:Category {_id: row._id})
                RETURN count(*) as total
            """
            return conn.query(query, parameters={"rows": categories}, db=self.db)


mongo = GalaxyStoreMongoDB("mongodb://localhost:27017")
neo = GalaxyStoreNeo4j(uri="bolt://localhost:7687", user="neo4j", passwd="test")
neo.setup_constraints()
total: int = neo.populate_categories(list(mongo.categories()))
print(total)
