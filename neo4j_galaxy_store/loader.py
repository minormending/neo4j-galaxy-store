from typing import Any, Dict, Iterable, List, Set
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
            # conn.query(f"CREATE DATABASE {self.db} IF NOT EXISTS", parameters=None, db=None)
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

    def populate_categories(self, categories: List[Dict[str, Any]]) -> int:
        return self._insert_model("Category", "_id", categories)

    def populate_apps(self, apps: List[Dict[str, Any]]) -> None:
        total: int = 0
        total_developers: int = 0

        batch_size: int = 1000
        for idx_start in range(0, len(apps), batch_size):
            idx_end = min(idx_start + batch_size, len(apps))
            batch: List[Dict[str, Any]] = apps[idx_start:idx_end]

            developers: List[Dict[str, str]] = [
                app["developer"] for app in batch if app.get("developer")
            ]
            total_developers += (
                self._insert_model("Developer", "name", developers) if developers else 0
            )

            relationship: str = f"""
                WITH row, app
                MATCH (developer:Developer {{name: row.developer.name}})
                MERGE (developer)-[:OWNS]->(app)

                WITH row, app
                MATCH (category:Category {{_id: row.category_id}})
                MERGE (app)-[:IN]->(category)

                WITH row, app
                UNWIND row.permissions AS p
                MERGE (permission:Permission {{name: p}})
                MERGE (app)-[:REQUIRES]->(permission)
            """

            total += self._insert_model("App", "_id", batch, relationship, ignore=["developer", "category_id", "permission"])

        return total, total_developers

    def _insert_model(
        self,
        model_name: str,
        merge_field: str,
        models: List[Dict[str, str]],
        relationship: str = "",
        ignore: List[str] = [],
    ) -> None:
        with self._get_connection() as conn:
            name: str = model_name.lower()
            fields: str = self._build_update_query(name, models, ignore)
            query: str = f"""
                UNWIND $rows AS row
                MERGE ({name}:{model_name} {{{merge_field}: row.{merge_field}}})
                ON CREATE SET {fields}
                ON MATCH SET {fields}
                {relationship}
                RETURN count(distinct {name}) as total
            """
            print(query)
            result: List[Dict[str, Any]] = conn.query(
                query, parameters={"rows": models}, db=self.db
            )
            return result[0]["total"]

    def _build_update_query(
        self, model_name: str, models: List[Dict[str, Any]], ignore: List[str]
    ) -> str:
        keys: Set[str] = set(
            key for model in models for key in model.keys() if key not in ignore
        )
        return ", ".join(f"{model_name}.{key} = row.{key}" for key in keys)


mongo = GalaxyStoreMongoDB("mongodb://localhost:27017")
neo = GalaxyStoreNeo4j(uri="bolt://localhost:7687", user="neo4j", passwd="test")
neo.setup_constraints()
total: int = neo.populate_categories(list(mongo.categories()))
print("categories:", total)

total: int = neo.populate_apps(list(mongo.apps()))
print("apps:", total)
