
from typing import Any
from .connection import MongoConnection, Neo4jConnection


with Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", passwd="test") as conn:
    pass


class MongoLoader:
    def __init__(self, uri: str) -> None:
        self.uri = uri

    def categories(self) -> Any:
        with MongoConnection(self.uri) as conn:
            pass