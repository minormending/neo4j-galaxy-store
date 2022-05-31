
from typing import Any
from .connection import MongoConnection, Neo4jConnection


with Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", passwd="test") as conn:
    pass

