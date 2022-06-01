from typing import Any, Dict
from neo4j import GraphDatabase, BoltDriver, Session
from pymongo.database import Database
from pymongo.mongo_client import MongoClient


class Neo4jConnection:
    def __init__(self, uri: str, user: str, passwd: str) -> None:
        self.uri: str = uri
        self.user: str = user
        self.passwd: str = passwd
        self.driver: BoltDriver = None

    def __enter__(self) -> BoltDriver:
        self.driver: BoltDriver = GraphDatabase.driver(self.uri, auth=(self.user, self.passwd))
        return self.driver

    def __exit__(self) -> None:
        if self.driver:
            self.driver.close()
    
    def query(self, query: str, parameters: Dict[str, Any], db: str) -> None:
        session: Session = None
        try:
            session: Session = self.driver.session(database=db)
            return list(session.run(query, parameters))
        finally:
            session.close()


class MongoConnection:
    def __init__(self, uri: str) -> None:
        self.uri: str = uri
        self.client: MongoClient = None

    def __enter__(self) -> MongoClient:
        self.client: MongoClient = MongoClient(self.uri)
        return self.client

    def __exit__(self) -> None:
        if self.client:
            self.client.close()