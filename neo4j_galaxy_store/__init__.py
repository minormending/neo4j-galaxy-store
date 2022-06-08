__version__ = "0.1.0"

from flask import Flask
app = Flask(__name__)

import neo4j_galaxy_store.config as config
app.config.from_object(config)

import neo4j_galaxy_store.routes