#from neo4j_galaxy_store.app import app

from flask import Flask
app = Flask(__name__)

@app.route("/")
def index() -> str:
    return "echo"