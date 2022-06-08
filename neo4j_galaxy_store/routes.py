from neo4j_galaxy_store import app

@app.route("/")
def index() -> str:
    return {"value": app.config["MONGO_DATABASE_URI"] }