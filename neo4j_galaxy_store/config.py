# Statement for enabling the development environment
DEBUG = True

# Define the application directory
import os
BASE_DIR = os.path.abspath(os.path.dirname(__file__))  

MONGO_DATABASE_URI = 'mongodb://localhost:27017'
MONGO_DATABASE_CONNECT_OPTIONS = {}

NEO4J_DATABASE_URI = 'bolt://localhost:7687'
NEO4J_DATABASE_CONNECT_OPTIONS = {'user':"neo4j", 'passwd':"test"}

# Application threads. A common general assumption is
# using 2 per available processor cores - to handle
# incoming requests using one and performing background
# operations using the other.
THREADS_PER_PAGE = 2

# Enable protection agains *Cross-site Request Forgery (CSRF)*
CSRF_ENABLED     = True

