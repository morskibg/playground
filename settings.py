import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

SECRET_KEY = os.environ.get("SECRET_KEY")
DB_USER = os.environ.get("DB_USER")
HOST = 'localhost'
PORT = 5432
DB_NAME = 'postgres' 
TABLE_NAME = 'elastic_sharding_test'
DATA_DIR_PATH = '/home/tests/data/'
THREADS = 5

STATIC_QUERY = f"SELECT * FROM {TABLE_NAME} WHERE url_hash = '\\x5f84272cacad'"

