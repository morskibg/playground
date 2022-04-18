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
SQL_LOG_PATH = '/home/sql/log/'
BENCH_LOG_PATH = '/home/dpetkov/playground/logs'
ASYNC_POOL_MAX = 100
ASYNC_POOL_MIN = 100
CSV_DATA_LENGTH = 2000

