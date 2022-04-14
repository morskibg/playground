# Test assignment Scaleflex
### by Dimitar Petkov  email: dimityrp@yahoo.com

### status: unfinished

## Usage:
### There are two files which can bu used: 

### 1. asyncpg_bench.py - based on https://magicstack.github.io/asyncpg/current/ 
### it can be run on the terminal after activating virtual environment with two parametters:
###  * query_numbers - non negative integer, specifying number of queries that will be used (defaul = 1000)
###  * test type - string - 'sync', 'async', 'single_query' (default = 'async')
###   -- 'sync' - perfom synchronously query execution wiht connection from pool and random query
###   -- 'async' - perfom concurrently query execution wiht connection from pool and random query
###   -- 'single_query' - perfom concurrently query execution wiht connection from pool with single query

### 2. psycopg_sync_bench.py - based on https://www.psycopg.org/psycopg3/
### it can be run on the terminal after activating virtual environment with single parametter:
###  * query_numbers - non negative integer, specifying number of queries that will be used (defaul = 1000)
### This script uses entirely synchronous approach with single query. 

### 3. psycopg2_thread_bench.py - based on https://www.psycopg.org/docs/
### it can be run on the terminal after activating virtual environment with single parametter:
###  * query_numbers - non negative integer, specifying number of queries that will be used (defaul = 1000)
### This script uses ThreadedConnectionPool, ThreadPoolExecutor  and  single query. 

### 4. shell script file mass_launcher.sh might be executed and will run all 3 bench script with 1000 queries for 50 times