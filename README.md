# Test assignment Scaleflex
### by Dimitar Petkov  email: dimityrp@yahoo.com

### status: unfinished

## Usage:
### There are two files which can bu used: 

### 1. sql_bench.py - based on https://magicstack.github.io/asyncpg/current/ 
### it can be run on the terminal after activating virtual environment with two parametters:
###  * query_numbers - non negative integer, specifying number of queries that will be used (defaul = 10000)
###   -- the log file may not be generated for queries under 10000 (async mode) or under 10 sec execution time 
###  * test type - string - 'sync', 'async', 'single_query' (default = 'async')
###   -- 'sync' - perfom synchronously query execution wiht connection from pool and random query
###   -- 'async' - perfom concurrently query execution wiht connection from pool and random query
###   -- 'single_query' - perfom concurrently query execution wiht connection from pool with single query

