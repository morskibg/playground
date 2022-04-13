import sys
import asyncio
import asyncpg
import logging

from utils import async_timed, create_queries_list, input_args_parser
from settings import *

module_logger = logging.getLogger('bench_app.asyncpg')

STATIC_QUERY = f"SELECT * FROM {TABLE_NAME} WHERE url_hash = '\\x5f84272cacad'"

async def fetch_data(pool, query):
    async with pool.acquire() as connection:        
        db_record = await connection.fetchrow(query)
        return db_record       

@async_timed()
async def query_db_synchronously(pool, queries):
    return [await fetch_data(pool, query) for query in queries]   

@async_timed()
async def query_db_concurrently(pool, queries):
    
    coroutines = [fetch_data(pool, query) for query in queries]
    return await asyncio.gather(*coroutines)

@async_timed()
async def query_db_concurrently_with_static(pool):    
    
    coroutines = [fetch_data(pool, STATIC_QUERY) for _ in range(kwargs_dict['query_numbers'])]      
    return await asyncio.gather(*coroutines)



async def main(kwargs_dict):    
   
    queries = create_queries_list(kwargs_dict['query_numbers'])

    async with asyncpg.create_pool(
        host = HOST,
        port = PORT,
        user = DB_USER,
        database = DB_NAME,
        min_size = 50,
        max_size = 50, 
        command_timeout=60
    ) as pool:

     
        if kwargs_dict['test_type'] == 'sync':
            module_logger.info(f'starting non concurent querying ')
            db_records = await query_db_synchronously(pool, queries)
        elif kwargs_dict['test_type'] == 'async':
            module_logger.info(f'starting async querying ')
            db_records = await query_db_concurrently(pool, queries)
        elif kwargs_dict['test_type'] == 'single_query':
            module_logger.info(f'starting querying with single query')
            db_records = await query_db_concurrently_with_static(pool)
            
        else:
            pass

    


if __name__ == '__main__':

    kwargs_dict = input_args_parser(sys.argv)
    
    asyncio.run(main(kwargs_dict))