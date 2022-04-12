import sys
import asyncio
import asyncpg
import pandas as pd
from utils import async_timed, create_queries_list, input_args_parser



from settings import *


QUERY_QTY = 10
queries = create_queries_list(QUERY_QTY)
print(len(queries))
static_query = f"SELECT * FROM {TABLE_NAME} WHERE url_hash = '\\x5f84272'"
# static_query = f"SELECT * FROM {TABLE_NAME} WHERE url_hash = '\\x5f84272cacad'"

@async_timed()
async def fetch_data(pool, query):
    async with pool.acquire() as connection:        
        db_record = await connection.fetchrow(query)
        return db_record
            
        

@async_timed()
async def query_db_synchronously(pool):
    return [await fetch_data(pool, query) for query in queries]   

@async_timed()
async def query_db_concurrently(pool):
    
    queries_ = [fetch_data(pool, query) for query in queries]
    return await asyncio.gather(*queries_)


async def query_db_concurrently_with_static(pool):
    queries_ = [fetch_data(pool, static_query) for _ in range(QUERY_QTY)]
    print(len(queries_))
    try:
        res = await asyncio.gather(*queries_)
    except Exception as e:
        res = None
        print(f'{e}  ')        
        print(f'{e}  \n Exception from "query_db_concurrently_with_static" at row --->{print(sys.exc_info()[2].tb_lineno)}')
    return res
        

async def foo():
    print('-'*20)

    async with asyncpg.create_pool(
        host = HOST,
        port = PORT,
        user = DB_USER,
        database = DB_NAME,
        min_size = 6,
        max_size = 6, 
        command_timeout=60
    ) as pool:
        # await query_db_synchronously(pool)
        await query_db_concurrently(pool)
        # await query_db_concurrently_with_static(pool)

    


asyncio.run(foo())