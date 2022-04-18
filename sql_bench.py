import sys
import asyncio
import asyncpg
import time


from utils import create_queries_list, input_args_parser, clear_bench_log_folder, parse_bench_log
from settings import *
from loger import get_logger

module_logger = get_logger('sql_bench') 


async def fetch_data(pool, query):
    """
    Acuire connection from pool and fetch sinle row from db.
    Input - pool - asyncpg.create_pool instance
          - query - prepeared query for execution
    """
    async with pool.acquire() as connection:        
        db_record = await connection.fetchrow(query)
        return db_record       


async def query_db_synchronously(pool, queries):
    """
    Synchronously fetching data. 
    Input - pool - asyncpg.create_pool instance
          - queries - list of prepeared query for execution
    Output - list of fetched data
    """
    return [await fetch_data(pool, query) for query in queries]   


async def query_db_concurrently(pool, queries):
    """
        Concurrently  fetching data. 
        Input - pool - asyncpg.create_pool instance
              - queries - list of prepeared query for execution
        Output - list of fetched data
    """
    
    coroutines = [fetch_data(pool, query) for query in queries]
    return await asyncio.gather(*coroutines)


async def main(kwargs_dict):    
   
    queries = create_queries_list(kwargs_dict['query_numbers'])
    

    async with asyncpg.create_pool(
        host = HOST,
        port = PORT,
        user = DB_USER,
        database = DB_NAME,
        min_size = ASYNC_POOL_MIN,
        max_size = ASYNC_POOL_MAX, 
        command_timeout=60
    ) as pool:

        t1 = time.perf_counter()

        if kwargs_dict['test_type'] == 'sync':            
            db_records = await query_db_synchronously(pool, queries)

        elif kwargs_dict['test_type'] == 'async':            
            db_records = await query_db_concurrently(pool, queries)

        else:   
            # enter here in case of 'single_query' - take only first query from created list and use only it.           
            queries = [queries[0] for _ in range(kwargs_dict['query_numbers'])]          
            db_records = await query_db_concurrently(pool, queries)            
        
        t2 = time.perf_counter()
        
        print(f'Queries complete for {t2-t1:.4f} seconds. \nProcessing results ...')
        
        for _ in range(3):
            time.sleep(10)
            try:
                parsed_result = parse_bench_log()                
                module_logger.info(f"elapsed_time(sec);{t2-t1:.4f} # queries;{len(db_records)} # test_type;{kwargs_dict['test_type']} # pool_connection_size;{ASYNC_POOL_MAX} # agregated_time_SELECT;{parsed_result.SELECT:.4f} # agregated_time_BIND;{parsed_result.BIND:.4f} # agregated_time_PARSE;{parsed_result.PARSE:.4f} # agregated_time_RESET;{parsed_result.RESET:.4f}")

                break
            except:
                pass
        else:
            print('No file ! \nPlease run again with more queries (recomended: 10_000).')

if __name__ == '__main__':

    clear_bench_log_folder(SQL_LOG_PATH)
    kwargs_dict = input_args_parser(sys.argv)
    print(f"Start for {kwargs_dict['query_numbers']} queries in {kwargs_dict['test_type']} mode ")
    asyncio.run(main(kwargs_dict))
    