import sys
import time
from psycopg2 import pool
import concurrent.futures
import logging
from utils import sync_timed, input_args_parser
from settings import *

module_logger = logging.getLogger('bench_app.psycopg2_thread')

def query_db_synchronously(threaded_connection_pool, qty):
    with threaded_connection_pool.getconn().cursor() as cur:
        db_data = []
        for _ in range( qty ):            
            cur.execute(STATIC_QUERY)
            db_data.append(cur.fetchone()) 
        return db_data


def main(kwargs_dict):

    threaded_connection_pool = pool.ThreadedConnectionPool(1, 20,
        user = DB_USER,    
        host = HOST,
        port = PORT,
        dbname = DB_NAME 
    )

    t1 = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        res_list = []
        args = [threaded_connection_pool, kwargs_dict['query_numbers'] // THREADS]
        results = [executor.submit(query_db_synchronously, *args) for _ in range(THREADS)]
        [res_list.append(r.result()) for r in concurrent.futures.as_completed(results)]

    db_records = sum(res_list,[])
    
    t2 = time.perf_counter()
    module_logger.info(f'Finished in {t2-t1:.4f} seconds for {len(db_records)} records')

if __name__ == '__main__':
    kwargs_dict = input_args_parser(sys.argv)
    main(kwargs_dict)
