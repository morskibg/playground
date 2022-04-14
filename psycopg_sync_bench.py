import sys
import psycopg
import logging
import time
from utils import sync_timed, input_args_parser
from settings import *
import loger

module_logger = logging.getLogger('bench_app.psycopg_sync')


def query_db_synchronously(connection, qty):
    with connection.cursor() as cur:    
        db_data = []    
        for _ in range(1, qty + 1):
            cur.execute(STATIC_QUERY)
            db_data.append(cur.fetchone())
        return db_data           
            

def main(kwargs_dict):
    with psycopg.connect(
        
        user = DB_USER,    
        host = HOST,
        port = PORT,
        dbname = DB_NAME    
    ) as connection:

        t1 = time.perf_counter()
       
        db_records = query_db_synchronously(connection, kwargs_dict['query_numbers'])
        t2 = time.perf_counter()
        module_logger.info(f'Finished in {t2-t1:.4f} seconds for {len(db_records)} records.')

if __name__ == '__main__':
    kwargs_dict = input_args_parser(sys.argv)
   
    main(kwargs_dict)