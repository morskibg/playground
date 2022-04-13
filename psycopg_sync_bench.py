import sys
import psycopg
import logging
from utils import sync_timed, input_args_parser
from settings import *
import loger

module_logger = logging.getLogger('bench_app.psycopg_sync')

STATIC_QUERY = f"SELECT * FROM {TABLE_NAME} WHERE url_hash = '\\x5f84272cacad'"

@sync_timed()
def query_db_synchronously(connection, qty):
    with connection.cursor() as cur:        
        for _ in range(1, qty + 1):
            cur.execute(STATIC_QUERY)
            db_record = cur.fetchone()
            
            

def main(kwargs_dict):
    with psycopg.connect(
        
        user = DB_USER,    
        host = HOST,
        port = PORT,
        dbname = DB_NAME    
    ) as connection:

        module_logger.info(f'starting sync querying with single query')
        query_db_synchronously(connection, kwargs_dict['query_numbers'])

if __name__ == '__main__':
    kwargs_dict = input_args_parser(sys.argv)
   
    main(kwargs_dict)