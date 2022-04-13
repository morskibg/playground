from os.path import join, abspath, dirname
import functools
import time
from typing import Callable, Any
import pandas as pd
import random
import logging

from settings import TABLE_NAME, DATA_DIR_PATH

import loger

module_logger = logging.getLogger('bench_app.utils')

def async_timed():
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped(*args, **kwargs) -> Any:
            module_logger.info(f'starting {func} ')
            start = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                end = time.time()
                total = end - start
                module_logger.info(f'finished {func} in {total:.4f} second(s)')
        return wrapped
    return wrapper

def sync_timed():
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapped(*args, **kwargs) -> Any:
            module_logger.info(f'starting {func} ')
            start = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                end = time.time()
                total = end - start
                module_logger.info(f'finished {func} in {total:.4f} second(s)')
        return wrapped
    return wrapper

def create_queries_list(qty):

    csv_path = abspath(dirname(DATA_DIR_PATH))

    files_to_open = qty // 2000 + (qty % 2000 > 0)   
    suffs = [random.randint(1,99) for _ in range(files_to_open)]
    data_lists = [
        list(
            '\\x' + 
            pd.read_csv(join(csv_path, f'test_data_t{suffix}.csv'), header = None)
            .iloc[:, 0]
            )
        for suffix in suffs
        ]

    urls = sum(data_lists, [])[:qty]
    
    
    
    querues = [f"SELECT * FROM {TABLE_NAME} WHERE url_hash = '{random.choice(urls)}'" for _ in  range(len(urls))]

    return querues

def input_args_parser(argv):

    default_num_queries = 1000
    default_type = 'async'

    kwargs = {'file':argv[0]}
    if len(argv) == 1:
        kwargs['query_numbers'] = default_num_queries
        kwargs['test_type'] = default_type
    else:
        if argv[1] in ['sync', 'async', 'single_query']:
            kwargs['query_numbers'] = default_num_queries
            kwargs['test_type'] = argv[1]
        else:
            try:
                kwargs['query_numbers'] = int(argv[1]) if int(argv[1]) > 0 else default_num_queries
                if len(argv) > 2:
                    if not argv[2] in ['sync', 'async', 'single_query']:
                        raise ValueError()
                    kwargs['test_type'] = argv[2]
                else:
                    kwargs['test_type'] = default_type
            except Exception as e:
                
                print(f"""
                    usage: [BENCHMARK NAME] [QUERY NUMBERS] [TEST TYPE]
                    BENCHMARK NAME - benchmark python script file name
                    QUERY NUMBERS - positive integer, default = 1000
                    TEST TYPE - sync | async | single_query, default = async
                """)
                exit(1)

    return kwargs