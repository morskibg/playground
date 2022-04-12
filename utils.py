from os.path import join, abspath, dirname
import functools
import time
from typing import Callable, Any
import pandas as pd
import random
from settings import TABLE_NAME, DATA_DIR_PATH

def async_timed():
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped(*args, **kwargs) -> Any:
            print(f'starting {func} with args {args} {kwargs}')
            start = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                end = time.time()
                total = end - start
                print(f'finished {func} in {total:.4f} second(s)')
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
    
    # return [f"SELECT * FROM {TABLE_NAME} WHERE url_hash = '\\x5f84272cacad'"]
    return querues

def input_args_parser(argv):

    kwargs = {'file':argv[0]}
    if len(argv) == 1:
        kwargs['query_numbers'] = 1000
    else:
        try:
            kwargs['query_numbers'] = int(argv[1]) if int(argv[1]) > 0 else 1000

        except:
            print(f"""
            usage: [BENCHMARK NAME] [QUERY NUMBERS]
            BENCHMARK NAME - benchmark python script file name
            QUERY NUMBERS - positive integer, default = 1000
            """)

    return kwargs