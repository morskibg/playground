from os.path import join, abspath, dirname
from os import  remove, listdir, walk
from collections import namedtuple
import functools
import time
import plotly.express as px

import pandas as pd
import random


from settings import TABLE_NAME, DATA_DIR_PATH, CSV_DATA_LENGTH, SQL_LOG_PATH

import loger

# module_logger = logging.getLogger('bench_app.utils')

def async_timed():
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            # print(f'starting {func} ')
            start = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                end = time.time()
                total = end - start
                # print(f'finished {func} in {total:.4f} second(s)')
        return wrapped
    return wrapper

def sync_timed():
    def wrapper(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs) :
            # print(f'starting {func} ')
            start = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                end = time.time()
                total = end - start
                # print(f'finished {func} in {total:.4f} second(s)')
        return wrapped
    return wrapper

def create_queries_list(qty):  
    """
        Create 'select' queries list with random url_hash-es picked from csv files
        Input - int > 0 - number of queries to be created
        Output  - list of created queries 
    """  

    csv_path = abspath(dirname(DATA_DIR_PATH))

    files_to_open = qty // CSV_DATA_LENGTH + (qty % CSV_DATA_LENGTH > 0)   
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
    """
        Input CLI parametters parser. If are not specified, default values will be applied.
        Input - args from input
        Return - dictionary with selected parameters
    """

    default_num_queries = 10000
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
                print(e)
                
                print(f"""
                    usage: [BENCHMARK NAME] [QUERY NUMBERS] [TEST TYPE]
                    BENCHMARK NAME - benchmark python script file name
                    QUERY NUMBERS - positive integer, default = {default_num_queries}
                    TEST TYPE - sync | async | single_query, default = {default_type}
                """)
                exit(1)

    return kwargs

def get_log_csv_df(path = SQL_LOG_PATH):  
    """
    Create dataframe from logcsv files created from postgresql server for every bench file invocation.
    Input - path to postgresql log directory
    Output - dataframe from all available csv files.
    """     
    
    df_list = []    
    for _, _, files in walk(path):
        for filename in files:
            if filename.endswith(".csv") & (filename.find("~") == -1):
                try:
                    temp_df = pd.read_csv(join(SQL_LOG_PATH, filename), header=None, low_memory=False)
                except Exception as e:                    
                    continue
                df_list.append(temp_df)                

    bench_df = pd.concat([x for x in df_list], ignore_index=True) if len(df_list) else None
    return bench_df
       

def clear_bench_log_folder(path):
    """
    Delete all csv files in postgresql csvlog directory. Exececuted between every bench call.
    Input - path to postgresql log directory.
    Output - none
    """

    for f in listdir(path):             
        remove(join(path, f))

def parse_bench_log():
    """
    Parse and aggregate dataframe from csvlog files for benchlog
    Input - none
    Output - parsed dataframe
    """

    try:
        bench_df = get_log_csv_df()
        bench_df = bench_df[[0, 1, 2, 3, 4, 7, 8, 13]]
        bench_df.columns = ['log_time','user_name','database_name','process_id',
                            'connection_from','command_tag','session_start_time','message_text']

        bench_df = bench_df[bench_df['command_tag'].isin(['PARSE', 'BIND', 'SELECT','RESET'])]
        pattern = r"(?<=duration: )(\d+.\d+)"
        
        bench_df['parsed_duration']=bench_df['message_text'].str.extract(pattern).astype(float)
        Result = namedtuple('Result',['PARSE', 'BIND', 'SELECT','RESET']) 
        parsed_result = Result(
            bench_df['parsed_duration'][bench_df['command_tag'] == 'PARSE'].sum() / 1000,
            bench_df['parsed_duration'][bench_df['command_tag'] == 'BIND'].sum() / 1000,
            bench_df['parsed_duration'][bench_df['command_tag'] == 'SELECT'].sum() / 1000,
            bench_df['parsed_duration'][bench_df['command_tag'] == 'RESET'].sum() / 1000,
            )
        
        return parsed_result
    except:
        return None

