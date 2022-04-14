#!/bin/bash
source /home/dpetkov/playground/venv/bin/activate
for i in {1..50}
do
   python3 psycopg2_thread_bench.py 1000
   python3 psycopg_sync_bench.py 5000
   python3 asyncpg_bench.py 5000 single_query
   
done
