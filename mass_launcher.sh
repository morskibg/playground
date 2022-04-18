#!/bin/bash
source /home/dpetkov/playground/venv/bin/activate
for i in {1..2}
do   
   python3 sql_bench.py  
   python3 sql_bench.py  50000
   python3 sql_bench.py  100000
   python3 sql_bench.py  sync
   python3 sql_bench.py  50000 sync
   python3 sql_bench.py  100000 sync
   python3 sql_bench.py  single_query
   python3 sql_bench.py  50000 single_query
   python3 sql_bench.py  100000 single_query
   
done
