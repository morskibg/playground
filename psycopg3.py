import psycopg
import asyncio
from settings import *
import sys

async def main():
    # print(sys.path)
    async with await psycopg.AsyncConnection.connect(
        user = DB_USER,    
        host = HOST,
        port = PORT,
        dbname = DB_NAME  
    ) as aconn:

        async with aconn.cursor() as acur:
            
            # await acur.execute(f"SELECT * FROM {TABLE_NAME} limit 10") 
            await acur.execute(f"SELECT * FROM {TABLE_NAME} WHERE url_hash = '\\x5f84272cacad'") 
            # await acur.execute(f"SELECT * FROM {TABLE_NAME} WHERE url_hash = decode('5f84272cacad','hex')") 
            # await acur.execute("SELECT * FROM elastic_sharding_test WHERE url_hash = decode('e3ab91ddffa3', 'hex')")
            r = await acur.fetchone()
            print(r)
            # will return (1, 100, "abc'def")
            # async for record in acur:
            #     print(record)
asyncio.run(main())


# with psycopg.connect(
    
#     user = DB_USER,    
#     host = HOST,
#     port = PORT,
#     dbname = DB_NAME   
# ) as conn:

   
    # with conn.cursor() as cur:

       
    #     cur.execute(f"SELECT * FROM {TABLE_NAME} limit 10")
    #     cur.fetchone()
       
    #     for record in cur:
    #         print(record)

        
    #     conn.commit()

       