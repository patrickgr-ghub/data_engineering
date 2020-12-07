import psycopg2
from connection import *

from sql_queries import create_table_queries, drop_table_queries


# Table Management & Connection functions

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


# Create the connection & launch "create_tables.py" functions    
    
if __name__ == "__main__":
    main()
