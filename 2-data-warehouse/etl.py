import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time


def load_staging_tables(cur, conn):
    """
    Load data from S3 and store it into stagging tables
    
    Parameters: 
    cur: Connection cursor 
    conn: (psycopg2) connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Select and tranform data from stagging tables and load into analytics tables
        
    Parameters: 
    cur: Connection cursor 
    conn: (psycopg2) connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Load data from S3 and store it into stagging tables and them select and tranform data from stagging tables and load into analytics tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
     
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()