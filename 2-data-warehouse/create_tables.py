import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Delete all existing tables
        
    Parameters: 
    cur: Connection cursor 
    conn: (psycopg2) connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create stagging and analytics tables
        
    Parameters: 
    cur: Connection cursor 
    conn: (psycopg2) connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Drop all tables if exists and them create stagging and analytics tables with proper relations, columns and constricts
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()