import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Get credential information from dwh.cfg file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # setup connection to redshift data warehouse
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # drop all tables (reset environment)
    drop_tables(cur, conn)
    
    # Create staging tables and fact, dimension tables
    create_tables(cur, conn)

    # close connection
    conn.close()


if __name__ == "__main__":
    main()