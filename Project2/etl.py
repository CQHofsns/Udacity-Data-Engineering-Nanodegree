import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Get credential information from dwh.cfg file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # setup connection to redshift data warehouse
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load (or copy) data from S3 to staging tables
    load_staging_tables(cur, conn)
    
    # insert data from staging tables to fact and dimension tables
    insert_tables(cur, conn)

    # close connection
    conn.close()


if __name__ == "__main__":
    main()