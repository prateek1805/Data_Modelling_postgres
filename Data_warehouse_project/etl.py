import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)                                               # We are copying the data from S3 to staging tables
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)                           # After running these the data from staging table will be move to analytics table
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

# Building the connection to the database in cluster which will be used by other function to perform the queries in redshift cluster
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)                                         # Calling the load_staging_table function               
    insert_tables(cur, conn)                                               # Calling the insert_table function              

    conn.close()


if __name__ == "__main__":
    main()                                         