import configparser
import psycopg2
from queries import copy_commands, insert_commands, analytic_queries


def load_staging_tables(cur, conn):
    """
    Load data from files stored in S3 to the staging tables
    The sql queries to do so are in the queries.py file
    """
    print('Inserting data from json files stored in S3 buckets into staging tables')
    for query in copy_commands:
        print('Running ' + query)
        cur.execute(query)
        conn.commit()


def insert_into_tables(cur, conn):
    """
    Extract and Transform data from staging tables and load it into the dimenson and fact tables
    The queries to do so are present in the queries.py file
    """
    print('Inserting data from staging tables into analytics tables')
    for query in insert_commands:
        print('Running ' + query)
        cur.execute(query)
        conn.commit()


def test_tables(cur, conn):
    """
    Test the Pipeline results using the queries provided by the Analystics Team.
    The queries so are present in the queries.py file
    """
    for query in copy_commands:
        cur.execute(query)
        conn.commit()

def main():
    """
    Function for the main ETL Pipeline with the following steps.
    Step1: Load data from S3 into staging tables
    Step2: Extract and Transform the data to fit the schema for dimension and fact tables and load the data
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #EXTRACT 
    load_staging_tables(cur, conn)

    #TRANSFORM AND LOAD
    insert_into_tables(cur, conn)

    #TEST STAR SCHEMA
    test_tables(cur, conn)

    print("ETL Successful")

    conn.close()


if __name__ == "__main__":
    main()