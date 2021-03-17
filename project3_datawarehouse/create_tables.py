import configparser
import psycopg2
from queries import create_tables_main, create_tables_staging, drop_table_queries

def drop_tables(cursor, conn):
    """
        Drop the stating and main tables in Redshift
    """
    print('Droping tables')
    for query in drop_table_queries:
        cursor.execute(query)
        conn.commit()

def create_staging_tables(cursor, conn):
    """
        Create staging tables
    """
    print("Creating Staging Tables")
    for query in create_tables_staging:
        cursor.execute(query)
        conn.commit()

def create_main_tables(cursor, conn):
    """
        Create Fact and Dimnesion tables
    """
    print("Creating Fact and Dimension Tables")

    for query in create_tables_main:
        cursor.execute(query)
        conn.commit()

def main():
    """
    Set up the database tables, create needed tables with the appropriate columns and constricts
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Establish Connection to cluster
    '''
        Host = Cluster Identifier
        DBName = Name of Database
        Password = Password
        Port = Usually 5439 for Redshift unless Firewall Settings allow different port
    '''
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to the cluster')

    #Dropping Tables if the are already presesnt
    drop_tables(cur, conn)

    #Creating Staging Tables
    create_staging_tables(cur, conn)

    #Creating Fact and Dimension Tables comforming to the STAR SCHEMA
    create_main_tables(cur, conn)

    print("Successfully created/reset tables")
    conn.close()


if __name__ == "__main__":
    main()