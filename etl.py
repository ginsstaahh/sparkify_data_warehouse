import boto3
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')
CLUSTER_IDENTIFIER = config.get('AWS','CLUSTER_IDENTIFIER')

def load_staging_tables(cur, conn):
    """Loads data from S3 into staging tables
    Arguments:
        cur {object} - cursor used to interact with the database
        conn {object} - session to connect to the database"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data from staging tables into dimension and fact tables
    Arguments:
        cur {object} - cursor used to interact with the database
        conn {object} - session to connect to the database"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def set_cluster_endpoint():
    """Sets cluster endpoint in the configuration file"""
    redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    cluster_props = redshift.describe_clusters(
                                ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    config.set('CLUSTER', 'DWH_HOST', cluster_props['Endpoint']['Address'])
        
def main():
    """Creates a connection to the sparkify cluster and transfers data from S3"""
    set_cluster_endpoint()
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))

    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()