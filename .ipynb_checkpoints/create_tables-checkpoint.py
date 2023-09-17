import boto3
import configparser
import json
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_schemadisplay import create_schema_graph

# Load paramaters from configuration file
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')
CLUSTER_IDENTIFIER = config.get('AWS','CLUSTER_IDENTIFIER')

IAM_ROLE_NAME = config.get('IAM_ROLE','IAM_ROLE_NAME')

DWH_HOST            = config.get('CLUSTER','DWH_HOST')
DWH_NAME            = config.get('CLUSTER','DWH_NAME')
DWH_USER            = config.get('CLUSTER','DWH_USER')
DWH_PASSWORD        = config.get('CLUSTER','DWH_PASSWORD')
DWH_PORT            = config.get('CLUSTER','DWH_PORT')


def drop_tables(cur, conn):
    """Drops each table using the queries in `drop_table_queries` list.
    Arguments:
        cur {object} - cursor used to interact with the database
        conn {object} - session to connect to the database"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates each table using the queries in `create_table_queries` list.
    Arguments:
        cur {object} - cursor used to interact with the database
        conn {object} - session to connect to the database"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        

def create_erd():
    redshift_credentials = {
        'username': DWH_USER,
        'password': DWH_PASSWORD,
        'host': DWH_HOST,
        'port': DWH_PORT,
        'database': DWH_NAME
    }
    redshift_url = URL(drivername='redshift+psycopg2', **redshift_credentials)
    engine = create_engine(redshift_url)
    metadata = MetaData()
    metadata.reflect(bind=engine)
    
    # Generate the ERD
    graph = create_schema_graph(
        metadata=metadata,
        show_datatypes=True,
        show_indexes=True,
        rankdir='LR',  # From left to right (LR) or top to bottom (TB)
        concentrate=False
    )
    # Save the ERD as a PNG file
    graph.write_png('erd.png')
    
    
def create_iam_role():
    """Creates an IAM resource"""
    iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY,
                 aws_secret_access_key=SECRET)
    try:
        print('Creating a new IAM Role')
        iam.create_role(
            Path = '/',
            RoleName = IAM_ROLE_NAME,
            Description = 'Allows redshift clusters to call on AWS services',
            AssumeRolePolicyDocument=json.dumps(
                { 'Statement': [{'Action': 'sts:AssumeRole',
                                 'Effect': 'Allow',
                                 'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
        
        iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                               PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                              )['ResponseMetadata']['HTTPStatusCode']
    
    except Exception as e:
        print(e)

    iam_role = iam.get_role(IAM_ROLE_NAME)
    return iam_role


def get_iam_role():
    """Get's an existing IAM role"""
    iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY,
                 aws_secret_access_key=SECRET)
    iam_role = iam.get_role(RoleName = IAM_ROLE_NAME)['Role']['Arn']
    
    if iam_role:
        print('IAM role already exists')
        return iam_role
    else:
        iam_role = create_iam_role()
        return iam_role
        
        
def create_redshift_cluster(roleArn):
    """Creates redshift cluster
    Arguments:
        roleArn {string} - The Amazon Resource Name (ARN) specifying the IAM role"""
    redshift = boto3.client('redshift', region_name='us-west-2', 
                            aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    try:
        print('creating cluster...')
        response = redshift.create_cluster(        
            # add parameters for hardware
            ClusterType = 'multi-node',
            NodeType = 'dc2.large',
            NumberOfNodes = 4,

            # add parameters for identifiers & credentials
            DBName = DWH_NAME,
            ClusterIdentifier = CLUSTER_IDENTIFIER,
            MasterUsername = DWH_USER,
            MasterUserPassword = DWH_PASSWORD,

            # add parameter for role (to allow s3 access)
            IamRoles = [roleArn]
        )
    except Exception as e:
        print(e)
        
    cluster_props = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    config.set('CLUSTER', 'DWH_HOST', cluster_props['Endpoint']['Address'])
    enable_TCP_port_access(cluster_props['VpcId'])
    
    return cluster_props
        
    
def enable_TCP_port_access(vpc_id):
    """Open a TCP port for enabling information access to the cluster
    Arguments:
        vpc_id {string} - virtual private cloud ID associated with redshift cluster"""
    ec2 = boto3.resource('ec2', region_name='us-west-2', aws_access_key_id=KEY,
                 aws_secret_access_key=SECRET)
    try:
        vpc = ec2.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

        
def main():
    """Establishes connection with the sparkify database in S3 and gets
    cursor to it.  
    Performs operations to create new tables in redshift and an ERD graph 
    that displays the relations between each table in a .png file"""
    iam_role = get_iam_role()
    cluster_props = create_redshift_cluster(iam_role)
    
#   Connect to the redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    create_erd()

    conn.close()
    
if __name__ == "__main__":
    main()