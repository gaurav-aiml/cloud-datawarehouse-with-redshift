import boto3
import json
import pandas as pd
import configparser
import time
import psycopg2



def createCluster(client, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, role_arn):
    '''
        Create a Redhsift cluster using the client with the properties specified
    '''
    try:
        response = client.create_cluster(        
            #Hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            # NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[role_arn]
        )
    except Exception as e:
        print(e)

def prettyRedshiftProps(props):
        '''
            Function to pretty print the properties of a Redshift Cluster. 
        '''

        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])


def getClusterProperties(client, DWH_CLUSTER_IDENTIFIER):
    '''
     Print the properties of the cluster identified by the DWH_CLUSTER_IDENTIFIER
    '''
    
    #Get cluster properties
    myClusterProps = client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    #Wait until the Cluster status becomes available. Each time before fetching cluster properties wait for 20 seconds.
    while(myClusterProps['ClusterStatus']!='available'):
        time.sleep(20)
        myClusterProps = client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        
    
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    return myClusterProps


def allowConnectionFromLocalhost(client, myClusterProps, DWH_PORT):
    '''
        Adds an Ingress rule to security group associated with created cluster to allow traffic from Localhost
    '''

    try:
        vpc = client.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        # print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT))
    except Exception as e:
        print(e)

def main():
    '''
        Function to Create the Redshift cluster. Has the following steps

        Step1. Create Cluster using config parameters
        Step2. Print the cluster properties
        Step3. Test the connection to the created cluster

    '''

    cfg = configparser.ConfigParser()
    cfg.read_file(open('dwh.cfg'))

    #AWS PARAMETERS
    KEY                    = cfg.get('AWS','KEY')
    SECRET                 = cfg.get('AWS','SECRET')

    #Redshift Parameters
    DWH_CLUSTER_TYPE       = cfg.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = cfg.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = cfg.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = cfg.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = cfg.get("DWH","DWH_DB")
    DWH_DB_USER            = cfg.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = cfg.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = cfg.get("DWH","DWH_PORT")
    DWH_IAM_ROLE_NAME      = cfg.get("DWH", "DWH_IAM_ROLE_NAME")

    #S3 Paramters
    LOG_DATA               = cfg.get("S3", "LOG_DATA")
    LOG_JSONPATH           = cfg.get("S3", "LOG_DATA")
    LOG_DATA               = cfg.get("S3", "LOG_DATA")


    #EC2 Client
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

    #Redshift Client
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

    #IAM Client
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
    
    #Get the IAM Role ARN associated with the Redshift Cluster & S3
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    #Spin up the Redhsift Cluster
    createCluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, role_arn)

    #Get the cluster properties once it has been created and is available
    myClusterProps = getClusterProperties(redshift, DWH_CLUSTER_IDENTIFIER)
    print(prettyRedshiftProps(myClusterProps))

    #Allow connections from Localhost by adding an Ingress rule to the Security Group associated with the cluster.
    allowConnectionFromLocalhost(ec2, myClusterProps, DWH_PORT)


    #Test the connection.
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*cfg['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected')
    conn.close()
    
if __name__ == "__main__":
    main()
