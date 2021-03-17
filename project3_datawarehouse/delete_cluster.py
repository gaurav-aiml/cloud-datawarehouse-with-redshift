import configparser
import boto3
def main():
    """
    Delete the redshift cluster identified by the DWH_CLUSTER_IDENTIFIER
    """

    cfg = configparser.ConfigParser()
    cfg.read_file(open('dwh.cfg'))

    #Extract properties from config file
    DWH_CLUSTER_IDENTIFIER = cfg.get("DWH","DWH_CLUSTER_IDENTIFIER")
    KEY                    = cfg.get('AWS','KEY')
    SECRET                 = cfg.get('AWS','SECRET')


    #Open a Redshift Client
    redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

    try:
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)
    

if __name__ == "__main__":
    main()