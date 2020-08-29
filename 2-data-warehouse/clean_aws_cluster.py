import pandas as pd
import boto3
import json
import configparser
import time
    
def clean_up_redshift(config, key, secret):
    """"
    Delete redshift cluster
    
    Parameters: 
    config (configparser): config parser
    key (string): aws key
    secret (string): aws secret
    """
    print("1.1 Deleting AWS Redshift cluster")
    
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")   
    
    redshift = boto3.client('redshift',
                   region_name="us-west-2",
                   aws_access_key_id=key,
                   aws_secret_access_key=secret
                   )
        
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
    time.sleep(10) 
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    while myClusterProps['ClusterStatus'] == 'deleting':
        try:
            print("Waiting cluster to be deleted")
            time.sleep(30) 
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]      
        except:
            print('1.2 Cluster deleted')
            break
            
def clean_up_role(config, key, secret):
    """"
    Detach role policy from IAM role and them delete IAM role
    
    Parameters: 
    config (configparser): config parser
    key (string): aws key
    secret (string): aws secret
    """
    print("2.1 Deleting AWS IAM role")
    
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    iam = boto3.client('iam',aws_access_key_id=key,
                 aws_secret_access_key=secret,
                 region_name='us-west-2'
              )    
    
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

    print("2.2 IAM role deleted")
    
def main():
    """"
    Delete redshift cluster and them detach role policy from IAM role and delete IAM role
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    key                    = config.get('AWS','KEY')
    secret                 = config.get('AWS','SECRET')
    
    clean_up_redshift(config, key, secret)
    clean_up_role(config, key, secret)

if __name__ == "__main__":
    main()    