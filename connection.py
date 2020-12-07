import pandas as pd
import boto3
import json
import configparser
from pprint import pprint
from time import time

# Load all config parameters from the config file "dwh.cfg"

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

DB_NAME=config.get("CLUSTER","DB_NAME")
DB_USER=config.get("CLUSTER","DB_USER")
DB_PASSWORD=config.get("CLUSTER","DB_PASSWORD")
DB_PORT=config.get("CLUSTER","DB_PORT")

DWH_ENDPOINT = config.get("CLUSTER","HOST")
DWH_ROLE_ARN = config.get("IAM_ROLE","DWH_ROLE_ARN")

SONG_DATA = config.get("S3","SONG_DATA")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")



pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_IAM_ROLE_NAME", "DWH_ENDPOINT", "DWH_ROLE_ARN", "SONG_DATA", "LOG_DATA", "LOG_JSONPATH", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME, DWH_ENDPOINT, DWH_ROLE_ARN, SONG_DATA, LOG_DATA, LOG_JSONPATH, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT]
             })

# Create the needed AWS Clients

ec2 = boto3.resource('ec2',
                 region_name="us-west-2",
                 aws_access_key_id=KEY,
                 aws_secret_access_key=SECRET
                 )

s3 = boto3.resource('s3',
                 region_name="us-west-2",
                 aws_access_key_id=KEY,
                 aws_secret_access_key=SECRET
                 )

iam = boto3.client('iam',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                  )

redshift = boto3.client('redshift',
                 region_name="us-west-2",
                 aws_access_key_id=KEY,
                 aws_secret_access_key=SECRET
                 )

    
# Create the IAM Role to have Redshift Access the S3 Bucket (ReadOnly)

def create_role(DWH_IAM_ROLE_NAME):
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
            )    
    except Exception as e:
        print(e)

    
# Attach Policy

def attach_policy(DWH_IAM_ROLE_NAME):
    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                   PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                  )['ResponseMetadata']['HTTPStatusCode']


# Get & Print the IAM Role ARN

def get_role(DWH_IAM_ROLE_NAME):
    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(roleArn)
    return roleArn


# Create the Redshift Cluster

def create_cluster(DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DB_NAME, DWH_CLUSTER_IDENTIFIER, DB_USER, DB_PASSWORD, roleArn):

    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
        
            #Roles (for s3 access)
            IamRoles=[roleArn]  
            )

    except Exception as e:
        print(e)


# Run "connection.py" functions to create role, attach policy, set roleArn, and create the cluster.        
        
def main():
    
    create_role(DWH_IAM_ROLE_NAME)
    attach_policy(DWH_IAM_ROLE_NAME)
    roleArn = get_role(DWH_IAM_ROLE_NAME)
    create_cluster(DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DB_NAME, DWH_CLUSTER_IDENTIFIER, DB_USER, DB_PASSWORD, roleArn)

    
if __name__ == "__main__":
    main()
