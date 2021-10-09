import boto3
from botocore.exceptions import ClientError
import configparser
import json
import time

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("CLUSTER", "DWH_CLUSTER_TYPE")
DWH_NODE_TYPE = config.get("CLUSTER", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")

DWH_DB = config.get("DB", "DWH_DB")
DWH_DB_USER = config.get("DB", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DB", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DB", "DWH_PORT")
DWH_ENDPOINT = config.get("DB", "DWH_ENDPOINT")

DWH_IAM_ROLE_NAME = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")

iam = boto3.client(
    'iam',
    region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

ec2 = boto3.resource(
    'ec2',
    region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

redshift = boto3.client(
    'redshift',
    region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

# If the host is provided get the DB connection string and the ROLE ARN (used in sql_queries.py to COPY data to staging tables)
if DWH_ENDPOINT != 'none':
    conn_string = "host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER,
                                                                         DWH_DB_PASSWORD, DWH_PORT)
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


# If the host is not provided created the Redshift cluster with the necessary components'''
else:

    try:
        iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("IAM role already exists")
        else:
            print("Unexpected error: %s" % e)

    try:
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
        )

    except Exception as e:
        print(e)

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    cluster_exists = False

    try:
        redshift.create_cluster(
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            NumberOfNodes=2,
            IamRoles=[
                roleArn,
            ]
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            cluster_exists = True
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
            print('Cluster already exists')
        else:
            print('Unexpected error: %s' % e)

    if not cluster_exists:
        for attempt in range(5):
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            try:
                DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
                cluster_created = True
                print("Cluster successfully created")
                break
            except KeyError:
                print("Waiting for the Redshift cluster to be created...")
                time.sleep(45)

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print('Required security permissions already set up')
        else:
            print('Unexpected error: %s' % e)

    conn_string = "host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER,
                                                                         DWH_DB_PASSWORD, DWH_PORT)