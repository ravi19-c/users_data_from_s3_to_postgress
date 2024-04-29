import pandas as pd
import boto3
import psycopg2
import json
import configparser
import numpy as np

config = configparser.ConfigParser()
config.read_file(open('cluster.config'))

key = config.get('AWS','key')
SECRET = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE = config.get('DWH','DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH','DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH','DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER = config.get('DWH','DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH','DWH_DB')
DWH_DB_USER = config.get('DWH','DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH','DWH_DB_PASSWORD')
DWH_PORT = config.get('DWH','DWH_PORT')
DWH_IAM_ROLE_NAME = config.get('DWH','DWH_IAM_ROLE_NAME')


para_df = pd.DataFrame({"parameters":["key","SECRET","DWH" ,"DWH_NUM_NODES","DWH_NODE_TYP","DWH_CLUSTER_IDENTIFIER",
                             "DWH_DB","DWH_user","DWH_DB_PASSWORD","DWH_PORT","DWH_IAM_ROLE_NAME"],
                    "values":[key,SECRET,DWH_CLUSTER_TYPE,DWH_NUM_NODES,DWH_NODE_TYPE,DWH_CLUSTER_IDENTIFIER,DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT,DWH_IAM_ROLE_NAME]

})

ec2 = boto3.resource('ec2',
                     region_name = "ap-south-1",
                     aws_access_key_id= key,
                     aws_secret_access_key= SECRET)

iam = boto3.client('iam',
                     region_name = "ap-south-1",
                     aws_access_key_id = key,
                     aws_secret_access_key = SECRET
)

s3 = boto3.resource('s3',
                     region_name = "ap-south-1",
                     aws_access_key_id = key,
                     aws_secret_access_key = SECRET
)

redshift = boto3.client('redshift',
                     region_name = "ap-south-1",
                     aws_access_key_id = key,
                     aws_secret_access_key = SECRET
)

bucket = s3.Bucket("stg-prac-bucket")
files = [file.key for file in bucket.objects.filter(Prefix='')]
roleArn = iam.get_role(RoleName= DWH_IAM_ROLE_NAME)['Role']['Arn']
try:
    response = redshift.create_cluster(
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            MasterUsername= DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles = [roleArn]
    )
except Exception as e:
    print(e)
response = redshift.describe_clusters(ClusterIdentifier =DWH_CLUSTER_IDENTIFIER)
def convert_pandas(prop):
    keysToShow = ["ClusterIdentifier",'NodeType','ClusterStatus','ClusterAvailabilityStatus','MasterUsername','DBName']
    x = [(k,v) for k, v in prop.items() if k in keysToShow]
    return  (pd.DataFrame(data = x, columns=["key","values"]))



try:
    conn = psycopg2.connect(database = "accounts",
                        user = "postgres",
                        host= '127.0.0.1',
                        password = "Welcome2@",
                        port = 5432,
                            )
    cur = conn.cursor()
    conn.set_session(autocommit=True)
except psycopg2.Error as e:
    print(e)
try:

    accounts_cntry = cur.execute("""create table if not exists allusers(
                                user_id int Primary key,
                                user_name varchar,
                                first_name varchar(30),
                                last_name varchar(30),
                                city varchar(30),
                                state char(2), 
                                email varchar (100), 
                                phone char (14),
                                likesports boolean, 
                                liketheatre boolean,
                                likeconcerts boolean, 
                                likejazz boolean,
                                likeclassical boolean, 
                                likeopera boolean,
                                likerock boolean,
                                likevegas boolean, 
                                likebroadway boolean, 
                                likemusicals boolean); """)
except psycopg2.Error as e:
    print(e)

try:
    cur.execute(""" CREATE TABLE if not exists venue (
        venue_id SMALLINT NOT NULL,
        venue_name VARCHAR(100),
        venue_city VARCHAR(30),
        venue_state CHAR(2),
        venue_seats INTEGER
    );""")
except psycopg2. Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("""create table if not exists category(
                cat_id smallint not null, 
                cat_group varchar (10), 
                cat_name varchar (10),
                cat_desc varchar(50));
    
                create table if not exists date(
                date_id smallint not null, 
                cal_date date not null, 
                day character (3) not null,
                week smallint not null, 
                month character (5) not null, 
                qtr character(5) not null, 
                year smallint not null,
                holiday boolean default( 'N'));
                
                create table if not exists event (
                event_id integer not null, 
                venue_id smallint not null, 
                cat_id smallint not null, 
                date_id smallint not null, 
                event_name varchar (200),
                start_time timestamp);
                
                create table if not exists listing(
                list_id integer not null, 
                seller_id integer not null, 
                event_id integer not null, 
                date_id smallint not null, 
                num_tickets smallint not null,
                priceper_ticket decimal (8,2),
                totalprice decimal (8,2),
                listtime timestamp);""")

except psycopg2.Error as e:
    print("Issue creating table: ",e)

try:
    conn = psycopg2.connect(database = "accounts",
                        user = "postgres",
                        host= '127.0.0.1',
                        password = "Welcome2@",
                        port = 5432,
                            )
    cur = conn.cursor()
    conn.set_session(autocommit=True)
except psycopg2.Error as e:
    print(e)
def load_from_s3_to_pg(table,file):

    try:
        cur.execute(f"""
                    COPY {table}
                    FROM 's3://stg-prac-bucket/{file}
                    iam_role '{roleArn}'
                    DELIMITER:'|' """)
    except Exception as e:
        print(e)

tables = ['event', 'allusers', 'category', 'listings','venue']
files = ['allevents_pipe.txt','allusers_pipe.txt','category_pipe.txt','listings_pipe.txt','venue_pipe.txt']

for table, file in zip(tables,files):
    load_from_s3_to_pg(table,file)
