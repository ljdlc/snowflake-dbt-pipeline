import json
import toml
import os
import snowflake.connector as sf
from dotenv import load_dotenv
import boto3

def lambda_handler(event, context):
 
    #load config.toml file into app_config variable
    app_config = toml.load('config.toml')
    
    #load environment variables from .env file
    load_dotenv()
    
    # set url of s3 bucket where file is being downloaded from
    s3_url = app_config['s3']['s3_url']
    
    # set destination folder and filename to download the file to lambda ephemoral storage
    destination_folder = app_config['download']['destination_folder']
    file_name = app_config['download']['file_name']
    
    # set snowflake configuration variables
    
    # set snowflake username, password, and account identifier information as variables from .env file
    # .env file should be in this format:
    # user = ''
    # password = ''
    # account = 'XXXXXXX-XXXXXXX'

    user = os.getenv('user')
    password = os.getenv('password')
    account = os.getenv('account')

    # set snowflake database connection variables from 'app_config' variable created from .toml file.
    warehouse = app_config['snowflake']['warehouse']
    database = app_config['snowflake']['database']
    schema = app_config['snowflake']['schema']
    role = app_config['snowflake']['role']
    file_format = app_config['snowflake']['file_format']
    stage_name = app_config['snowflake']['stage_name']
    table = app_config['snowflake']['table']

    # Set AWS authentication variables from .env file to download .csv file from S3
    # AWS_ID, AWS_KEY
    aws_id = os.getenv('AWS_ID')
    aws_key = os.getenv('AWS_KEY') 
    
    # Create boto3 client connection using AWS access key and secret access key credentials
    client = boto3.client(
        's3',
        aws_access_key_id=aws_id,
        aws_secret_access_key=aws_key
        )

    # set download filepath and filename into lambda ephemoral storage folder using destination_folder and filename in config.toml file
    file_path = os.path.join(destination_folder, file_name)
    
    # Use boto3 to download the file from the S3. Bucket is configured as "requester pays"
    client.download_file(Bucket='de-materials-tpcds', Key=file_name, Filename=file_path, ExtraArgs={'RequestPayer': 'requester'})
    
    # print first 10 lines of file to confirm file is downloaded correctly   
    with open(file_path) as f:
        head = [next(f) for line in range(10)]
        print(head)
        
    # connect to snowflake with snowflake user account info and database configuration saved in .env and config.toml.
    conn = sf.connect(user = user, password = password, \
                 account = account, warehouse=warehouse, \
                  database=database,  schema=schema,  role=role)

    
    # create cursor to execute commands in snowflake
    cursor = conn.cursor()    
    
    # use warehouse
    use_warehouse = f'USE WAREHOUSE {warehouse};'
    cursor.execute(use_warehouse)
    
    # use schema
    use_schema = f'USE SCHEMA {schema};'
    cursor.execute(use_schema)
    
    # create file format
    create_csv_format = f"CREATE OR REPLACE FILE FORMAT {file_format} TYPE='CSV' FIELD_DELIMITER=',';"
    cursor.execute(create_csv_format)
    
    # create stage where file will be uploaded to in snowflake
    create_stage = f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT={file_format};"
    cursor.execute(create_stage)
    
    #upload file to stage
    upload_file = f"PUT 'file://{file_path}' @{stage_name};"
    cursor.execute(upload_file)
    
    # list stage
    list_stage = f"LIST @{stage_name};"
    cursor.execute(list_stage)
    
    # truncate table to clear existing rows and columns before copying new data
    truncate_table = f"TRUNCATE TABLE {schema}.{table};"
    cursor.execute(truncate_table)
    
    # copy data from file in staging area into schema.table configured in config.toml file. 
    copy_into_table = f"COPY INTO {schema}.{table} FROM @{stage_name}/{file_name} FILE_FORMAT={file_format} ON_ERROR='CONTINUE';"
    cursor.execute(copy_into_table)
 
    print('File uploaded to Snowflake successfully')

    return {
        'statusCode': 200,
        'body': json.dumps('File uploaded to Snowflake successfully')
    }