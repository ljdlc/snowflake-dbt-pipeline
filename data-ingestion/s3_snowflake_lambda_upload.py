import json
import toml
import requests
import os
import snowflake.connector as sf
from dotenv import load_dotenv

def lambda_handler(event, context):
 
    #load config.toml file into app_config variable
    app_config = toml.load('config.toml')
    
    #load environment variables from .env file
    load_dotenv()
    
    # set url of s3 bucket where file is being downloaded from
    s3_url = app_config['s3']['s3_url']
    
    # set destination folder and filename to download the file to lambda ephemoral storage
    destination_folder = app_config['download']['destination_folder']
    filename = app_config['download']['filename']
    
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

    # save url request response into the 'response' variable
    response = requests.get(s3_url)
    # return HTTPError if error occurs during request
    response.raise_for_status()
    
    # set download filepath and filename into lambda ephemoral storage folder using destination_folder and filename in config.toml file
    file_path = os.path.join(destination_folder, filename)
    
    # open file and write response content to the file
    with open(file_path, 'wb') as f:
        f.write(response.content)
    
    # open file and set file contents to file_content variable. Print file_contents to check if the file has been written correctly.    
    with open(file_path, 'r') as f:
        file_content = f.read()
        print(file_content)
    
    
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
    copy_into_table = f"COPY INTO {schema}.{table} FROM @{stage_name}/{filename} FILE_FORMAT={file_format} ON_ERROR='CONTINUE';"
    cursor.execute(copy_into_table)
 
    print('File uploaded to Snowflake successfully')

    return {
        'statusCode': 200,
        'body': json.dumps('File uploaded to Snowflake successfully')
    }