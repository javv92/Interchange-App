import json
from datetime import datetime
import pandas as pd
from dataclasses import dataclass
import psycopg2
import boto3
from psycopg2.extras import RealDictCursor
import hashlib
import time

def get_secret():

    secret_name = "app-interchange-secret-rds-dev"
    region_name = "us-east-1"
    

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = get_secret_value_response['SecretString']
        json_object = json.loads(secret)
        return json_object
    except Exception as e:
        print(f"Error retrieving secret: {str(e)}")
        raise

    
def cnn_postgresql(secret_data):
    db_host = secret_data['host']     
    db_name = "interchange"       
    db_port = secret_data['port']
    db_username = secret_data['username']
    db_password = secret_data['password']

    # Initialize the database connection if not already created
    conn = psycopg2.connect(
        host=db_host,
        user=db_username,
        password=db_password,
        database=db_name,
        port=db_port,
        connect_timeout=5
        )
    return conn

def lambda_handler(event, context):
    print("Time at which lambda invoke: " + str(datetime.now()) + "   Event : " + str(event))
    bucket_name = "intelica-interchange-landing-dev"
    schema_name = 'control'
    table_name = 'uploaded_files_itx'

    flattened_data = {
        'id': event['id'],
        'detail_type': event['detail-type'],
        'status_code': event['detail']['status-code'],
        'protocol': event['detail']['protocol'],
        'bytes': event['detail']['bytes'],
        'file_path': event['detail']['file-path'],
        'username': event['detail']['username'],
        'session_id': event['detail']['session-id'],
        'date_send': event['detail']['start-timestamp']
    }
    
   
    bucket_key_1=''
    full_new_path= flattened_data['file_path']
    full_new_path = full_new_path.replace('.filepart', '')
    bucket_key_1 = full_new_path.lstrip("/"+bucket_name) 
    

    path_split = bucket_key_1.split('/')
    try:
        if ( flattened_data['detail_type'] == "SFTP Server File Upload Completed"):
            flattened_data['failure_message'] = ""
        
        else:
            flattened_data['failure_message'] = event['detail']['failure-message']
        
        secret_data = get_secret()
            
    
        current_date = datetime.now()
        flattened_data['t_fch_load'] = current_date
        flattened_data['file_path'] = full_new_path
        flattened_data['client'] = path_split[0]
        flattened_data['file_name'] = path_split[1]
        flattened_data['Periodo'] = int(current_date.strftime('%Y%m'))
    
        # Creation insert
        columns = flattened_data.keys()
        values = [flattened_data[column] for column in columns]
        insert_statement = f"INSERT INTO {schema_name}.{table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"
        # Insert to DB
        cnx = cnn_postgresql(secret_data)
        with cnx.cursor() as cursor:
            cursor.execute(insert_statement, values)
        
        cnx.commit()
        msgg = ">> Data inserted correctly!"
        print(msgg)
        cnx.close()
        return {
            'statusCode': 200,
            'body': json.dumps({'Result': msgg})
        }
    except Exception as e:
        # Manejo de cualquier otro error
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    