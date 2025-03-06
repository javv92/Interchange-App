from datetime import datetime, timedelta
import calendar
import pgdb
import os
import argparse
import sys
import pandas as pd
import boto3
from dotenv import load_dotenv
import yaml
import numpy as np
import re
import json
from pathlib import Path


load_dotenv()

def get_secret(secret_name,aws_access_key_id,aws_secret_access_key,region_name):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
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
    """Returns a connection class already initializated for postgreSQL database
        Example of use:
            connecting to database
            x = connect_to_postgreSQL().conecction()
            cur = x.cursor()
            Exec function
            cur.execute('SELECT version()')
            Fetching result
            db_version = cur.fetchone()
            shows version
            print(db_version)
            then we close cursor
            cur.close()

        Returns:
            object: connection object.
    """
    try:
        db_host = secret_data['host']     
        db_name = "interchange"       
        db_port = secret_data['port']
        db_username = secret_data['username']
        db_password = secret_data['password']
        connection = pgdb.Connection(
            database=db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            **{"port": db_port}
        )
        ## CAMBIAR A SECREET
        return connection
    except Exception as e:
        print(e)
        return [(False, e)]


# Función para ejecutar una consulta y devolver un DataFrame
def query_to_dataframe(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)

def invoke_lambda(function_name, payload_json,aws_access_key_id,aws_secret_access_key):
    lambda_client = boto3.client('lambda',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')  # Cambia la región si es necesario
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Utiliza 'RequestResponse' para esperar la respuesta o 'Event' para invocación asíncrona
            Payload=payload_json
        )
        print("Lambda invocation response:", response)
    except Exception as e:
        print(f"Error invoking Lambda function: {e}")
        sys.exit(1)


if __name__ == "__main__":
    aws_key = os.getenv("USER_NOTIFICATION_AWS_KEY")
    aws_access_key = os.getenv("USER_NOTIFICATION_AWS_ACCESS_KEY") 
    secret_data = get_secret(os.getenv("SECRET_RDS"),aws_key,aws_access_key,os.getenv("REGION_NAME"))
    cnn = cnn_postgresql(secret_data)
    q_list = "select name,code  from control.t_customer where status = 'ACTIVE'"
    df_clients = query_to_dataframe(cnn, q_list)

    for index, row in df_clients.iterrows():
        PRM_FACTOR_SEARCH = 0
        now = datetime.now() + timedelta(hours=PRM_FACTOR_SEARCH)
        PRM_CLIENT = row['code'].strip()
        PRM_NAME_CLIENT =row['name'].strip()
        print("\n================================= INICIO DEL PROCESO ===================================")
        print(f"================= {PRM_CLIENT} - {PRM_NAME_CLIENT}  ========================")
        print("==========================================================================================")
        print(">> Start date: " + str(now))
        formatted_date = now.strftime('%Y%m')
        month_name = calendar.month_name[int(formatted_date[-2:])]
        PRM_PERIODO= int(formatted_date)
        script_directory = os.path.dirname(os.path.abspath(__file__))
        path_yml = os.path.join(script_directory, "input","lst_distrb.yaml")
        try:
            with open(path_yml, 'r') as file:
                config = yaml.safe_load(file)
        
            PRM_TO_SEND = config[PRM_CLIENT]['emails']

        except KeyError as e:
            print(f"Error: The client code '{e.args[0]}' is not found in the YAML file")
            continue
        
        
        one_hour_ago = now - timedelta(hours=1)
        query= "select periodo,EXTRACT(DAY FROM date_send) as dia_envio,client,file_name, \
            status_code as status,failure_message,count(*) as count_sends \
            from control.uploaded_files_itx \
            where periodo = "+str(PRM_PERIODO)+" and  client = '"+PRM_CLIENT+"' and date_send between '"+str(one_hour_ago)+"' and '"+str(now)+"'\
            group by periodo,EXTRACT(DAY FROM date_send) ,client,file_name,status_code,failure_message order by 2,4" 
        df_resume = query_to_dataframe(cnn, query)

        if(df_resume.shape[0]>0):
            aws_access_key_id =   aws_key
            aws_secret_access_key  = aws_access_key
            sub_subject = "Interchange Files Submission Summary" 
            subject = sub_subject+ " - " + PRM_CLIENT
            df_result = df_resume.fillna(0)
            df_result['Month'] = month_name
            result_1 = df_result[['Month','dia_envio','file_name','status','failure_message','count_sends']]
            
            result_1.rename(columns={
                'dia_envio': 'Date received',
                'file_name': 'File Name',
                'status': 'SFTP Transmission Status',
                'failure_message': 'Error Message',
                'count_sends': 'Times sent'
            }, inplace=True) 
            
            print(">> tabla final")
            pd.set_option('display.max_columns', None)
            print(result_1.head(5))

            html_table = result_1.to_html(index=False, classes='data-table',border="0")
            style_th = ("padding: 10px 0px; text-align: center; background-color: #17375e; "
                "color: #fff; border: 1px solid #a6a5a1; font-size: 12px; font-family: Arial, sans-serif;")
            style_td = ("padding: 10px; text-align: left; color: #17375e; "
                        "font-size: 12px; font-family: Arial, sans-serif; border: 1px solid #a6a5a1;")

            html_styled = re.sub(r'(<th>)', f'<th style="{style_th}">', html_table)
            html_styled = re.sub(r'(<td>)', f'<td style="{style_td}">', html_styled)

            header = f'<tr><td><table class="line1" width="100%" cellspacing="0" cellpadding="0"><tr class="font-family-arial"><th class="text-left"><h1 class="header-title"><b><span class="color-blue">File Submission Summary </span><br /><span class="color-orange">{PRM_NAME_CLIENT}</span></b></h1></th><th class="logo"><img src="https://incontrol.intelica.com/WebApiAssets/img/Logo-Intelica_Email.png" class="logo-img" editable="true" alt="" /></th></tr></table></td></tr><tr class="font-family-arial"><td class="text-content"><p style="margin: 0px"><br />Dear user, <br /><br />In the following table, you will find the report of the files we received in the last hour.</p></td></tr>'

            footer= '<td align="center" valign="top"><table class="footer-table" border="0" cellspacing="0"><tr class="font-family-arial" border="0"><td class="footer-text"><br /><p style="margin-top: 0px"><br /><b class="confidential">Confidentiality Note </b><br /><br />This message and any accompanying attachments contain information from Intelica Consulting which is confidential or privileged. The information is intended to be for the use of the individual or entity named above. If you are not the intended recipient, be aware that any disclosure, copying, distribution or use of the contents of this information is prohibited. If you have received this e-mail in error, please notify our offices immediately by e-mail at <a href="mailto:security@intelica.com" class="email-link">security@intelica.com</a>.<span style="color: #17375e">.</span></p><div><span><img class="footer-img" src="https://incontrol.intelica.com/Mailing/save-paper.png" alt="" /></span>&nbsp;<span>Save paper. Print this email only if necessary.</span></div></td></tr><tr class="font-family-arial" border="0"><td class="footer-bottom"><b><a href="https://www.intelica.com" class="footer-link">www.intelica.com</a></b></td></tr></table></td>'
            style = "font { display: none; } table.main-table { min-width: 100%; width: 100%; font-family: Arial, sans-serif; } tbody.body-background { background: #fff; } table.inner-table { width: 600px; font-family: Arial, sans-serif; } table.line1 { border-bottom: 2px solid #d9d9d9; padding: 5px 0 5px; } tr.font-family-arial { font-family: Arial, sans-serif; } th.text-left { text-align: left; } h1.header-title { margin: 0px; font-size: 30px; text-align: left; padding: 10px 0px; } span.color-blue { color: #24377b; } span.color-orange { color: #ff7f00; } th.logo { font-size: 0px; line-height: 0px; text-align: right; display: flex; justify-content: flex-end; padding: 10px 0px; } img.logo-img { width: 153px; border: 0; } td.text-content { text-align: left; color: #1a3268; font-size: 14px; font-family: Arial, sans-serif; padding-top: 15px; padding-bottom: 20px; } table.data-table { border-collapse: collapse; border: 1px solid #a6a5a1; font-size: 12px; font-family: Arial, sans-serif; margin-bottom: 30px; width: 100%; } .data-table th, .data-table td { padding: 10px; text-align: center; color: #17375e; font-size: 12px; font-family: Arial, sans-serif; border: 1px solid #a6a5a1; } .data-table th { background-color: #17375e; color: #fff; } .data-table th.cell-33 { width: 33.3%; } .data-table th.cell-11 { width: 11.12%; } .data-table th.cell-22 { width: 22.2%; } table.footer-table { background-color: #f0f0f0; width: 800px; } td.footer-text { font-size: 11px; color: #a6a5a1; padding: 0px 15px 20px 15px; background-color: #f0f0f0; font-family: Arial, sans-serif; } b.confidential { color: #737373; } a.email-link { color: #17375e; text-decoration: underline; } img.footer-img { position: relative; } td.footer-bottom { background-color: #17375e; color: #fff; padding: 10px; font-size: 14px; text-align: center; } a.footer-link { color: #fff; text-decoration: none; } .data-table tr th { padding: 10px 0; text-align: center; background-color: #17375e; color: #fff; border: 1px solid #a6a5a1; } .data-table tr td { padding: 10px; text-align: left; color: #17375e; border: 1px solid #a6a5a1; }"
            #Build JSON
            send_email = {
                    "recipient": PRM_TO_SEND,
                    "subject": subject,
                    "header": header,
                    "body": html_styled,
                    "footer": footer,
                    "style": style
            }
            print(">> JSON CREATED")
            event_payload_json = json.dumps(send_email)
            print(">> Invoke Lambda to send email")
            invoke_lambda('app-interchange-lambda-send-email-prod', event_payload_json,aws_access_key_id,aws_secret_access_key)
        else:
            print(">> THERE ARE NO ITEMS TO REPORT.")
        print(">> End Date: " + str(datetime.now()))
        print("Duration: " + str(datetime.now() - now))

    print("\n=============== FIN DEL PROCESO ============================")
