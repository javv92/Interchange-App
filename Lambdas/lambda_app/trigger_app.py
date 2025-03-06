import json
import boto3
import botocore
import os
import random
import logging
import boto3
from botocore.exceptions import ClientError
import json

AWS_REGION = 'us-east-1'
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

def send_queue_message(queue_url:str, msg_attributes:dict, msg_body:str)->object:
    """Sends a message to the specified queue.

    Args:
        queue_url (str): queue url.
        msg_attributes (dict): dict with message attributes.
        msg_body (str): message body for request.

    Returns:
        response (object): response object.

    """
    try:
        response = sqs_client.send_message(QueueUrl=queue_url,
                                           MessageAttributes=msg_attributes,
                                           MessageBody=msg_body)
    except ClientError:
        raise
    else:
        return response
    
def lambda_handler(event :object, context: object)->str:
    """ Lambda handler function

    Args:
        event (object): lambda event object.
        context (object): lambda context object.

    Returns:
        str: Message

    """
    print(f'S3 Evento:  {event}')
    if isinstance(event, str):
        event = json.loads(event)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/818835242461/app-interchange-' + key.split('/')[0].lower() + '-sqs-' + bucket.split('-')[-1]  #
    MSG_ATTRIBUTES = {
        'Title': {
            'DataType': 'String',
            'StringValue': 'Working with SQS in Python using Boto3'
        },
        'Author': {
            'DataType': 'String',
            'StringValue': 'Abhinav D'
        }
    }
    
    MSG_BODY = f'Bucket: {bucket},Key: {key}'
    msg = send_queue_message(QUEUE_URL, MSG_ATTRIBUTES, MSG_BODY)
    print(f'Bucket: {bucket}, Key: {key},sqs_target: {QUEUE_URL}')
    return "Success"
