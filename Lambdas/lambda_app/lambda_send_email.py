import json
import boto3
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
#config 10s execution in lambda

def get_secret():

    secret_name = "app-interchange-secret-smtp-prod"
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

# =============================================================================
# SEND EMAIL FUNCTION
# =============================================================================
def send_email(event):
    # Change the items with: ######Change Me#######
    secret_data = get_secret()
    gmail_user = secret_data['email_user']
    gmail_app_password = secret_data['email_password']
    email_server_port = int(secret_data['server_port'])
    header= event['header']
    sent_to = ', '.join(event['recipient'])
    
    subject = event['subject']
    sent_body = event['body']
    footer=event['footer']
    style = event['style']
    
    html_content = f"""
    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
        <html
        xmlns="http://www.w3.org/1999/xhtml"
        xmlns:v="urn:schemas-microsoft-com:vml"
        xmlns:o="urn:schemas-microsoft-com:office:office"
        >
        <head>
            <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
            <meta
                name="viewport"
                content="width=device-width, initial-scale=1, maximum-scale=1"
            />
            <meta http-equiv="X-UA-Compatible" content="IE=edge" />
            <meta name="format-detection" content="date=no" />
            <meta name="format-detection" content="address=no" />
            <meta name="format-detection" content="telephone=no" />
            <meta name="x-apple-disable-message-reformatting" />
        
            <style>
                {style}
            </style>
        </head>
        <body>
            <table class="main-table" cellpadding="0" cellspacing="0">
                <tbody class="body-background">
                    <tr>
                    <td align="center" valign="top">
                        <table class="inner-table" border="0">
                            {header}
                            <tr>
                                <th>
                                    {sent_body}
                                </th>
                            </tr>
                        </table>
                    </td>
                    </tr>
                    <tr class="font-family-arial">
                        {footer}
                    </tr>
                </tbody>
            </table>
        </body>
        </html>
    """
    #print("HTML FINAL")
    #print(html_content)
    
    msg = MIMEMultipart('alternative')
    
    msg['Subject'] = subject
    msg['From'] = gmail_user
    msg['To'] = sent_to
    
    part = MIMEText(html_content, 'html')
    msg.attach(part)
    

    try:
        with smtplib.SMTP(secret_data['server_smtp'], email_server_port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(gmail_user, gmail_app_password)
            smtp.send_message(msg)
        return "Email sent!"
        
        
        
        
    except Exception as exception:
        print("Error: %s!\n\n" % exception)
# =============================================================================
# END OF SEND EMAIL FUNCTION
# =============================================================================


def lambda_handler(event, context):
    # TODO implement
    mssg = send_email(event)
    return {
        'statusCode': 200,
        'body': json.dumps({'Result': mssg})
    }
