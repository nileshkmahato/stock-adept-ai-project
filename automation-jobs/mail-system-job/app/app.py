""" 
mail notification cloud funtion for daily job updates.
"""
import smtplib
import os
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import functions_framework

# Zoho Mail SMTP Configuration
SMTP_SERVER = "smtp.zoho.in"
SMTP_PORT = 587  # TLS port
SMTP_USER = os.environ.get('SMTP_USER')  # Your Zoho email
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')  # Your Zoho password or app-specific password

@functions_framework.http
def send_email(request):
    """
    HTTP Cloud Function to send emails via Zoho Mail.
    
    Args:
        request (flask.Request): The request object.
        Expected JSON payload:
        {
            "to_mail": "recipient@example.com" or ["recipient1@example.com", "recipient2@example.com"],
            "cc": "cc@example.com" or ["cc1@example.com", "cc2@example.com"] (optional),
            "subject": "Email Subject",
            "content": "Email content/body",
            "content_type": "plain" or "html" (optional, default: "plain")
        }
    
    Returns:
        JSON response with status and message
    """
    
    # Handle CORS for browser requests
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    # Set CORS headers for main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }
    
    try:
        # Parse request data
        request_json = request.get_json(silent=True)
        
        if not request_json:
            return (json.dumps({
                'status': 'error',
                'message': 'Invalid request. JSON payload required.'
            }), 400, headers)
        
        # Validate required fields
        to_mail = request_json.get('to_mail')
        subject = request_json.get('subject')
        content = request_json.get('content')
        
        if not all([to_mail, subject, content]):
            return (json.dumps({
                'status': 'error',
                'message': 'Missing required fields: to_mail, subject, and content are required.'
            }), 400, headers)
        
        # Optional fields
        cc = request_json.get('cc', [])
        content_type = request_json.get('content_type', 'plain')  # 'plain' or 'html'
        
        # Check SMTP credentials
        if not SMTP_USER or not SMTP_PASSWORD:
            return (json.dumps({
                'status': 'error',
                'message': 'SMTP credentials not configured. Please set SMTP_USER and SMTP_PASSWORD environment variables.'
            }), 500, headers)
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = SMTP_USER
        msg['Subject'] = subject
        
        # Handle to_mail as string or list
        if isinstance(to_mail, str):
            to_mail = [to_mail]
        msg['To'] = ', '.join(to_mail)
        
        # Handle cc as string or list (optional)
        if cc:
            if isinstance(cc, str):
                cc = [cc]
            msg['Cc'] = ', '.join(cc)
            recipients = to_mail + cc
        else:
            recipients = to_mail
        
        # Attach the email body
        if content_type == 'html':
            msg.attach(MIMEText(content, 'html'))
        else:
            msg.attach(MIMEText(content, 'plain'))
        
        # Send email via Zoho SMTP
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()  # Enable TLS encryption
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, recipients, msg.as_string())
        
        return (json.dumps({
            'status': 'success',
            'message': f'Email sent successfully to {len(recipients)} recipient(s)',
            'recipients': recipients
        }), 200, headers)
        
    except smtplib.SMTPAuthenticationError as e:
        return (json.dumps({
            'status': 'error',
            'message': f'SMTP Authentication failed: {str(e)}'
        }), 401, headers)
        
    except smtplib.SMTPException as e:
        return (json.dumps({
            'status': 'error',
            'message': f'SMTP error occurred: {str(e)}'
        }), 500, headers)
        
    except Exception as e:
        return (json.dumps({
            'status': 'error',
            'message': f'An error occurred: {str(e)}'
        }), 500, headers)
