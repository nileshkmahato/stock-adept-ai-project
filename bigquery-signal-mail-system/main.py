# --------------------------------------------------
# EMAIL SENDER CLOUD FUNCTION - USED in Bigquery connector
# --------------------------------------------------
# This function is deployed as a Google Cloud Function and can be invoked
# as a BigQuery Remote Function. It receives email details (recipients,
# subject, content) from BigQuery, builds the message, and sends it via
# Zoho SMTP using credentials stored securely in environment variables.
# It supports multiple recipients, handles CORS requests, and returns
# a BigQuery-compatible JSON response indicating success or failure.
# --------------------------------------------------

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import functions_framework
from flask import jsonify

# Configuration
SMTP_USER = os.environ.get('SMTP_USER')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')

@functions_framework.http
def send_email(request):
    # 1. CORS handling
    if request.method == 'OPTIONS':
        return ('', 204, {'Access-Control-Allow-Origin': '*'})

    # 2. Validate BigQuery Remote Function format
    request_json = request.get_json(silent=True)
    if not request_json or 'calls' not in request_json:
        return jsonify({"error": "Invalid BigQuery Remote Function format"}), 400

    replies = []
    calls = request_json.get('calls', [])

    for call in calls:
        data = call[0] if isinstance(call[0], dict) else {}

        # 3. Parse fields
        to_email_raw = data.get('to_mail', '')
        subject      = data.get('subject', 'No Subject')
        content      = data.get('content', 'No Content')
        content_type = data.get('content_type', 'plain')

        # 4. Handle comma-separated emails
        to_email_list = [e.strip() for e in to_email_raw.split(',') if e.strip()]

        if not to_email_list:
            replies.append("Error: Recipient email is required")
            continue

        try:
            # 5. Build email
            msg = MIMEMultipart()
            msg["From"]    = SMTP_USER
            msg["To"]      = ", ".join(to_email_list)
            msg["Subject"] = subject
            msg.attach(MIMEText(content, content_type))

            # 6. Send via SMTP
            with smtplib.SMTP("smtp.zoho.in", 587) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, to_email_list, msg.as_string())

            replies.append(f"Success: Email sent to {', '.join(to_email_list)}")

        except Exception as e:
            replies.append(f"Error: {str(e)}")

    # 7. Return BigQuery-compatible response
    return jsonify({"replies": replies})