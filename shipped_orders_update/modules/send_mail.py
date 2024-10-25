
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib


# Verstuur een e-mail met het Excel-bestand als bijlage
def send_email(message, recipient_email, smtp_server, smtp_port, sender_email, sender_password):
    # Het onderwerp
    email_subject = f"""
    Verkeerd verpakte orders van afgelopen week
    """

    email_text = f"""\
Hi Henk,

Zie hier de lijst met de verkeerd verzonden orders van de afgelopen 7 dagen:

{message}

Groet,
Max
    """

    # Bericht samenstellen
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = email_subject
    message.attach(MIMEText(email_text, 'plain'))

    # Verbinding maken met de SMTP-server en e-mail verzenden
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, message.as_string())

    print('E-mail met bijlage verzonden')