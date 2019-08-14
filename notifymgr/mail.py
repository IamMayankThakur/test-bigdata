import smtplib
import ssl


def send_mail(receiver_email):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "noreplybigdata@gmail.com"  # Enter your address
    password = "test@bigdata"
    message = """\
    Subject: Hi there

    This message is sent to test sending automated mails."""

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)
