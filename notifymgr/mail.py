import smtplib, ssl

port = 465  # For SSL
smtp_server = "smtp.gmail.com"
sender_email = "noreplybigdata@gmail.com"  # Enter your address
receiver_email = "thakurmayank88@gmail.com"  # Enter receiver address
password = "test@bigdata"
message = """\
Subject: Hi there

This message is sent from Python to test sending automated mails."""

context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message)

# import smtplib, ssl
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
#
# sender_email = "noreplybigdata@gmail.com"
# receiver_email = "nishantravishankar@gmail.com"
# password = "test@bigdata"
#
# message = MIMEMultipart("alternative")
# message["Subject"] = "Test mail for bigdata evauation"
# message["From"] = sender_email
# message["To"] = receiver_email
#
# # Create the plain-text and HTML version of your message
# text = """\
# Hi,
# How are you?"""
# html = """\
# <html>
#   <body>
#     <p>Hi,<br>
#        How are you?<br>
#     </p>
#   </body>
# </html>
# """
#
# # Turn these into plain/html MIMEText objects
# part1 = MIMEText(text, "plain")
# part2 = MIMEText(html, "html")
#
# # Add HTML/plain-text parts to MIMEMultipart message
# # The email client will try to render the last part first
# message.attach(part1)
# message.attach(part2)
#
# # Create secure connection with server and send email
# context = ssl.create_default_context()
# with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
#     server.login(sender_email, password)
#     server.sendmail(
#         sender_email, receiver_email, message.as_string()
#     )