import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from api.models import SubmissionAssignmentOne


def send_mail(submission_id):
    print(" In send_mail ")
    sub = SubmissionAssignmentOne.objects.get(id=submission_id)
    team = sub.team
    message = MIMEMultipart("alternative")
    message["Subject"] = "Big Data Assignment Result"
    message["From"] = "noreplybigdata@gmail.com"

    html = "Hi \n Your bigdata submission with submission id " + str(submission_id) + " has been evaluated \n" \
    "<html> " \
    "<body> " \
    "<h3> Scores </h3> " \
    "<p> Task 1: " + str(sub.score_1) + "</p>" + "<p> Task 2: " + str(sub.score_2) + "</p>" + "<p> Task 3: " + str(sub.score_3) + "</p>" + "<p> Task 4: " + str(sub.score_4) + "</p>" \
    "<h3> Remarks </h3> " + str(sub.remarks)
    "</body> "  \
    "</html>"

    part = MIMEText(html, "html")

    message.attach(part)

    emails = [team.member_1, team.member_2, team.member_3, team.member_4]
    for email in emails:
        if email != '':
            _send(email, message)


def _send(receiver_email, message):
    print(" Sending mail ")
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "noreplybigdata@gmail.com"  # Enter your address
    password = "bigdata2019"

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
