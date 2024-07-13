import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import psycopg2
from configs import host, database, port, user, password, sender, recipient, token

def db_conn():
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    return conn

def db_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()

def submit_mail(from_adr, to_adr, subject, body, token):
    # server config using SSL
    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login(from_adr, token)
    # message config
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_adr
    msg['To'] = to_adr
    # HTML version of the email
    html = """
    <html>
      <head>
        <style>
          body {{
            font-family: Arial, sans-serif;
          }}
          h2 {{
            color: #333;
          }}
          table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
          }}
          th, td {{
            border: 1px solid #ddd;
            padding: 8px;
          }}
          th {{
            background-color: #f2f2f2;
            text-align: left;
          }}
          tr:nth-child(even) {{
            background-color: #f9f9f9;
          }}
        </style>
      </head>
      <body>
        <h2>Rumah Dijual Terbaru di Surabaya Timur</h2>
        <h3>Rumah Real Estate</h3>
        <table>
          <tr>
            <th>Agen Properti</th>
            <th>Judul</th>
            <th>Harga</th>
            <th>Nomor Makelar</th>
          </tr>
          {}
        </table>
        <h3>Rumah Winston</h3>
        <table>
          <tr>
            <th>Agen Properti</th>
            <th>Judul</th>
            <th>Harga</th>
            <th>Nomor Makelar</th>
          </tr>
          {}
        </table>
        <h3>Rumah Xavier</h3>
        <table>
          <tr>
            <th>Agen Properti</th>
            <th>Judul</th>
            <th>Harga</th>
            <th>Nomor Makelar</th>
          </tr>
          {}
        </table>
      </body>
    </html>
    """.format(body['realestate'], body['winston'], body['xavier'])

    part = MIMEText(html, 'html')
    msg.attach(part)
    # send mail
    server.sendmail(from_adr, to_adr, msg.as_string())
    # close server connection
    server.quit()

def format_message(rows):
    formatted_message = ""
    for row in rows:
        formatted_row = "<tr>" + "".join("<td>{}</td>".format(item) for item in row) + "</tr>"
        formatted_message += formatted_row
    return formatted_message

def mail_update():
    # mail credential
    token = "qubeagtrehkhhtij"
    sender = "azkiyaakmal25@gmail.com"
    recipient = "maulidya.mps@gmail.com"
    # establish database connection
    conn = db_conn()
    cur = conn.cursor()
    # create sql queries
    realestate_query = "select agen_properti, judul, harga, nomor_makelar from db_real_estate limit 3"
    winston_query = "select agen_properti, judul, harga, nomor_makelar from db_winston limit 3"
    xavier_query = "select agen_properti, judul, harga, nomor_makelar from db_xavier limit 3"
    # execute sql query
    realestate_data = db_query(cur, realestate_query)
    winston_data = db_query(cur, winston_query)
    xavier_data = db_query(cur, xavier_query)
    # format data
    realestate_msg = format_message(realestate_data)
    winston_msg = format_message(winston_data)
    xavier_msg = format_message(xavier_data)
    # mail format
    subject = "UPDATE RUMAH SURABAYA DIJUALL!"
    body = {'realestate': realestate_msg, 'winston': winston_msg, 'xavier': xavier_msg}
    # send mail
    submit_mail(sender, recipient, subject, body, token)