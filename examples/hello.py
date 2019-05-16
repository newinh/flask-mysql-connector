from flask import Flask
from flask_mysql_connector import MySQL

app = Flask(__name__)

MySQL.init_app(app)


@app.route("/")
def hello():
    query = "SELECT 'Hello_World';"
    db = MySQL()
    with db.cursor(dictionary=True) as cursor:
        cursor.execute(query)
        rv = cursor.fetchone()
    return rv
