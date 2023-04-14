from flask import Flask, jsonify, request
import os
import psycopg2
from dotenv import load_dotenv
app = Flask(__name__)

#will load the .env file to get the variables
load_dotenv()

#DBUSER is used instead of USER as it will get the value of system user instead of the variable assigned
host = os.getenv("HOST")
database = os.getenv("DATABASE")
user = os.getenv("DBUSER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")


#As these are GET APIs the parameters will be sent through URL. For example - http://127.0.0.1:5000/api/most_active?start_time="2023-04-06 13:33:28.967"&end_time="2023-04-06 17:34:05.372"

#I went with the use of table business_pay_acc_transfer_transactions to determine the most active business as it contains the essence of the app i.e. Procurements, Billing, payments etc
@app.get('/api/most_active')
def most_active():
    conn = psycopg2.connect(database=database,user=user,password=password,host=host,port=port)
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    
    cur = conn.cursor()

    cur.execute("SELECT b.business_name, COUNT(*) AS activity_count FROM business_pay_acc_transfer_transactions t INNER JOIN business b ON t.business_id = b.business_id WHERE t.created_on  BETWEEN %s AND %s GROUP BY b.business_name ORDER BY activity_count DESC LIMIT 1;",(start_time,end_time))

    result = cur.fetchone()

    cur.close()
    conn.close()


    return jsonify({'business_name':result[0],'activity_count':result[1]})

#This API was created to try out the other idea I had on activity i.e. distribution-business sales as it is one of the most important factor to determine the activity.
@app.get('/api/distribution_business_most_active')
def most_active_business_distribution():
    conn = psycopg2.connect(database=database,user=user,password=password,host=host,port=port)
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    
    cur = conn.cursor()

    cur.execute("SELECT b.business_name, COUNT(*) AS activity_count FROM distribution_business_sales dbs INNER JOIN business b ON dbs.business_id = b.business_id WHERE dbs.created_on  BETWEEN %s AND %s GROUP BY b.business_name ORDER BY activity_count DESC LIMIT 1;", (start_time,end_time))

    result = cur.fetchone()

    cur.close()
    conn.close()
    

    return jsonify({'business_name':result[0],'activity_count':result[1]})
