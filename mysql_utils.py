# mysql_utils.py
import mysql.connector
import pandas as pd

def execute_sql(query):
    cnx = mysql.connector.connect(user='root',
                                  password='jordan23',
                                  host='127.0.0.1',
                                  database='mrts')
    cursor = cnx.cursor()
    
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    result_df = pd.DataFrame(rows, columns=columns)

    cnx.close()
    
    return result_df