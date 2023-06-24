import psycopg2
import pandas as pd




def fetchData (from_date , to_date):
    con = psycopg2.connect(database="main",user="postgres",password="password",host="141.11.21.13",port= '5432')
    cursor_obj = con.cursor()
    cursor_obj.execute("SELECT * FROM devices where time >= '{}' and time < '{}' " .format(from_date , to_date))
    result = cursor_obj.fetchall()
    df_result = pd.DataFrame(result)
    return df_result


def Create_Config_Table():
    conn = psycopg2.connect(database="main", user='postgres', password='password', host='141.11.21.13', port= '5432')
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS Config")
    sql ='''CREATE TABLE Config(
    LastStateValue CHAR(200) NOT NULL,
    ResultSize int Not Null
    )'''
    cursor.execute(sql)
    print("Table created successfully........")
    conn.commit()
    conn.close()



