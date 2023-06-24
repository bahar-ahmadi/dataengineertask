import psycopg2
import pandas as pd


def GetSqlConnectionString(pipeline_config):
    if (pipeline_config['destination_sql_login_type'] == "SQL"):
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;uid=%s;pwd=%s' % (pipeline_config['destination_sql_server'], pipeline_config['destination_sql_database'], pipeline_config['destination_sql_uid'], pipeline_config['destination_sql_pwd'])
    elif (pipeline_config['destination_sql_login_type'] == "Windows"):
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;Trusted_Connection=yes;' % (pipeline_config['destination_sql_server'], pipeline_config['destination_sql_database'])
    else:
        raise Exception('Destination SQL login type is not set properly. Please use "SQL" with username and password or use "Windows" to login using windows credentials.')    
    return  connection_string


def fetchData (from_date , to_date):
    con = psycopg2.connect(database="main",user="postgres",password="password",host="141.11.21.13",port= '5432')
    cursor_obj = con.cursor()
    cursor_obj.execute("SELECT * FROM devices where time >= '{}' and time < '{}' " .format(from_date , to_date))
    result = cursor_obj.fetchall()
    df_result = pd.DataFrame(result)
    return df_result




