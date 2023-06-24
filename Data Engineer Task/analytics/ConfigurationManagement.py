import psycopg2





def GetLastStateValue():
    con = psycopg2.connect(database="main",user="postgres",password="password",host="141.11.21.13",port= '5432')
    cursor_obj = con.cursor()
    cursor_obj.execute("SELECT LastStateValue FROM Config")
    lastvalue = cursor_obj.fetchval()
    return lastvalue


def SetLastStateValue(LastStateValue, pipeline_result_size):
    query = "Update Config Set LastStateValue = '{}', pipeline_result_size = '{}'  ".format(LastStateValue , pipeline_result_size)
    con = psycopg2.connect(database="main",user="postgres",password="password",host="141.11.21.13",port= '5432')
    cursor_obj = con.cursor()
    cursor_obj.execute(query)
    lastvalue = cursor_obj.fetchval()
    return lastvalue