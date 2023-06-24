import mysql.connector

def createTable_MaxTemprature():
    mydb = mysql.connector.connect(host="141.11.21.13",user="nonroot",password="nonroot",database="main")
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE MaxTemprature (device_id VARCHAR(255), temprature VARCHAR(255))")


def InsertInto_MaxTemprature(df):   
    mydb = mysql.connector.connect(host = "localhost",user = "username",password = "password",database = "database_name")
    mycursor = mydb.cursor() 
    insert_to_tmp_tbl_stmt = f"INSERT INTO MaxTemprature([device_id],[temprature])values(?,?)"
    mycursor.executemany(insert_to_tmp_tbl_stmt,  df[['device_id' , 'temprature' ]].values.tolist())
    mydb.commit()
    print(mycursor.rowcount, "details inserted")
    # disconnecting from server
    mycursor.commit()
    mycursor.close()
    mydb.close()


def createTable_PointsCount():
    mydb = mysql.connector.connect(host="141.11.21.13",user="nonroot",password="nonroot",database="main")
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE PointsCount (device_id VARCHAR(255), Count VARCHAR(255))")



def InsertInto_PointsCount(df):   
    mydb = mysql.connector.connect(host = "localhost",user = "username",password = "password",database = "database_name")
    mycursor = mydb.cursor() 
    insert_to_tmp_tbl_stmt = f"INSERT PointsCount([device_id],[Count])values(?,?)"
    mycursor.executemany(insert_to_tmp_tbl_stmt,  df[['device_id' , 'temprature' ]].values.tolist())
    mydb.commit()
    print(mycursor.rowcount, "details inserted")
    # disconnecting from server
    mycursor.commit()
    mycursor.close()
    mydb.close()    



def createTable_Distance():
    mydb = mysql.connector.connect(host="141.11.21.13",user="nonroot",password="nonroot",database="main")
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE Distance (device_id VARCHAR(255), Distance VARCHAR(255))")



def InsertInto_Distance(df):   
    mydb = mysql.connector.connect(host = "localhost",user = "username",password = "password",database = "database_name")
    mycursor = mydb.cursor() 
    insert_to_tmp_tbl_stmt = f"INSERT Distance([device_id],[Distance])values(?,?)"
    mycursor.executemany(insert_to_tmp_tbl_stmt,  df[['device_id' , 'distance' ]].values.tolist())
    mydb.commit()
    print(mycursor.rowcount, "details inserted")
    # disconnecting from server
    mycursor.commit()
    mycursor.close()
    mydb.close()    