from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import ConfigurationManagement
import MysqlConnection
import datetime
import time
import PosgresConnection
from pandas import json_normalize
import pandas as pd
import json
from math import sin , cos


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')


# --------------------- initiate pipeline variables ------------------------
counter = 0
mongo_result_size = 0
total_record = 0
retry_count= 4
retry_counter = 1
time_frame = 60
now = datetime.datetime.now()
now = datetime.datetime.strptime(str(now) + ' +00:00', '%Y-%m-%d %H:%M:%S.%f %z')

PosgresConnection.Create_Config_Table()
last_state_value = ConfigurationManagement.GetLastStateValue() 
#last_state_value = '2023-06-22 12:50:55.000000'
max_creationdatetime = last_state_value

from_date = datetime.datetime.strptime(last_state_value + ' +00:00', '%Y-%m-%d %H:%M:%S.%f %z')
to_date = from_date + datetime.timedelta(minutes = int(time_frame))  

attributesInTuple = from_date.timetuple()
From_date_seconds = time.mktime(attributesInTuple)
attributesInTuple = to_date.timetuple()
to_date_seconds = time.mktime(attributesInTuple)

# --------------------------- iteration loop -------------------------------
while to_date < now : 
    try:                                                          
        start_time = datetime.datetime.now()                      
    
        print('\n-- {}: round {} ---------------'.format(start_time, counter + 1))
        print('Fetching data form {} to {}'.format(from_date, to_date))

        pipeline = PosgresConnection.fetchData(From_date_seconds, to_date_seconds)
        result_size = len(pipeline)
        end_time = datetime.datetime.now()
        print("Data Fetched From mongo: {} Records in {}.".format(result_size, (end_time - start_time))) 
  # --------------------------- Preparing DataFrame -------------------------------      
        df_new = pipeline.rename(columns={  0  : 'device_id'  , 1 : 'temprature' , 2 : 'location'   ,  3 :'time'   })

        def ConvertToDate (x) :
            result = time.localtime(x)
            time_string = time.strftime("%m/%d/%Y, %H:%M:%S", result)
            return time_string
        
        df_new['NewTime'] = df_new['time'].astype(int)
        df_new['DateTime'] = df_new['NewTime'].apply(ConvertToDate)

  #-----------------------------Find maximum temperature---------------------------      
        df_max = df_new.loc[df_new.temprature.eq(df_new.groupby('device_id').temprature.transform('max'))]
        MysqlConnection.createTable_MaxTemprature()
        MysqlConnection.InsertInto_MaxTemprature(df_max)
  #-------------------------Find count of points-----------------------------------
        df_count = df_new.groupby(['device_id']).agg(['count'])
        MysqlConnection.createTable_PointsCount()
        MysqlConnection.InsertInto_PointsCount(df_count)
  #--------------------------Find total distance of movements----------------------      
        df_move_first =df_new.groupby('device_id').first()
        df_move_last =df_new.groupby('device_id').last()
        df3=pd.merge(df_move_first,df_move_last, on='device_id')
        df3['latitude_x'] = df3.apply(lambda x: json.loads(x['location_x'])['latitude'], axis = 1)
        df3['latitude_y'] = df3.apply(lambda x: json.loads(x['location_y'])['latitude'], axis = 1)
        df3['longitude_x'] = df3.apply(lambda x: json.loads(x['location_x'])['longitude'], axis = 1)
        df3['longitude_y'] = df3.apply(lambda x: json.loads(x['location_y'])['longitude'], axis = 1)
        df3['distance'] = cos(sin(df3['latitude_x']) * sin(df3['latitude_y']) + cos(df3['latitude_x']) * cos(df3['latitude_y']) * cos(df3['longitude_y'] - df3['longitude_x'])) * 6371

        MysqlConnection.createTable_Distance()
        MysqlConnection.InsertInto_Distance(df3)
        from_date = to_date        
        to_date = from_date + datetime.timedelta(minutes=int(time_frame))  
        ConfigurationManagement.SetLastStateValue(from_date , result_size)
        counter += 1               
    except Exception as exp:
        if (retry_counter < retry_count):            
            print('error in el procedure for the {} time'.format(retry_counter))
            retry_counter += 1       
            continue
        else:
            print('max retry count occured, exiting with error:')
            print(exp)
            break        
    