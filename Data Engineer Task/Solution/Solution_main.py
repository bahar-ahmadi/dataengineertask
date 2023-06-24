import ConfigurationManagement
import datetime
import time
import PosgresConnection
# --------------------- initiate pipeline variables ------------------------
counter = 0
mongo_result_size = 0
total_record = 0
retry_count= 4
retry_counter = 1
time_frame = 60
now = datetime.datetime.now()
now = datetime.datetime.strptime(str(now) + ' +00:00', '%Y-%m-%d %H:%M:%S.%f %z')


#last_state_value = ConfigurationManagement.GetLastStateValue() 
last_state_value = '2023-06-22 12:50:55.000000'
max_creationdatetime = last_state_value

from_date = datetime.datetime.strptime(last_state_value + ' +00:00', '%Y-%m-%d %H:%M:%S.%f %z')
to_date = from_date + datetime.timedelta(minutes = int(time_frame))  

attributesInTuple = from_date.timetuple()
From_date_seconds = time.mktime(attributesInTuple)
attributesInTuple = to_date.timetuple()
to_date_seconds = time.mktime(attributesInTuple)

# --------------------------- iteration loop -------------------------------
while to_date < now : 
    #try:                                                          
        start_time = datetime.datetime.now()                      
    
        print('\n-- {}: round {} ---------------'.format(start_time, counter + 1))
        print('Fetching data form {} to {}'.format(from_date, to_date))

        pipeline = PosgresConnection.fetchData(From_date_seconds, to_date_seconds)
        mongo_result_size = len(pipeline)
        end_time = datetime.datetime.now()
        print("Data Fetched From mongo: {} Records in {}.".format(mongo_result_size, (end_time - start_time))) 
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

  #-------------------------Find count of points-----------------------------------
        df_count = df_new.groupby(['device_id']).agg(['count'])
  