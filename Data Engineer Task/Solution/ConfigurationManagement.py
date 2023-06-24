def GetLastStateValue():
    query = "SELECT LastStateValue FROM {} WHERE PipelineName = '{}'".format(config_data['config_sql_table'], config_data['config_pipeline_name'])
    cursor = sql_connection.cursor()
    cursor.execute(query)
    lastvalue = cursor.fetchval()
    cursor.close()
    return lastvalue