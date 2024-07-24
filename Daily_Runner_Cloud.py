# This file is meant to run each day to collect the ibm and sensor data and to push it to the database

# Custom API + SQL functions
import sys
sys.path.insert(0, '/home/mborrus/Model-Validation-GCP/ds-common/')
import API_Function_Library.Forecast_Functions as api_bl
import API_Function_Library.SQL_Functions as SQL_bl

# Import libraries
import datetime
import json
import pandas as pd

import sqlalchemy
from sqlalchemy import create_engine

import mysql.connector
from mysql.connector import Error

# Import variables
from Namelist import Account_Station
print(Account_Station)
from Namelist import db_info
print(db_info)
from Namelist import query_build
##############

hostname = db_info['hostname']
dbname = db_info['dbname']
uname = db_info['uname']
pwd = db_info['pwd']

print("Connecting to SQLAlchemy engine")
# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

# Create mysql.connection to do merge tables
connection = SQL_bl.create_db_connection(hostname, uname, pwd, dbname) # Connect to the Database

unix_socket_path = 'cloudsql/model-testing-1:us-west1:model-testing-database'
###############

AS_Index = 0; # This should be a for loop but there's only one station right now
start_datetime = datetime.date.today()-datetime.timedelta(days=1)
end_datetime = datetime.date.today()+datetime.timedelta(days=1)

AccountUid = Account_Station.AccountUid[AS_Index]
StationUid = Account_Station.StationUid[AS_Index]
Provider = Account_Station.Provider[AS_Index]

station_data_all = api_bl.Station_Info(AccountUid);
station_data = station_data_all[station_data_all['stationUid']==StationUid]

SensorID = station_data['identifier'].item()
print(type(SensorID))

# SENSOR DATA:
if Provider == 'WEATHER_LINK':
    # Get sensor data and clean it
    sensor_data =api_bl.get_Davis_data(SensorID,start_datetime,end_datetime,0)
    sensor_data_clean = api_bl.Davis_Cleaner(sensor_data, AccountUid, StationUid)
else:  
    "Provider not accepted"
    sensor_data_clean = pd.DataFrame();
##### Send data to temp sql table
sensor_data_clean.to_sql('Station_Data_Temp', engine, index=False, if_exists = 'replace')
##### Merge data with full table
query_merge = query_build("Station")
SQL_bl.execute_query(connection, query_merge) # Execute our defined query

print("Sensor Complete")

# IBM HISTORY:
IBM_data_clean = api_bl.get_IBM_data(AccountUid,StationUid,start_datetime,end_datetime)
##### Send data to temp sql table
IBM_data_clean.to_sql('IBM_Data_Temp', engine, index=False, if_exists = 'replace')
##### Merge data with full table
query_merge = query_build("IBM")
SQL_bl.execute_query(connection, query_merge) # Execute our defined query

print("IBM Complete")

print("ending for " + str(end_datetime))

print("with data collection done, start training")

# Import Training (this automatically runs it, could use exec instead)
# import PKL_Generation_Runner_Cloud as Training
# exec(open("PKL_Generation_Runner.py").read())
