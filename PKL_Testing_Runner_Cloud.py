# This file is meant to generate BL forecasts and save the ibm forecasts

# Custom API + SQL functions
import sys
sys.path.insert(0, '/home/mborrus/Model-Validation-GCP/ds-common/')
import API_Function_Library.Forecast_Functions as api_bl
import API_Function_Library.SQL_Functions as SQL_bl
from olm import *

# Import libraries
import sqlalchemy as db
from sqlalchemy import create_engine
import pickle
import datetime

# Import variables
from Namelist import db_info
from Namelist import Account_Station
from Namelist import Training_vars
from Namelist import pkl_path
###############

hostname = db_info['hostname']
dbname = db_info['dbname']
uname = db_info['uname']
pwd = db_info['pwd']

# Create SQLAlchemy engine to connect to MySQL Database
engine = db.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                       .format(host=hostname, db=dbname, user=uname, pw=pwd))

# in for data from database/table, out for data to database/table
connection_in = engine.connect()
connection_out = SQL_bl.create_db_connection(hostname, uname, pwd, dbname) # Connect to the Database

#Connect to the existing SQL tables
metadata = db.MetaData()
Pkl_table = db.Table('Pkl_Performance_Data', metadata, autoload=True, autoload_with=engine)

###############
import numpy as np
import pandas as pd
def Forecast_From_Model(input_pkl, Input_Forecast_Data, Training_var):
    Input_Forecast_Data=Input_Forecast_Data.dropna()
    XForecast=Input_Forecast_Data['%s' % (Training_var)].to_numpy()
    XForecast=XForecast.reshape(XForecast.shape[0],1)

    # Create benchmark FORECASTS
    Yhat=Elm_predict(input_pkl, XForecast)
    Yhat=np.transpose(Yhat)
    df_Yhat = pd.DataFrame(Yhat,columns = ['forecast'],index=Input_Forecast_Data.Local_datetime)
    df_Yhat = df_Yhat.reset_index()
    df_Yhat['Local_datetime'] = pd.to_datetime(df_Yhat['Local_datetime'])

    Q1 = input_pkl['q1']
    Q3 = input_pkl['q3']
    IQR = Q3 - Q1

    # print((Q1 - 1.5 * IQR).item())
    # print((Q3 + 1.5 * IQR).item())

    lower_bound = (Q1 - 1.5 * IQR).item()
    upper_bound = (Q3 + 1.5 * IQR).item()

    #save data before outlier detection
    forecast_df = pd.DataFrame();
    forecast_df[['Local_datetime', 'dirty_forecast']] = df_Yhat;
    forecast_df['IBM_forecast'] = Input_Forecast_Data['%s' % (Training_var)];

    #This is slightly different than what happens in the real code
    Outlier_Conditions = (df_Yhat['forecast'] > upper_bound) | (df_Yhat['forecast'] < lower_bound)
    df_Yhat.loc[Outlier_Conditions,'forecast'] = Input_Forecast_Data.loc[Outlier_Conditions,'%s' % (Training_var)]
    print("outliers: " + str(Outlier_Conditions.sum()))
    forecast_df['forecast'] = df_Yhat['forecast'];
    forecast_df = forecast_df.reset_index()
    forecast_df = forecast_df.rename(columns={'index': 'Lead_Time'})
    forecast_df['stationUid'] = Input_Forecast_Data['stationUid'];
    forecast_df['metric'] = Training_var
    outliers = Outlier_Conditions.sum()
    return(forecast_df,outliers)
############### 1. Download IBM Forecast data

AS_Index = 0; # This should be a for loop but there's only one station right now
AccountUid = Account_Station.AccountUid[AS_Index]
StationUid = Account_Station.StationUid[AS_Index]
Provider = Account_Station.Provider[AS_Index]

IBM_Forecasts = api_bl.get_IBM_Forecasts(AccountUid,StationUid)
IBM_Forecasts

IBM_Forecasts.to_sql('IBM_Forecast_Data_Temp', engine, index=False, if_exists = 'replace')

############### 2. Save IBM Forecast data to database
##### Merge data with full table

name = "IBM_Forecast";
query_merge = '''
INSERT INTO %s_Data
SELECT %s_Data_Temp.*
FROM %s_Data_Temp
WHERE NOT EXISTS(SELECT * FROM %s_Data
WHERE %s_Data_Temp.Lead_Time = %s_Data.Lead_Time
AND %s_Data_Temp.Local_datetime = %s_Data.Local_datetime
AND %s_Data_Temp.stationUid = %s_Data.stationUid);
''' % (name,name,name,name,name,name,name,name,name,name)

SQL_bl.execute_query(connection_out, query_merge) # Execute our defined query

############### 3. Import PKLs and generate BL Forecasts
# Training_var = Training_vars[0] # This should be a for loop but there's only one variable right now

query = db.select([Pkl_table]).where(Pkl_table.columns.stationUid == StationUid)
Station_Data = SQL_bl.query_to_df(connection_in, query)

# unique_pkls = Station_Data.pkl_file_name.unique()

for index, station in Station_Data.iterrows():
    pkl_test_filename = station.pkl_file_name # This should be a for loop but there's only one variable right now
    print(pkl_test_filename)
    with open(pkl_path+pkl_test_filename, 'rb') as f:
        trainedmodel = pickle.load(f)
    Training_var = station.Metric
    forecasts, outliers = Forecast_From_Model(trainedmodel,IBM_Forecasts, Training_var)
    forecasts['pkl_file_name'] =  pkl_test_filename;

    outliers_df = pd.DataFrame();
    outliers_df[['stationUid','metric','pkl_file_name']]=forecasts[['stationUid','metric','pkl_file_name']].loc[[1]]
    outliers_df['Outlier_Count']=outliers

    week_og = station.weeks
    day_og = station.Model_Date
    days_since = abs(day_og - datetime.date.today()).days

    outliers_df[['weeks','Model_Date','Days_Since']]=[[week_og,day_og,days_since]]

    forecasts.to_sql('BL_Forecast_Data_Temp', engine, index=False, if_exists = 'replace')

    ##### Merge data with full table

    name = "BL_Forecast";
    query_merge = '''
    INSERT INTO %s_Data
    SELECT %s_Data_Temp.*
    FROM %s_Data_Temp
    WHERE NOT EXISTS(SELECT * FROM %s_Data
    WHERE %s_Data_Temp.Lead_Time = %s_Data.Lead_Time
    AND %s_Data_Temp.Local_datetime = %s_Data.Local_datetime
    AND %s_Data_Temp.pkl_file_name = %s_Data.pkl_file_name
    AND %s_Data_Temp.metric = %s_Data.metric);
    ''' % (name,name,name,name,name,name,name,name,name,name,name,name)

    SQL_bl.execute_query(connection_out, query_merge) # Execute our defined query

    outliers_df.to_sql('Outlier_Data', engine, index=False, if_exists = 'append')

    ##### Merge data with full table

    name = "Outlier";
    query_merge = '''
    INSERT INTO %s_Data
    SELECT %s_Data_Temp.*
    FROM %s_Data_Temp
    WHERE NOT EXISTS(SELECT * FROM %s_Data
    WHERE %s_Data_Temp.pkl_file_name = %s_Data.pkl_file_name
    AND %s_Data_Temp.metric = %s_Data.metric
    AND %s_Data_Temp.metric = %s_Data.metric);
    ''' % (name,name,name,name,name,name,name,name)

    # SQL_bl.execute_query(connection_out, query_merge) # Execute our defined query
