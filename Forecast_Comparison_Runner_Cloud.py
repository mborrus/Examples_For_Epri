# This file is meant to Run Statistics on the BL forecasts

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
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import datetime

# Import variables
from Namelist import db_info
from Namelist import Account_Station
from Namelist import Training_vars, Lead_Time_bins

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

###############

# Load in PKL table to get station and pkl information
metadata = db.MetaData()
Pkl_table = db.Table('Pkl_Performance_Data', metadata, autoload=True, autoload_with=engine)

# Select which station you want to test
AS_Index = 0; # This should be a for loop but there's only one station right now
AccountUid = Account_Station.AccountUid[AS_Index]
StationUid = Account_Station.StationUid[AS_Index]
Provider = Account_Station.Provider[AS_Index]

# Get just the PKL information for the station you're interested in
# Station ID matches, Date is before today (save a few seconds of running)
# In a world where I have more than one metric, this would run for both at once I believe
query = db.select([Pkl_table]).where(db.and_(Pkl_table.columns.stationUid == StationUid,Pkl_table.columns.Model_Date < datetime.date.today()))

Pkl_table_df = SQL_bl.query_to_df(connection_in, query)

# Get db tables for BL forecasts and station data (sensor readings)
df_performance_all = pd.DataFrame()
BL_Forecast_table = db.Table('BL_Forecast_Data', metadata, autoload=True, autoload_with=engine)
Station_Data_table = db.Table('Station_Data', metadata, autoload=True, autoload_with=engine)

# Range through all available pkl names to calculate the forecast statistics at different forecast hours

for pkl_index in range(0,len(Pkl_table_df.pkl_file_name.unique())):
# for pkl_index in range(0,2):
    print(pkl_index)
    first_test_pkl = Pkl_table_df.pkl_file_name[pkl_index]
    Pkl_table_df

    query = db.select([BL_Forecast_table]).where(BL_Forecast_table.columns.pkl_file_name == first_test_pkl )
    BL_Forecast_table_df = SQL_bl.query_to_df(connection_in, query)

    # training var is already in pkl name
    Training_var = Pkl_table_df.Metric[pkl_index]
    
    # Lead time bins go in 24 hour bunches, each bin is 1 day
    for lead_time_bin in range(0,15):

        #Get forecast data for pkl specified and the range of forecast hours
        conditions = (BL_Forecast_table_df['Lead_Time'] >= min(Lead_Time_bins[lead_time_bin])) & (BL_Forecast_table_df['Lead_Time'] <= max(Lead_Time_bins[lead_time_bin]))
        Forecast_data_binned = BL_Forecast_table_df.loc[conditions]

        #get dates of forecast for query in sensor data
        date_overlap = Forecast_data_binned['Local_datetime']
        query = db.select([Station_Data_table]).where(db.and_(Station_Data_table.columns.stationUid == StationUid, Station_Data_table.columns.Local_datetime.in_(date_overlap)))
        Sensor_data_binned = SQL_bl.query_to_df(connection_in, query)

        #If more the 24 data points, calculate performance stats
        if len(Sensor_data_binned) > 24:
            Data_All = pd.merge(Sensor_data_binned,Forecast_data_binned, how='inner', on = 'Local_datetime')
            IBM = Data_All["IBM_forecast"]
            BL = Data_All["forecast"]
            Sensor = Data_All[Training_var]

            week_og = Pkl_table_df.weeks[pkl_index]
            day_og = Pkl_table_df.Model_Date[pkl_index]

            performance_stats = {
                        'MSE_Forecast': [mean_squared_error(Sensor, BL)],
                        'MSE_IBM': [mean_squared_error(Sensor, IBM)],
                        'MAE_Forecast':[mean_absolute_error(Sensor, BL)],
                        'MAE_IBM':[mean_absolute_error(Sensor, IBM)],
                        'r2_Forecast':[r2_score(Sensor, BL)],
                        'r2_IBM':[r2_score(Sensor, IBM)],
                        'weeks': week_og,
                        'Model_Date': day_og,
                        'Days_Since': abs((day_og - Data_All["Local_datetime"].max().date()).days)
                    }
            df_performance_temp = pd.DataFrame(performance_stats, columns=['MSE_Forecast','MSE_IBM','MAE_Forecast','MAE_IBM','r2_Forecast','r2_IBM','weeks','Model_Date','Days_Since'])

            df_performance_temp['stationUid'] = Pkl_table_df.stationUid[pkl_index] ;
            df_performance_temp['Metric'] = Pkl_table_df.Metric[pkl_index];
            df_performance_temp['pkl_file_name'] = Pkl_table_df.pkl_file_name[pkl_index];
            df_performance_temp['Lead_Time_Bin'] = lead_time_bin; 
            df_performance_all = df_performance_all.append(df_performance_temp, ignore_index = True)

df_performance_all.to_sql('Pkl_Online_Performance_Data_Temp', engine, index=False, if_exists = 'replace')

name = "Pkl_Online_Performance";
query_merge = '''
INSERT INTO %s_Data
SELECT %s_Data_Temp.*
FROM %s_Data_Temp
WHERE NOT EXISTS(SELECT * FROM %s_Data 
WHERE %s_Data_Temp.pkl_file_name = %s_Data.pkl_file_name 
AND %s_Data_Temp.Days_Since = %s_Data.Days_Since 
AND %s_Data_Temp.Lead_Time_Bin = %s_Data.Lead_Time_Bin);
''' % (name,name,name,name,name,name,name,name,name,name)

SQL_bl.execute_query(connection_out, query_merge) # Execute our defined query