# This file is meant to generate pkls and create the initial statistics

# Custom API + SQL functions
import sys
from olm import *
sys.path.insert(0, '/home/mborrus/Model-Validation-GCP/ds-common/')
import API_Function_Library.SQL_Functions as SQL_bl

# Import libraries
import sqlalchemy as db
import pandas as pd
import numpy as np
import datetime
import pickle
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# Import variables
from Namelist import db_info
from Namelist import Account_Station
from Namelist import query_build
from Namelist import Training_vars, compare_count, Neurons
from Namelist import pkl_path
##############

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
Station_Data_table = db.Table('Station_Data', metadata, autoload=True, autoload_with=engine)
IBM_Data_table = db.Table('IBM_Data', metadata, autoload=True, autoload_with=engine)
###############

def Train_Model(Input_Data, Training_var):
    Input_Data=Input_Data.dropna()
    Xdata=Input_Data['%s_IBM' % (Training_var)].to_numpy()
    Ydata=Input_Data['%s_Sensor' % (Training_var)].to_numpy()

    comparison = Input_Data['%s_IBM' % (Training_var)].astype(float)
    comparison_lectures = Input_Data['%s_Sensor' % (Training_var)].astype(float)

    XData=Xdata.reshape(Xdata.shape[0],1).astype(float) #first version
    YData=Ydata.reshape(Ydata.shape[0],1).astype(float)
    n_blocks=5 # For k folding?
    rowstodrop=len(Xdata)%n_blocks
    XData=XData[rowstodrop:]
    YData=YData[rowstodrop:]
    hn = Neurons # define number of hidden neurons (it can change)

    TrainedElm=Elm_train(XData,YData,hn,1,1)
    # TrainedElm=Elm_cross_val(XData,YData,hn)
    logits = Elm_predict(TrainedElm,XData)

    Q1 = comparison_lectures.quantile(0.10)
    TrainedElm['q1'] = Q1

    Q3 = comparison_lectures.quantile(0.90)
    TrainedElm['q3'] = Q3

    weeks = abs(round((Input_Data['Local_datetime'][rowstodrop:].min()-Input_Data['Local_datetime'][rowstodrop:].max()).days/7))

    performance_stats = {'MSE_Forecast': [mean_squared_error(YData, logits[0])],
        'MSE_IBM': [mean_squared_error(YData, comparison[rowstodrop:])],
            'MAE_Forecast':[mean_absolute_error(YData, logits[0])],
            'MAE_IBM':[mean_absolute_error(YData, comparison[rowstodrop:])],
            'r2_Forecast':[r2_score(YData, logits[0])],
            'r2_IBM':[r2_score(YData, comparison[rowstodrop:])],
            'weeks': weeks,
            'Model_Date': datetime.date.today(),
            'Days_Since': (datetime.date.today() - datetime.date.today()).days
        }
    df_performance_temp = pd.DataFrame(performance_stats, columns=['MSE_Forecast','MSE_IBM','MAE_Forecast','MAE_IBM','r2_Forecast','r2_IBM','weeks','Model_Date','Days_Since'])
    return(TrainedElm, df_performance_temp)

def Gladiator_Ring(Input_Data, compare_count=20):
    [model_candidate, perf_candidate]= Train_Model(Input_Data, Training_var)
    for compare in range(0,compare_count):
        [model_gladiator, perf_gladiator]= Train_Model(Input_Data, Training_var)
        if perf_gladiator['MSE_Forecast'].item() <= perf_candidate["MSE_Forecast"].item():
            model_candidate=model_gladiator;
            perf_candidate=perf_gladiator;
            # print("Are you not entertained")
    return(model_candidate, perf_candidate)

###############

AS_Index = 0; # This should be a for loop but there's only one station right now
AccountUid = Account_Station.AccountUid[AS_Index]
StationUid = Account_Station.StationUid[AS_Index]
Provider = Account_Station.Provider[AS_Index]

## Station Data
query = db.select([Station_Data_table]).where(Station_Data_table.columns.stationUid == StationUid)
Station_Data = SQL_bl.query_to_df(connection_in, query)
Station_Data = Station_Data.sort_values(by=['Local_datetime'])

query = db.select([IBM_Data_table]).where(IBM_Data_table.columns.stationUid == StationUid)
IBM_Data = SQL_bl.query_to_df(connection_in, query)
IBM_Data = IBM_Data.sort_values(by=['Local_datetime'])

Training_Data_All = pd.merge(Station_Data,IBM_Data, how='inner', on = 'Local_datetime',suffixes=('_Sensor', '_IBM'))

##############

first_date = Training_Data_All.Local_datetime.min()
last_date = Training_Data_All.Local_datetime.max()
date_range = round((last_date - first_date).days/7)

if date_range>=60:
    weeks_count = 60;
    weeks_range = [1,2,3,4,5,6,7,*range(8,weeks_count+1,2)];
elif date_range<=0:
    print("issues with dates (0 or neg)")
    weeks_range = []
elif date_range>=0 & date_range<=8:
    weeks_count = date_range
    weeks_range = [*range(1,weeks_count+1)];
else:
    weeks_count = date_range

# weeks_count = 1 # Uncomment this for shorter testing
compare_count = compare_count; #set in namelist
for Training_var in Training_vars:
# Training_var = Training_vars[0] #set in namelist

    df_performance_all = pd.DataFrame()
    File_Names = [];

    for week_length in weeks_range:
        print(str(week_length))
        Start_date = datetime.date.today()
        End_date = Start_date - datetime.timedelta(weeks=week_length)
        range_eval = pd.eval('(Training_Data_All.Local_datetime.dt.date > End_date) & (Training_Data_All.Local_datetime.dt.date < Start_date)')
        Data_cut = Training_Data_All[range_eval].reset_index()
        [model_candidate, perf_candidate]= Train_Model(Data_cut, Training_var)
        for compare in range(0,compare_count):
            [model_gladiator, perf_gladiator]= Train_Model(Data_cut, Training_var)
            if perf_gladiator['MSE_Forecast'].item() <= perf_candidate["MSE_Forecast"].item():
                model_candidate=model_gladiator;
                perf_candidate=perf_gladiator;
                # print("Are you not entertained")
        df_performance_all = df_performance_all.append(perf_candidate, ignore_index = True)

        filename = AccountUid+'_'+StationUid+'_'+Training_var+'_'+str(Start_date)+'_'+str(week_length)+".pkl"
        File_Names.append(filename)
        f = open(pkl_path+filename,"wb")
        pickle.dump(model_candidate,f)
        f.close()

    df_performance_all['stationUid'] = StationUid;
    df_performance_all['Metric'] = Training_var;
    df_performance_all['pkl_file_name'] = File_Names;

    df_performance_all.to_sql('Pkl_Performance_Data_Temp', engine, index=False, if_exists = 'replace')

    name = "Pkl_Performance";
    query_merge = '''
    INSERT INTO %s_Data
    SELECT %s_Data_Temp.*
    FROM %s_Data_Temp
    WHERE NOT EXISTS(SELECT * FROM %s_Data
    WHERE %s_Data_Temp.weeks = %s_Data.weeks
    AND %s_Data_Temp.Model_Date = %s_Data.Model_Date
    AND %s_Data_Temp.Days_Since = %s_Data.Days_Since
    AND %s_Data_Temp.Metric = %s_Data.Metric
    AND %s_Data_Temp.stationUid = %s_Data.stationUid);
    ''' % (name,name,name,name,name,name,name,name,name,name,name,name,name,name)

    SQL_bl.execute_query(connection_out, query_merge) # Execute our defined query
