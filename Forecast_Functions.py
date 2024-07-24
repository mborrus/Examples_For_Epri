""""
This module supports pulling data from the Benchmark API 
and the live data from zentra and davis weather stations
@author: Marshall Borrus
"""

import json
import requests
import pandas as pd
import datetime
from datetime import date
import time
import csv

def Help():
    """
    Prints the available functions and required inputs
    """
    print('Login(username,password,Account_UID)')
    print('Station_Info(station_Account_UID)')
    print('Provider_Info(station_Account_UID,Provider_UID)')
    print('Hourly_API_Data(station_Account_UID ,Station_UID,Metric,Start_Date,End_Date)')
    print('Historical_API_Data(station_Account_UID,Station_UID,Metric,Start_Date,End_Date,Sensor_Provider)')
    print('Historical_API_Data(station_Account_UID,Station_UID,Metric,Start_Date,End_Date,Sensor_Provider)')
    print('get_Zentra_Cloud(device_sn,start_month,start_day,end_month,end_day)')
    print('get_Davis_data(station_id,start_datetime,end_datetime,sensor_index)')

def Login(username,password,Account_UID):
    '''
    This logs you into the API and returns the auth key
    '''

    url = "https://api.benchmarklabs.com/api/v1/"+Account_UID+"/authentication/login"

    payload = json.dumps({
      "username": username,
      "password": password,
      "deviceId": "deviceId"
    })
    headers = {
      'Content-Type': 'application/json',
      'Cookie': 'JSESSIONID=7DD8740820BC48680E3175B686BD2589' #Does the cookie ever need to change?
    }

    #Convert response file into text and then into a json format
    response_login = requests.request("POST", url, headers=headers, data=payload)
    response_login = json.loads(response_login.text)
    Auth_Key = str(response_login['token']); #This pulls just the bearer token
    Login_Time = time.time()
    return(Auth_Key,Login_Time)

def Station_Info(station_Account_UID, username = "Remove_For_Example", password = "Remove_For_Example", login_Account_UID = "Remove_For_Example"):
    '''
    This gets you all the station infomation, primarily so you can get the timezone for conversions.
    We'll have issues with this once we go beyond 300 stations
    '''
    url = "https://api.benchmarklabs.com/api/v1/"+station_Account_UID+"/station/list?size=300&page=0"

    if 'login_time' not in locals():
        [Auth_Key,login_time] = Login(username,password,login_Account_UID);
    elif abs(login_time-time.time()) > 60*5:
        [Auth_Key,login_time] = Login(username,password,login_Account_UID);

    payload = json.dumps({
    })
    headers = {
      'Authorization': 'Bearer ' + Auth_Key,
      'Content-Type': 'application/json',
      'Cookie': 'JSESSIONID=7DD8740820BC48680E3175B686BD2589'
    }

    #Convert response file into text and then into a json format
    response_station = requests.request("GET", url, headers=headers, data=payload)
    response_station = json.loads(response_station.text)
    Station_Data = pd.DataFrame.from_dict(response_station['content'])
    Station_Data = Station_Data.rename(columns={"uid":"stationUid"})
    return(Station_Data)

def Provider_Info(station_Account_UID,Provider_UID,  username = "Remove_For_Example", password = "Remove_For_Example", login_Account_UID = "Remove_For_Example"):
    '''
    This gets you all the provider infomation, primarily so you can get the particular provider key that is used
    i.e. WEATHER_LINK, or METER
    '''
    url = "https://api.benchmarklabs.com/api/v1/"+station_Account_UID+"/data-provider/"+Provider_UID

    if 'login_time' not in locals():
        [Auth_Key,login_time] = Login(username,password,login_Account_UID);
    elif abs(login_time-time.time()) > 60*5:
        [Auth_Key,login_time] = Login(username,password,login_Account_UID);

    payload = json.dumps({
    })
    headers = {
      'Authorization': 'Bearer ' + Auth_Key,
      'Content-Type': 'application/json',
      'Cookie': 'JSESSIONID=7DD8740820BC48680E3175B686BD2589'
    }

    #Convert response file into text and then into a json format
    response_station = requests.request("GET", url, headers=headers, data=payload)
    try:
        response_station.raise_for_status()
    except requests.exceptions.HTTPError:
        # Gave a 500 or 404
        print("station data - there was an error with status code:",response_station.status_code)
        Provider_Data = response_station
    else:
        print("station data - Requrest worked, returning station data")
        response_station = json.loads(response_station.text)
        Provider_Data = pd.DataFrame(response_station,index=[0])
    return(Provider_Data)

def Hourly_API_Data(station_Account_UID ,Station_UID,Metric,Start_Date,End_Date, username = "Remove_For_Example", password = "Remove_For_Example", login_Account_UID = "Remove_For_Example"):
    '''
    This returns hourly forecast data
    '''
    url = "https://api.benchmarklabs.com/api/v1/"+station_Account_UID+"/reporting/"+Station_UID+"/hourly/"+Metric+"/"+Start_Date+"/"+End_Date

    if 'login_time' not in locals():
        [Auth_Key,login_time] = Login(username,password,login_Account_UID);
    elif abs(login_time-time.time()) > 60*5:
        [Auth_Key,login_time] = Login(username,password,login_Account_UID);

    payload = json.dumps({
    })
    headers = {
      'Authorization': 'Bearer ' + Auth_Key,
      'Content-Type': 'application/json',
      'Cookie': 'JSESSIONID=7DD8740820BC48680E3175B686BD2589'
    }
    Forecasts_response = requests.request("GET", url, headers=headers, data=payload)
    output = json.loads(Forecasts_response.text);
    try:
        Hourly_Data = pd.DataFrame.from_dict(output['forecasts'])
        Hourly_Data["localDateTime"] = pd.to_datetime(Hourly_Data["localDateTime"])
        Hourly_Data["localDateTime"] = Hourly_Data.localDateTime.dt.tz_localize(Hourly_Data['timeZone'][0])
    except:
        print("there was an error with station id #" + Station_UID)
        Hourly_Data = output
    return(Hourly_Data)

def Historical_API_Data(station_Account_UID,Station_UID,Metric,Start_Date,End_Date,Sensor_Provider, username = "Remove_For_Example", password = "Remove_For_Example", login_Account_UID = "Remove_For_Example",Time_From_Zero_Hour = "24"):
    """
    Returns historical api data for both GFS and the UNIFIER (either IBM or benchmark, not specified in data)
    """
    url = "https://api.benchmarklabs.com/api/v1/"+station_Account_UID+"/export/"+Station_UID+"/comparison/historic/"+Metric+"/"+Start_Date+"/"+End_Date+"/"+Time_From_Zero_Hour+"?providers=UNIFIER&providers=NOAA_GFS&providers="+Sensor_Provider;

    if 'login_time' not in locals():
        [Auth_Key,login_time] = Login(username,password,login_Account_UID);
    elif abs(login_time-time.time()) > 60*5:
        [Auth_Key,login_time] = Login(username,password,login_Account_UID);

    payload = json.dumps({
    })
    headers = {
      'Authorization': 'Bearer ' + Auth_Key,
      'Content-Type': 'application/json',
      'Cookie': 'JSESSIONID=7DD8740820BC48680E3175B686BD2589'
    }

    Forecasts_response = requests.request("GET", url, headers=headers, data=payload)
    try:
        Forecasts_response.raise_for_status()
    except requests.exceptions.HTTPError:
        # Gave a 500 or 404
        print("there was an error with status code:",Forecasts_response.status_code)
    else:
        print("Requrest worked, parsing data")
    output =Forecasts_response.text;

    try:
        lines = output.splitlines()
        reader = csv.reader(lines)
        parsed_csv = list(reader)

        API_Data = pd.DataFrame(parsed_csv[1:], columns = parsed_csv[0])
        API_Data["UTC_TIME"] = pd.to_datetime(API_Data["dateTime"])
        ### NEED TO CHANGE THIS TIME ZONE WHICH IS HARD CODED - COULD CALL STATION INFORMATION AND GET TIME ZONE FROM THAT

        if 'Station_Data' not in locals():
            Station_Data = Station_Info(station_Account_UID)
        elif Station_Data['stationUid'].item() != Station_UID:
            Station_Data = Station_Info(station_Account_UID)

        TIMEZONE = Station_Data['timeZone'][Station_Data['stationUid'] == Station_UID]
        API_Data["localDateTime"] = API_Data.UTC_TIME.dt.tz_convert(TIMEZONE.item())

        API_Data['NOAA_GFS'] = pd.to_numeric(API_Data['NOAA_GFS'], errors='coerce')
        ### NOTE HOW BENCHMARK == UNIFIER FOR SIMPLICITY'S SAKE
        API_Data['BENCHMARK'] = pd.to_numeric(API_Data['UNIFIER'], errors='coerce')
        API_Data[Sensor_Provider] = pd.to_numeric(API_Data[Sensor_Provider], errors='coerce')
    except:
        print("there was an error with station id #" + Station_UID)
        API_Data = output

    return(API_Data)

def Forecast_Compare_API_Data(station_Account_UID,Station_UID,Metric,Start_Date,End_Date,Time_From_Zero_Hour = "24", username = "Remove_For_Example", password = "Remove_For_Example", login_Account_UID = "Remove_For_Example"):
    """
    Provides statistics such as MSE and RMSE for the available datasources
    """
    url = "https://api.benchmarklabs.com/api/v1/"+station_Account_UID+"/reporting/"+Station_UID+"/comparison/forecast/"+Metric+"/"+Start_Date+"/"+End_Date+"/"+Time_From_Zero_Hour+"?providers=UNIFIER&providers=NOAA_GFS&providers=IBM_GRAF&providers=BENCHMARK";
    Auth_Key = Login(username,password,login_Account_UID);
    payload = json.dumps({
    })
    headers = {
      'Authorization': 'Bearer ' + Auth_Key,
      'Content-Type': 'application/json',
      'Cookie': 'JSESSIONID=7DD8740820BC48680E3175B686BD2589'
    }

    Forecasts_response = requests.request("GET", url, headers=headers, data=payload)
    try:
        Forecasts_response.raise_for_status()
    except requests.exceptions.HTTPError:
        # Gave a 500 or 404
        print("there was an error with status code:",Forecasts_response.status_code)
    else:
        print("it worked")
    output = json.loads(Forecasts_response.text);
    try:
        API_Data = pd.DataFrame.from_dict(output['comparison'])
        API_Data["UTC_TIME"] = pd.to_datetime(API_Data["Date"])
        ### NEED TO CHANGE THIS TIME ZONE WHICH IS HARD CODED - COULD CALL STATION INFORMATION AND GET TIME ZONE FROM THAT
        Station_Data = Station_Info(station_Account_UID)
        TIMEZONE = Station_Data['timeZone'][Station_Data['stationUid'] == Station_UID]
        API_Data["localDateTime"] = API_Data.UTC_TIME.dt.tz_convert(TIMEZONE.item())

        API_Data['IBM_GRAF'] = pd.to_numeric(API_Data['IBM_GRAF'], errors='coerce')
        API_Data['NOAA_GFS'] = pd.to_numeric(API_Data['NOAA_GFS'], errors='coerce')
        API_Data['UNIFIER'] = pd.to_numeric(API_Data['UNIFIER'], errors='coerce')
        API_Data['BENCHMARK'] = pd.to_numeric(API_Data['BENCHMARK'], errors='coerce')
    except:
        print("there was an error with station id #" + Station_UID)
        API_Data = output
    return(API_Data)

def get_Zentra_Cloud(device_sn,start_month,start_day,end_month,end_day):
    """
    Gets sensor data directly from zentra cloud for weather stations
    """
    # get_readings example
    # device_sn = "z6-15959"
    token = "Token {TOKEN}".format(TOKEN="Token_#_Removed_for_Example")
    url = "https://zentracloud.com/api/v3/get_readings/"
    headers = {'content-type': 'application/json', 'Authorization': token}
    output_format = "df"
    end_date = date(2022, end_month, end_day)
    start_date = date(2022, start_month, start_day)
    per_page = "3000"
    page_num = "1"
    params = {'device_sn': device_sn, 'start_date': start_date, 'end_date': end_date, 'output_format': output_format, 'per_page': per_page, 'page_num': page_num}

    response = requests.get(url, params=params, headers=headers)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        # Gave a 500 or 404
        print("there was an error with status code:",response.status_code)
        if response.status_code == 429:
            print("too many requests, waiting 1 minute...")
            time.sleep(60)
            print("Trying again")
            response = requests.get(url, params=params, headers=headers)
    else:
        print("Zentra Cloud data collected")
    content = json.loads(response.content)
    df = pd.read_json(content['data'], convert_dates=False, orient='split')
    # df
    return(df)

import collections
import hashlib
import hmac
import time
import math


def get_Davis_data(station_id,start_datetime,end_datetime,sensor_index):
    """
    Gets sensor data directly from Davis weather stations
    """
    print('dont forget to set the correct sensor_index, set to 999 for full list')
    start_unix = math.floor(time.mktime(start_datetime.timetuple()))
    end_unix = math.floor(time.mktime(end_datetime.timetuple()))
    df_all = pd.DataFrame()

    if sensor_index == 999:
        print("all sensors included, use index to select correct sensor")
    else:
        print("only sensor Number: " + str(sensor_index))
    for daterange_start in range(start_unix,end_unix,86400):
#         print(daterange_start)
        parameters = {
          "api-key": "API_KEY_Removed_for_Example",
          "api-secret": "API_SECRET_Removed_for_Example",
          "station-id": station_id, # this is an example station ID, you need to replace it with your real station ID which you can retrieve by making a call to the /stations API endpoint
          "t": int(time.time()),
          "start-timestamp": daterange_start,
          "end-timestamp": daterange_start+86400
        }

        parameters = collections.OrderedDict(sorted(parameters.items()))

        apiSecret = parameters["api-secret"];
        parameters.pop("api-secret", None);

        data = ""
        for key in parameters:
          data = data + key + str(parameters[key])

        apiSignature = hmac.new(
          apiSecret.encode('utf-8'),
          data.encode('utf-8'),
          hashlib.sha256
        ).hexdigest()

        url = "https://api.weatherlink.com/v2/historic/{}?api-key={}&api-signature={}&end-timestamp={}&start-timestamp={}&t={}".format(parameters["station-id"], parameters["api-key"], apiSignature,parameters["end-timestamp"],parameters["start-timestamp"],parameters["t"])

        payload={}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        content = json.loads(response.content)
        if sensor_index == 999:
            content_df = content['sensors']
        else:
            content_df = content['sensors'][sensor_index]['data']
        print(datetime.datetime.fromtimestamp(daterange_start))
        type(content_df)
        df = pd.DataFrame.from_dict(content_df)
        df_all = df_all.append(df)
    return(df_all)
