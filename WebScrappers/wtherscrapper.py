import sys
import datetime as dt
import traceback
import requests
from mysql.connector import IntegrityError
from mysql.connector import *
from sqlalchemy import *
from sqlalchemy import Table, Column, Integer, Float, String, DateTime, log
from sqlalchemy import MetaData
from sqlalchemy import create_engine
import json


NAME = "Dublin"
WEATHER = "https://api.openweathermap.org/data/2.5/onecall"
LAT, LON = 53.344, -6.2672
WeatherAPI = "07daa9f106813518ee71c9fd2dc84557"
USER = "basra"
PASSWORD = "dbbikes11"
HOST = "dbbikes1@dbbikes.cyfo0q3xoqx7.us-east-1.rds.amazonaws.com"
PORT = "3306"
DATABASE = "dbbikes"


engine = create_engine(
    f'mysql+mysqlconnector://basra:dbbikes11@dbbikes.cyfo0q3xoqx7.us-east-1.rds.amazonaws.com/dbbikes', echo=True)
connection = engine.connect()


def write_to_db(table_name, values):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables.get(table_name)

    with engine.connect() as connection:
        for val in values:
            try:
                res = connection.execute(table.insert().values(val))
                log.debug(f"inserted {res.rowcount} rows into {table_name} for {val}")
            except IntegrityError as err:
                log.debug(f'Integrity error for {table_name}:{val} {err}')
                log.debug(f'OOPS {table_name}:{val} {traceback.format_exc()}')
    return


def initialise_db():
    metadata = MetaData(bind=engine)

    with engine.connect() as conn:

        if engine.dialect.has_table(conn, "current") is False:
            current = Table('current', metadata,
                            Column('dt', DateTime, primary_key=True),
                            Column('feels_like', Float),
                            Column('humidity', Integer),
                            Column('pressure', Integer),
                            Column('sunrise', DateTime),
                            Column('sunset', DateTime),
                            Column('temp', Float),
                            Column('uvi', Float),
                            Column('weather_id', Integer),
                            Column('wind_gust', Float),
                            Column('wind_speed', Float),
                            Column('rain_1h', Float),
                            Column('snow_1h', Float))

        if engine.dialect.has_table(conn, "hourly") is False:
            hourly = Table('hourly', metadata,
                           Column('dt', DateTime, primary_key=True),
                           Column('future_dt', DateTime, primary_key=True),
                           Column('feels_like', Float),
                           Column('humidity', Integer),
                           Column('pop', Float),
                           Column('pressure', Integer),
                           Column('temp', Float),
                           Column('uvi', Float),
                           Column('weather_id', Integer),
                           Column('wind_speed', Float),
                           Column('wind_gust', Float),
                           Column('rain_1h', Float),
                           Column('snow_1h', Float))

        if engine.dialect.has_table(conn, "daily") is False:
            daily = Table('daily', metadata,
                          Column('dt', DateTime, primary_key=True),
                          Column('future_dt', DateTime, primary_key=True),
                          Column('humidity', Integer),
                          Column('pop', Float),
                          Column('pressure', Integer),
                          Column('temp_max', Float),
                          Column('temp_min', Float),
                          Column('uvi', Float),
                          Column('weather_id', Integer),
                          Column('wind_speed', Float),
                          Column('wind_gust', Float),
                          Column('rain', Float),
                          Column('snow', Float))

            try:
                metadata.create_all(engine)
            except:
                traceback.formal_exc()

        return


def store_weather():
    try:
        req = requests.get(
            "http://api.openweathermap.org/data/2.5/onecall?id=524901&appid=07daa9f106813518ee71c9fd2dc84557&lat=53.344&lon=-6.2672&exclude=minutely,alerts")

        weather = req.json()

        curr_dt = dt.datetime.fromtimestamp(int(weather.get('current').get('dt')))
        write_to_db('current', [get_current(weather.get('current'))])
        write_to_db('hourly', [get_hourly(h, curr_dt) for h in weather.get('hourly')])
        write_to_db('daily', [get_daily(d, curr_dt) for d in weather.get('daily')])

    except:
        log.debug(traceback.format_exc())

    return


def get_current(curr):
    curr_dt = dt.datetime.fromtimestamp(int(curr.get('dt')))

    wind_gust = curr.get('wind_gust')
    if not wind_gust:
        wind_gust = 0

    rain = curr.get('rain')
    if rain:
        rain_1h = rain.get('1h')
    else:
        rain_1h = 0

    snow = curr.get('snow')
    if snow:
        snow_1h = snow.get('1h')
    else:
        snow_1h = 0

    return {'dt': curr_dt, 'feels_like': curr['feels_like'], 'humidity': curr['humidity'],
            'pressure': curr['pressure'], 'sunrise': dt.datetime.fromtimestamp(int(curr['sunrise'])),
            'sunset': dt.datetime.fromtimestamp(int(curr['sunset'])), 'temp': curr['temp'], 'uvi': curr['uvi'],
            'weather_id': curr['weather'][0]['id'], 'wind_speed': curr['wind_speed'], 'wind_gust': wind_gust,
            'rain_1h': rain_1h, 'snow_1h': snow_1h}


def get_hourly(h, curr_dt):
    h_wind_gust = h.get('wind_gust')
    if not h_wind_gust:
        h_wind_gust = 0

    h_rain = h.get('rain')
    if h_rain:
        h_rain_1h = h.get('rain').get('1h')
    else:
        h_rain_1h = 0

    h_snow = h.get('snow')
    if h_snow:
        h_rain_1h = h_snow.get('1h')
    else:
        h_snow_1h = 0

    return {'dt': curr_dt, 'future_dt': dt.datetime.fromtimestamp(int(h['dt'])),
            'feels_like': h['feels_like'], 'humidity': h['humidity'], 'pop': h['pop'],
            'pressure': h['pressure'], 'temp': h['temp'], 'uvi': h['uvi'], 'weather_id': h['weather'][0]['id'],
            'wind_speed': h['wind_speed'], 'wind_gust': h_wind_gust, 'rain_1h': h_rain_1h, 'snow_1h': h_snow_1h}


def get_daily(d, curr_dt):
    d_wind_gust = d.get('wind_gust')
    if not d_wind_gust:
        d_wind_gust = 0

    d_rain = d.get('rain')
    if not d_rain:
        d_rain = 0

    d_snow = d.get('snow')
    if not d_snow:
        d_snow = 0

    return {'dt': curr_dt, 'future_dt': dt.datetime.fromtimestamp(int(d['dt'])),
            'humidity': d['humidity'], 'pop': d['pop'],
            'pressure': d['pressure'], 'temp_max': d['temp']['max'],
            'temp_min': d['temp']['min'], 'uvi': d['uvi'], 'weather_id': d['weather'][0]['id'],
            'wind_speed': d['wind_speed'], 'wind_gust': d_wind_gust, 'rain': d_rain, 'snow': d_snow}




def main():
    initialise_db()

    while True:
        store_weather()

        time.sleep(5 * 60)

    return


if __name__ == "__main__":
    main()
