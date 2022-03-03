# webscrapper.py
import sys
import datetime as dt
import traceback
import requests
import pytz
from mysql.connector import IntegrityError
from mysql.connector import *
from sqlalchemy import *
from sqlalchemy import Table, Column, Integer, Float, String, DateTime
from sqlalchemy import MetaData
from sqlalchemy import create_engine
import json



NAME = "Dublin"
STATIONS = "https://api.jcdecaux.com/vls/v1/stations"
APIKEY = "2eb32b43008bde9142e08158c2db562686d715f2"
USER = "basra"
PASSWORD = "dbbikes11"
HOST = "dbbikes1@dbbikes.cyfo0q3xoqx7.us-east-1.rds.amazonaws.com"
PORT = "3306"
DATABASE = "dbbikes"

engine = create_engine(
    f'mysql+mysqlconnector://basra:dbbikes11@dbbikes.cyfo0q3xoqx7.us-east-1.rds.amazonaws.com/dbbikes', echo=False)
connection = engine.connect()


def write_to_db(table_name, values):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables.get(table_name)

    with engine.connect() as connection:
        for val in values:
            try:
                res = connection.execute(table.insert().values(val))
            except IntegrityError as err:
    return


def initialise_db():
    metadata = MetaData(bind=engine)

    with engine.connect() as conn:
        if engine.dialect.has_table(conn, "station") is False:
            station = Table('station', metadata,
                            Column('number', Integer, primary_key=True),
                            Column('name', String(128)),
                            Column('address', String(128)),
                            Column('position_lat', Float),
                            Column('position_lng', Float),
                            Column('bike_stands', Integer),
                            )

            try:
                metadata.create_all(engine)
                # r = requests.get(STATIONS, params={"apiKey": APIKEY, "CONTRACT": NAME})
                r = requests.get(
                    "https://api.jcdecaux.com/vls/v1/stations?apiKey=2eb32b43008bde9142e08158c2db562686d715f2&contract=Dublin")

                values = map(get_stations, r.json())
                write_to_db("station", values)
            except:
                traceback.format_exc()

        if engine.dialect.has_table(conn, "availability") is False:
            availability = Table('availability', metadata,
                                 Column('number', Integer, primary_key=True),
                                 Column('last_update', DateTime, primary_key=True),
                                 Column('available_bike_stands', Integer),
                                 Column('available_bikes', Integer),
                                 Column('status', String(128)))

            try:
                metadata.create_all(engine)
            except:
                traceback.formal_exc()

        return


def get_stations(s):
    return {'number': s['number'], 'name': s['name'], 'address': s['address'], 'position_lat': s['position']['lat'], 'position_lng': s['position']['lng'], 'bike_stands': s['bike_stands']}


def store_availability():
    try:

        #req = requests.get(STATIONS, params={"apikey": APIKEY, "contract": NAME})
        req = requests.get(
            "https://api.jcdecaux.com/vls/v1/stations?apiKey=2eb32b43008bde9142e08158c2db562686d715f2&contract=Dublin")

        values = filter(lambda x: x is not None, map(get_availability, req.json()))
        write_to_db('availability', values)
    except:
        traceback.format_exc()

    return


def get_availability(s):
    if 'last_update' not in s or s['last_update'] is None:
        return None
    return {'number': int(s['number']), 'available_bike_stands': int(s['available_bike_stands']),
            'available_bikes': int(s['available_bikes']),
            'last_update': datetime.datetime.fromtimestamp(int(s['last_update'] / 1e3)), 'status': s['status']}


def main():
    initialise_db()

    while True:
        now = dt.datetime.now(tz=pytz.timezone('Europe/Dublin')).time()
        if now >= dt.time(5, 0) or now <= dt.time(0, 30):  # exclude 12:30 = 05:00
            store_availability()

        time.sleep(5 * 60)

    return


if __name__ == "__main__":
    main()
