import json
from bson import json_util
import pymysql
from flask import Flask, render_template, request
from jinja2 import Template
from sqlalchemy import create_engine, select, MetaData, Table, and_
import dbinfo_for_group
import pandas as pd
from joblib import dump, load
from flask_googlecharts import LineChart

app = Flask(__name__)


@app.route("/")
def hello():
    return render_template("index.html")


@app.route("/stations")
def stations():
    engine = create_engine(
        f"mysql+mysqlconnector://{dbinfo_for_group.USERNAME}:{dbinfo_for_group.PASSWORD}@{dbinfo_for_group.HOST}"
        f":3306/{dbinfo_for_group.DATABASE}", echo=True)

    sql = "SELECT s.number, s.name, s.address, s.pos_lat, s.pos_lng, a.available_bike_stands, a.available_bikes, " \
          "a.status, MAX(a.last_update) AS `current_availability` " \
          "FROM dbbikes.availability as a " \
          "INNER JOIN dbbikes.stations as s ON s.number = a.number " \
          "GROUP BY s.number " \
          "ORDER BY s.number;"

    df = pd.read_sql(sql, engine)

    return df.to_json(orient="records")


@app.route("/static_stations")
def static_stations():
    engine = create_engine(
        f"mysql+mysqlconnector://{dbinfo_for_group.USERNAME}:{dbinfo_for_group.PASSWORD}@{dbinfo_for_group.HOST}"
        f":3306/{dbinfo_for_group.DATABASE}", echo=True)

    sql = "SELECT * FROM dbbikes.station " \
          "ORDER BY name;"

    df = pd.read_sql(sql, engine)

    return df.to_json(orient="records")


@app.route('/occupancy/<int:station_id>')
def get_occupancy(station_id):
    engine = create_engine(
        f"mysql+mysqlconnector://{dbinfo_for_group.USERNAME}:{dbinfo_for_group.PASSWORD}@{dbinfo_for_group.HOST}"
        f":3306/{dbinfo_for_group.DATABASE}", echo=True)

    sql = f"""SELECT s.name, avg(a.available_bike_stands) as Avg_bike_stands,
        avg(a.available_bikes) as Avg_bikes_free, DAYNAME(a.last_update) as DayName
        FROM dbbikes.availability as a
        JOIN dbbikes.station as s
        ON s.number = a.number
        WHERE s.number = {station_id}
        GROUP BY s.name , DayName 
        ORDER BY s.name , DayName;"""

    df = pd.read_sql(sql, engine)

    return df.to_json(orient="records")


@app.route('/hourly/<int:station_id>')
def get_hourly_data(station_id):
    engine = create_engine(
        f"mysql+mysqlconnector://{dbinfo_for_group.USERNAME}:{dbinfo_for_group.PASSWORD}@{dbinfo_for_group.HOST}"
        f":3306/{dbinfo_for_group.DATABASE}", echo=True)

    sql = f"""SELECT s.name,count(a.number),avg(available_bike_stands) as Avg_bike_stands,
        avg(available_bikes) as Avg_bikes_free,EXTRACT(HOUR FROM last_update) as Hourly
        FROM dbbikes.availability as a
        JOIN dbbikes.station as s
        ON s.number = a.number
        WHERE a.number = {station_id}
        GROUP BY EXTRACT(HOUR FROM last_update) 
        ORDER BY EXTRACT(HOUR FROM last_update) asc"""

    df = pd.read_sql(sql, engine)

    return df.to_json(orient="records")


@app.route("/weather_forecast")
def weather_forecast():
    engine = create_engine(
        f"mysql+mysqlconnector://{dbinfo_for_group.USERNAME}:{dbinfo_for_group.PASSWORD}@{dbinfo_for_group.HOST}"
        f":3306/{dbinfo_for_group.DATABASE}", echo=True)

    sql = f"""SELECT ROUND((temp - 273),2) AS "temp", rain_1h, humidity, wind_speed
    FROM dbbikes.current
    ORDER BY dt DESC
    LIMIT 1;"""

    df = pd.read_sql(sql, engine)

    return df.to_json(orient="records")


@app.route('/predict/<int:station_id>')
def predict(station_id):
    model = load('availabilitypredictions.joblib')
    avail_predict = model.predict([[station_id, 2, 5, 2, 4]])

    predict_list = avail_predict.tolist()
    predict_dict = {"bikes": predict_list[0]}
    result = json.dumps(predict_dict)

    return result


# @app.route('/predict/<int:station_id><rain><temp><wind_speed><int:day_of_week>')
# def predict(station_id, rain, temp, wind_speed, day_of_week):
#     model = load('availabilitypredictions.joblib')
#     predicted_bikes = model.predict([[station_id, rain, temp, wind_speed, day_of_week]])
#
#     return str(predicted_bikes[0])


if __name__ == "__main__":
    # app.run(debug=True)
    app.run(host="0.0.0.0", port=5000, debug=True)
