import configparser

import paho.mqtt.client as mqtt
import sqlite3
from time import time

import pathlib

TOPIC = 'ESP_Easy/#'


def on_connect(mqtt_client, user_data, flags, conn_result):
    mqtt_client.subscribe(TOPIC)


def on_message(mqtt_client, user_data, message):
    payload = message.payload.decode('utf-8')
    if(message.topic != 'ESP_Easy/status' ):
        db_conn = user_data['db_conn']
        sql = 'INSERT INTO sensors_data (topic, payload) VALUES (?, ?)'
        cursor = db_conn.cursor()
        cursor.execute(sql, (message.topic, payload))
        db_conn.commit()
        cursor.close()


def main():

    basedir = pathlib.Path(__file__).parent.resolve()
    config = configparser.ConfigParser()
    config.read(basedir / 'config.ini')


    db_conn = sqlite3.connect(config.get('mqqt_server','DATABASE_FILE'))
    sql = """
    CREATE TABLE IF NOT EXISTS sensors_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at DATE DEFAULT (datetime('now','localtime'))
    )
    """
    cursor = db_conn.cursor()
    cursor.execute(sql)
    cursor.close()

    mqtt_client = mqtt.Client(config.get('mqqt_server','MQTT_BROKER_URL').replace('.','_'))
    mqtt_client.username_pw_set(config.get('mqqt_server','MQTT_USERNAME'), config.get('mqqt_server','MQTT_PASSWORD'))
    mqtt_client.user_data_set({'db_conn': db_conn})

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(config.get('mqqt_server','MQTT_BROKER_URL'), config.getint('mqqt_server','MQTT_BROKER_PORT'))
    mqtt_client.loop_forever()


main()