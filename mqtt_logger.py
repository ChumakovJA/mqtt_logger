import configparser

import paho.mqtt.client as mqtt
import pymysql

import pathlib

TOPIC = 'ESP_Easy/#'
#connection = None

def on_connect(mqtt_client, user_data, flags, conn_result):
    mqtt_client.subscribe(TOPIC)


def on_message(mqtt_client, user_data, message):
    payload = message.payload.decode('utf-8')
    if message.topic.startswith('ESP_Easy/status') != True :
        db_conn = user_data['db_conn']
        with db_conn.cursor() as cursor:
           insert_query = "INSERT INTO status_topic (topic, value) VALUES ('" + message.topic + "', '" + payload + "');"
           cursor.execute(insert_query)
           db_conn.commit()

def main():

    basedir = pathlib.Path(__file__).parent.resolve()
    config = configparser.ConfigParser()
    config.read(basedir / 'config.ini')

    try:
        connection = pymysql.connect(
            host=config.get('mysql_server','host'),
            port=config.getint('mysql_server','port'),
            user=config.get('mysql_server','user'),
            password=config.get('mysql_server','password'),
            database=config.get('mysql_server','database'),
            cursorclass=pymysql.cursors.DictCursor
        )
        print("successfully connected...")

    except Exception as ex:
        print("Connection refused...")

    mqtt_client = mqtt.Client(config.get('mqqt_server','MQTT_BROKER_URL').replace('.','_'))
    mqtt_client.username_pw_set(config.get('mqqt_server','MQTT_USERNAME'), config.get('mqqt_server','MQTT_PASSWORD'))
    mqtt_client.user_data_set({'db_conn': connection})

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(config.get('mqqt_server','MQTT_BROKER_URL'), config.getint('mqqt_server','MQTT_BROKER_PORT'))
    mqtt_client.loop_forever()


main()