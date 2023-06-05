import os
import requests
import pandas as pd
import json
from picamera import PiCamera
from time import sleep
from numpy import asarray
from kafka import KafkaProducer
from json import dumps
import numpy as np
import tensorflow as tf
import cv2
from decouple import config

# Get Databricks API Key
API_KEY = config('API_KEY')


# Use Serving Endpoint
def create_tf_serving_json(data):
    return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}


def score_model(dataset):
    url = 'https://adb-8182502936076118.18.azuredatabricks.net/serving-endpoints/testy/invocations'
    headers = {"Authorization": f"Bearer {API_KEY}", 'Content-Type': 'application/json'}
    ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
    data_json = json.dumps(ds_dict, allow_nan=True)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')

    return response.json()


camera = PiCamera()
camera.rotation = -90
while True:
    camera.capture('/home/pmaka/Desktop/iot-capture.jpg')
    # Open image
    img = cv2.imread("/home/pmaka/Desktop/iot-capture.jpg")
    # Resize and format
    img_array_resize = tf.image.resize(img, (256, 256))
    data = np.expand_dims(img_array_resize / 255, 0)

    # Use Databricks Model Serving Endpoint to get prediction
    prediction = score_model(dataset=data)
    print(prediction)

    # Create and format Kafka msg payload
    msg = {
        "location": "room-1",
        "prediction": f"{prediction}"
    }

    msg = json.dumps(msg).encode('utf-8')

    # Produce Kafka message using img
    producer = KafkaProducer(bootstrap_servers=['54.219.197.22 :9092'])

    producer.send('quickstart-events', value=msg)

    # Script to run every 60 seconds
    sleep(60)
