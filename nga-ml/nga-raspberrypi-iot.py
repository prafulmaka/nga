from picamera import PiCamera
from time import sleep
from PIL import Image
from numpy import asarray
from kafka import KafkaProducer
from json import dumps

camera = PiCamera()
camera.rotation = -90
while True:
    camera.capture('/home/pmaka/Desktop/iot-capture.jpg')
    sleep(5)

# Open image
img = Image.open("/home/pmaka/Desktop/iot-capture.jpg")

# Convert image to array
img_array = asarray(img)

# Convert image array to str
img_array_str = str(img_array)

# Produce Kafka message using img
producer = KafkaProducer(bootstrap_servers=['54.153.48.74:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

producer.send('quickstart-events', value=img_array_str)


                    

    