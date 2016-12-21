#!/usr/bin/env python
import pika
import cv2


"""
Verbindungsdaten RabbitMQ

"""

# Change Credentials in Order to connect to RabbitMQ
credentials = pika.PlainCredentials('rabbitmq', 'TJ3EzFdWEK3zVJE4')
connection = pika.BlockingConnection(pika.ConnectionParameters(
        'localhost', 5672, '/', credentials))
channel = connection.channel()

# Create task_queue and make it durable
channel.queue_declare(queue='task_queue', durable=True)

"""
VideoStream to RabbitMQ
"""
# Start the WebcamStream
cap = cv2.VideoCapture(0)

while(True):

    ret, frame = cap.read() # capture frame by frame
    img_str = cv2.imencode('.jpg', frame)[1].tostring() # array to String

    # Send the string and make the message persistent
    channel.basic_publish(exchange='',
        routing_key='task_queue',
        body=img_str,
        properties=pika.BasicProperties(
        delivery_mode = 2, # make message persistent
        ))

connection.close()
