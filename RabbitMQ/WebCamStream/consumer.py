#!/usr/bin/env python
import numpy as np
import pika
import cv2
from skimage import filters

"""
Verbindungsdaten RabbitMQ
"""

credentials = pika.PlainCredentials('rabbitmq', 'TJ3EzFdWEK3zVJE4')
connection = pika.BlockingConnection(pika.ConnectionParameters(
        'localhost', 5672, '/', credentials))

channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)


"""
Grayscale VideoStream
"""


def imageDecode(ch, method, properties, body):

    np_array = np.fromstring(body, np.uint8)  # umwandlung string to numpy
    grayImage = cv2.imdecode(np_array, 0)  # nur in graustufen umwandeln
    edges = filters.sobel(grayImage)  # berechnet die Umrisse + Darstellung

# Bild anzeigen bis Q gedrueckt wird
    while(True):
        cv2.imshow('frame', edges)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
        ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)  # jeweils nur eine Nachricht annehmen
channel.basic_consume(imageDecode, queue='task_queue')
channel.start_consuming()
