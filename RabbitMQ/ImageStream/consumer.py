#!/usr/bin/env python

import numpy as np
import pika
import cv2
from skimage import filters

"""
Verbindung mit der Rabbit  Queue herstellen, schauen, ob die Queue taks_queue2
vorhanden ist, wenn nicht wird sie erstellt.

"""

credentials = pika.PlainCredentials('rabbitmq', 'TJ3EzFdWEK3zVJE4')
connection = pika.BlockingConnection(pika.ConnectionParameters(
        'localhost', 5672, '/', credentials))

channel = connection.channel()
channel.queue_declare(queue='task_queue2', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


"""
baut aus dem gesendeten String, wieder in ein Numpy Array, welches dann in ein
png Bild umgewandelt werden kann.

"""


def imageDecode(ch, method, properties, body):

    np_array = np.fromstring(body, np.uint8)  # umwandlung string to numpy
    grayImage = cv2.imdecode(np_array, 0)  # nur in graustufen umwandeln
    edges = filters.sobel(grayImage)  # berechnet die Umrisse + Darstellung

    # Fuer Performance test auskommentieren
    # while (True):
    #    cv2.imshow('frame', edges)
    #    if cv2.waitKey(1) & 0xFF == ord('q'):
    #        break
    ch.basic_ack(delivery_tag=method.delivery_tag)


"""
erhaelt nur immer eine Nachricht und geht erst zur naechsten, wenn diese
bearbeitet wurde.

"""

channel.basic_qos(prefetch_count=1)  # jeweils nur eine Nachricht annehmen
channel.basic_consume(imageDecode, queue='task_queue2')
channel.start_consuming()
