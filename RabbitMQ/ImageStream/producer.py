#!/usr/bin/env python
import os
import pika
import cv2

from skimage import io

"""
Verbindungsdaten RabbitMQ

"""

credentials = pika.PlainCredentials('rabbitmq', 'TJ3EzFdWEK3zVJE4')
connection = pika.BlockingConnection(pika.ConnectionParameters(
        'localhost', 5672, '/', credentials))
channel = connection.channel()
channel.queue_declare(queue='task_queue2', durable=True)

"""
Bild umwandeln / senden
"""

# setze den Bildpfad
filename = os.path.join('image.jpg')

# wandle das Bild in ein Numpy Array und speichere es in der Variable
picture = io.imread(filename)

# wandle das Numpy Array in einen string um ( wird in den Buffer geschrieben)
img_str = cv2.imencode('.jpg', picture)[1].tostring()

# Setze die Menge an Bildern, welche erstellt werden  sollen
x = 12000

# verschicke das gebufferte Bild
while x > 2:

    # Verschicke, den String und mache die Nachricht persistent
    channel.basic_publish(exchange='',
                          routing_key='task_queue2',
                          body=img_str,
                          properties=pika.BasicProperties(
                             delivery_mode=2,  # make message persistent
                          ))
    x = x - 1
print(" [x] Sent ")
connection.close()
