import pika
from pika import BlockingConnection
from pika import ConnectionParameters
from pika.exceptions import ConnectionClosed
from streamparse import Spout



class WordSpout(Spout):
    outputs = ['word']

    def initialize(self, stormconf, context):
        self.credentials = pika.PlainCredentials('rabbitmq', 'TJ3EzFdWEK3zVJE4')
        self.m2m_conn = BlockingConnection(ConnectionParameters('localhost', 5672, '/', self.credentials))
        self.channel = self.m2m_conn.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.queue_bind(exchange="Test", queue="test.queue", routing_key="test.route")


    def next_tuple(self):
        def callback(ch, method, properties, body):
            word = body
            self.logger.info(word)
            self.emit([word])
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(callback, queue="test.queue")  # Funktion wird auf die Queue angewandt
        self.channel.start_consuming()
