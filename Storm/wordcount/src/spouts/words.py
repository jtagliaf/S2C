from itertools import cycle
import pika
from pika import BlockingConnection
from pika import ConnectionParameters
from pika.exceptions import ConnectionClosed
from streamparse import Spout



class WordSpout(Spout):
    outputs = ['body']

    def initialize(self, stormconf, context):
        self.credentials = pika.PlainCredentials('rabbitmq', 'TJ3EzFdWEK3zVJE4')
        self.m2m_conn = BlockingConnection(ConnectionParameters('localhost', 5672, '/', self.credentials))
        self.channel = self.m2m_conn.channel()
        # self.channel.basic_qos(prefetch_count=1)
        self.channel.queue_bind(exchange="Test", queue="test.queue", routing_key="test.route")
        #self.words = cycle(['dog', 'cat', 'zebra', 'elephant'])

    def next_tuple(self):
        def callback(ch, method, properties, body):
            print (" [x] %r:%r" % (method.routing_key, body,))
            self.log(body)
            self.emit([body])

        try:
            self.channel.basic_consume(callback, queue="test.queue", no_ack=True)
        except ConnectionClosed as e:
            pass