import pika
import sys, signal , os
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()
channel.queue_delete(queue=sys.argv[1])
connection.close()

