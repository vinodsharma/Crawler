import pika
from logging import *

class RabbitMQ:
    def __init__(self,hostaddr,msgqueue,purge=False):
        self.msgqueue=msgqueue
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=hostaddr))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.msgqueue, durable=False,
                      exclusive=False, auto_delete=True)
        if purge:
            self.channel.queue_purge(queue=self.msgqueue)

    def send(self, message):
        self.channel.basic_publish(exchange='',
                                routing_key=self.msgqueue,
                                body=message)
        info("Enqueue: " + message)

    def recv(self, handler):
        self.channel.basic_qos(prefetch_count=5)
        self.channel.basic_consume(handler,
                        queue=self.msgqueue)
        self.channel.start_consuming()

    def ack(self,ch,method):
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def stoprecv(self):
        self.channel.stop_consuming()

    def msgCount(self):
        # Looks like we must 
        status = self.channel.queue_declare(queue=self.msgqueue,
                durable=False, exclusive=False, auto_delete=True)
        return status.method.message_count

    def close(self):
        # Queue is automatically deleted on last disconnect
        # (auto_delete=True), so no need to do this.
        #self.channel.queue_delete(queue=self.msgqueue)
        self.connection.close()
