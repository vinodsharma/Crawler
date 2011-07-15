#!/usr/bin/env python
import pika
import os, signal, time
import sys
from controller import RabbitMQ
import json
quit = "quit"
visit = "visit"

wid = sys.argv[1]
rabbitMQServer = sys.argv[2]
rabbitMQQueue = sys.argv[3]
msg = RabbitMQ(rabbitMQServer,rabbitMQQueue)
#msg = RabbitMQ("localhost","newhello")

print " [worker",wid,"] Started"
#print sys.argv[1]
def callback(ch, method, properties, body):
	msg.ack(ch,method)
	#ch.basic_ack(delivery_tag = method.delivery_tag)
	#print " [x] Received %r" % (body,)
	print " [worker",wid,"] Received %r" % (body,)
	#message = body
	message = json.loads(body)
	if message.get("command") == quit:
		time.sleep(2);
		msg.stoprecv()
	elif message.get("command") == visit:
		print " [worker",wid,"] visiting: ", message.get("url")
	else:
		print " [worker",wid,"] Invalid command"
	
	#if message == quit:
	#	time.sleep(2);
	#	msg.stoprecv()
	#elif message == visit:
	#	print " [worker",wid,"] visiting: "
	#else:
	#	print " [worker",wid,"] Invalid command"

msg.recv(callback)

print "retunr"
