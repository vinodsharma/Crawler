import subprocess, sys, os, time
import pika
import xlrd
import random
import getopt
import couchdb
import json
#def startWorkers(worker,numWorker):
#	for i in range(int(numWorker)):
#		p = subprocess.Popen(worker)
#		#p = subprocess.Popen("/home/bitblaze145/deploy/receive.py")

class workers:
    def __init__(self,worker,numWorker, rabbitMQServer ,rabbitMQQueue, 
			couchDbServer, couchDbName, depth, fastcrawl, maxurl):
	self.worker = worker
	self.numWorker = numWorker
	self.rmqServer = rabbitMQServer
	self.rmqQueue = rabbitMQQueue
	self.cdbServer = couchDbServer
	self.cdbName = couchDbName
	self.depth = depth
	self.fastcrawl = fastcrawl
	self.maxurl = maxurl

    def start(self):
	for i in range(int(self.numWorker)):
		p = subprocess.Popen([self.worker, 
					"-i", str(i), 
					"-r", self.rmqServer, 
					"-q", self.rmqQueue,
					"-s", self.cdbServer,
					"-b", self.cdbName,
					"-d", self.depth,
					"-f", self.fastcrawl,
					"-m", self.maxurl])
		#p = subprocess.Popen(self.worker)
	
    def stop(self,msgobj):
	for i in range(int(self.numWorker)):
		msgToSend = {
			'command': "quit"
			}
		msgobj.send(json.dumps(msgToSend))
		#msg.send("quit")

class RabbitMQ:
    def __init__(self,hostaddr,msgqueue):
	self.msgqueue=msgqueue
	self.connection = pika.BlockingConnection(pika.ConnectionParameters(
		host=hostaddr))
	self.channel = self.connection.channel()
	self.channel.queue_declare(queue=self.msgqueue)

    def send(self, message):
	self.channel.basic_publish(exchange='',
				routing_key=self.msgqueue,
				body=message)
	print " [Controller] Sent ", message
    
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
	status = self.channel.queue_declare(queue=self.msgqueue)
	return status.method.message_count

    def close(self):
	#self.channel.queue_delete(queue=self.msgqueue)
	self.connection.close()


def usage():
	print "python browser.py -n <num_workers> -r <rabbitMQ_server> -q <rabbitMQ_queue> -s <couchdb_server> -b <dbName> -d <crawl_depth> -f <fastcrawl> -m <maxurl> -h <help>"

if __name__ == '__main__':

	#Handle command line arguments
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hn:r:q:s:b:d:f:m: ",["help", "numWorkers=", "rabbitMQServer=", "rabbitMQQueue=", "server=", "dbName=", "depth=", "fastcrawl=", "maxurl="])
	except getopt.GetoptError, err:
		#print help information & exit
		print str(err)
		usage()
		sys.exit(2)

	numWorkers = None
	rabbitMQServer = None
	rabbitMQQueue = None
	couchDbServer = None
	couchDbName = None
        depth = None
	fastcrawl = None
	maxurl = None
	for o, a in opts:
		if o in ("-h", "--help"):
			usage()
			sys.exit()
		elif o in ("-n", "--numWorkers"):
			numWorkers = a
		elif o in ("-r", "--rabbitMQServer"):
			rabbitMQServer = a
		elif o in ("-q", "--rabbitMQQueue"):
			rabbitMQQueue = a
		elif o in ("-s", "--couchDbServer"):
			couchDbServer = a
		elif o in ("-b", "--couchdbname"):
			couchDbName = a
		elif o in ("-d", "--depth"):
			depth = a
		elif o in ("-f", "--fastcrawl"):
			fastcrawl = str(a)
		elif o in ("-m", "--maxurl"):
			maxurl = a
		else:
			assert False, "unhandled option"
	if None in [numWorkers, rabbitMQServer, rabbitMQQueue, couchDbServer, couchDbName, depth, fastcrawl, maxurl]:
		print couchDbName
		usage()
		sys.exit()


	#connect to couchdb server & create the datebase
	cdb = couchdb.Server(couchDbServer)
	try:
		db = cdb.create(couchDbName)
	except:	
		cdb.delete(couchDbName)
		db = cdb.create(couchDbName)

	#Initiate & start the Worker Processes
	#rabbitMQServer = "localhost"
	#rabbitMQQueue = "newhello"
	#numWorkers = sys.argv[1]
	#start the worker now if fast crawl is disabled
	#workers = workers("./receive.py", numWorkers, rabbitMQServer, rabbitMQQueue)
	
	#initiate rabbitMQ connection
	#msg = RabbitMQ("localhost","newhello")
	msg = RabbitMQ(rabbitMQServer,rabbitMQQueue)
	if fastcrawl == "yes":
		msg1 = RabbitMQ(rabbitMQServer,"crawl"+rabbitMQQueue)

	#Delegate the work to workers
	workBook = xlrd.open_workbook('input.xls') 
	sheet = workBook.sheet_by_index(0) 	
	for rowIndex in range(sheet.nrows):
        	if rowIndex == 0:
            		continue
        
        	url = str(sheet.cell(rowIndex, 0).value)
		#msgToSend = "visit "+url
		msgToSend = {
			'command': "visit",
			'url': url,
			'depth': depth
			}
		msg.send(json.dumps(msgToSend))
		if fastcrawl == "yes":
			msg1.send(json.dumps(msgToSend))
	
	#start the workers on the main queue
	workers = workers("./browser.py", numWorkers, rabbitMQServer, rabbitMQQueue, couchDbServer, couchDbName, depth, fastcrawl, maxurl)
	workers.start()
	time.sleep(30)
	
	if fastcrawl == "yes":
		#Stop the workers when on the crawl queue
		while True:
			count1 = msg1.msgCount()
			print " [Controller] Message Count: ", count1
			if count1 == 0:
				workers.stop(msg1)
				break
			time.sleep(10)
		msg1.close()

	
	#Stop the workers when work is done
	while True:
		count = msg.msgCount()
		print " [Controller] Message Count: ", count
		if count == 0:
			time.sleep(30)
			count1 = msg.msgCount()
			print " [Controller] Message Count: ", count1
			if count1 == 0:
				workers.stop(msg)
				break
		time.sleep(20)
	
	msg.close()

	

