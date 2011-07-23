#!/usr/bin/env python
#
# vim:ts=4:sw=4:expandtab
######################################################################

import subprocess, sys, os, time
import pika
import xlrd
import random
import getopt
import couchdb
import json

logfile = None

class Workers:
    def __init__(self,worker,numWorker, rabbitMQServer ,rabbitMQQueue,
                        couchDbServer, couchDbName, depth, fastcrawl, 
                        maxurl, navigationDepth, historyJmpValue,
                        lFile):
        self.worker = worker
        self.numWorker = numWorker
        self.rmqServer = rabbitMQServer
        self.rmqQueue = rabbitMQQueue
        self.cdbServer = couchDbServer
        self.cdbName = couchDbName
        self.depth = depth
        self.fastcrawl = fastcrawl
        self.maxurl = maxurl
        self.navigationDepth = navigationDepth
        self.historyJmpValue = historyJmpValue
        self.logFile = lFile

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
                                    "-m", self.maxurl,
                                    "-v", self.navigationDepth,
                                    "-j", self.historyJmpValue,
                                    "-l", self.logFile
                                    ])
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
    print 'python controller.py -n <num_workers> -r <rabbitMQ_server> ' \
            '-q <rabbitMQ_queue> -s <couchdb_server> -b <dbName> ' \
            '-d <crawl_depth> -f <fastcrawl> -m <maxurl> -h <help>' \
            '-v <navigation_depth> -j <history_jmp_value> -u <url_file>'\
            '-l <logfile>' 

if __name__ == '__main__':

    #Handle command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                        "hn:r:q:s:b:d:f:m:v:j:u:l: ",
                        ["help", "numWorkers=", "rabbitMQServer=", 
                        "rabbitMQQueue=", "server=", "dbName=", 
                        "depth=", "fastcrawl=", "maxurl=", "navigationDepth=", 
                        "historyJmpValue=", "urlFile=", "logFile="])
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
    navigationDepth = None
    historyJmpValue = None
    urlFile =None
    logFileName = None
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
        elif o in ("-v", "--navigationDepth"):
            navigationDepth = a
        elif o in ("-j", "--historyJmpValue"):
            historyJmpValue = a
        elif o in ("-u", "--urlFile"):
            urlFile = a
        elif o in ("-l", "--logFile"):
             logFileName = a
        else:
            assert False, "unhandled option"
    if None in [numWorkers, rabbitMQServer, rabbitMQQueue, couchDbServer, couchDbName, depth, fastcrawl, maxurl, navigationDepth, historyJmpValue, urlFile, logFileName]:
        usage()
        sys.exit()
    
    logfile = open(logFileName+"_controller.log","w")
    logfile.write(" [Controller Starte]\n")
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
    workBook = xlrd.open_workbook(urlFile)
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
        logfile.write(" [Controller] Sent " + str(msgToSend) + "\n")
        if fastcrawl == "yes":
            msg1.send(json.dumps(msgToSend))

    #start the workers on the main queue
    workers = Workers("./browser.py", numWorkers, rabbitMQServer, rabbitMQQueue, couchDbServer, couchDbName, depth, fastcrawl, maxurl, navigationDepth, historyJmpValue, logFileName)
    workers.start()
    time.sleep(30)

    if fastcrawl == "yes":
        #Stop the workers when on the crawl queue
        while True:
            count1 = msg1.msgCount()
            print " [Controller] fastCrawl Message Count: ", count1
            logfile.write(" [Controller] fastCrawl Message Count: " + str(count1) + "\n")
            if count1 == 0:
                workers.stop(msg1)
                break
            time.sleep(10)
        msg1.close()


    #Stop the workers when work is done
    while True:
        count = msg.msgCount()
        print " [Controller] Message Count: ", count
        logfile.write(" [Controller] Message Count: " + str(count) + "\n")
        if count == 0:
            time.sleep(30)
            count1 = msg.msgCount()
            print " [Controller] Second Message Count: ", count1
            logfile.write(" [Controller] Second Message Count: " + str(count1) + "\n")
            if count1 == 0:
                workers.stop(msg)
                break
        time.sleep(20)

    msg.close()
    print " [Controller] Quitting"
    logfile.write(" [Controller] Quitting\n")
    logfile.close()
