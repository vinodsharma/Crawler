#
# Author: Vinod Sharma
#
# vim:ts=4:sw=4:expandtab
######################################################################

"""Workload generator."""

import subprocess, sys, os, time
import xlrd
import random
import getopt
import couchdb
import json
import atexit
import argparse
import logging
from logging import *
from queue import RabbitMQ

class Workers:
    def __init__(self,worker,numWorker, rabbitMQServer ,rabbitMQQueue,
                        couchDbServer, couchDbName, depth, maxurl,
                        navigationDepth,historyJmpValue):
        self.worker = worker
        self.numWorker = numWorker
        self.rmqServer = rabbitMQServer
        self.rmqQueue = rabbitMQQueue
        self.cdbServer = couchDbServer
        self.cdbName = couchDbName
        self.depth = depth
        self.maxurl = maxurl
        self.navigationDepth = navigationDepth
        self.historyJmpValue = historyJmpValue
        self.children = []

    def start(self):
        for i in range(int(self.numWorker)):
            arg_list = [self.worker,
                        "-i", str(i),
                        "-r", self.rmqServer,
                        "-q", self.rmqQueue,
                        "-s", self.cdbServer,
                        "-b", self.cdbName,
                        "-d", str(self.depth),
                        "-m", str(self.maxurl),
                        "-v", str(self.navigationDepth),
                        "-j", str(self.historyJmpValue)]
            if args.proxy:
                arg_list.extend(["-p", args.proxy])
            p = subprocess.Popen(arg_list)
            self.children.append(p)

    def stop(self,msgobj):
        info("Stopping all workers...")
        for i in range(int(self.numWorker)):
            msgToSend = {
                    'command': "quit"
                    }
            msgobj.send(json.dumps(msgToSend))

        # Wait for workers to terminate.
        info("Waiting for workers to terminate...")
        for child in self.children:
            child.wait()

def cleanup():
    workers.stop(queue)
    queue.close()

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.strip(),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            add_help=False)
    parser.add_argument("-h", "--help", action="help",
                        help="Show this help message and exit")
    parser.add_argument("-n", "--nr-workers", type=int, default=1,
            help="Number of workers to run concurrently")
    parser.add_argument("-r", "--queue-server", default="localhost",
            help="Hostname of RabbitMQ queueing service")
    parser.add_argument("-q", "--queue-name", default="testrun",
            help="Name of queue used to distribute work")
    parser.add_argument("-s", "--db-server", default="http://localhost:5984",
            help="URL of database server used to store results")
    parser.add_argument("-b", "--db-name", default="workload_sim",
            help="Name of database to store results in")
    parser.add_argument("-d", "--depth", type=int, default=3,
            help="Maximum crawl depth")
    parser.add_argument("-m", "--branch-factor", type=int, default=5,
            help="Maximum number of URLs to explore per page visited")
    parser.add_argument("-p", "--proxy", default=None,
            help="Proxy to use when generating load")
    parser.add_argument("-v", "--navigation-depth", type=int, default=3,
            help="Maximum crawl depth for navigation test")
    parser.add_argument("-j", "--history-jump", type=int, default=1,
            help="Maximum Jump in history when navigate")
    parser.add_argument("-u", "--url-file", default=None,
            help="File containing the list of urls")

    args = parser.parse_args()
    return args

if __name__ == '__main__':

    args = parse_args()
    logging.basicConfig(level=logging.INFO,
      format='[controller] %(levelname)s %(message)s',
      filename=None,
      stream=sys.stderr,
      filemode='w')


    # Connect to couchdb server & create the datebase
    cdb = couchdb.Server(args.db_server)
    for i in xrange(2):
        try:
            db = cdb.create(args.db_name)
        except couchdb.http.PreconditionFailed:
            cdb.delete(args.db_name)
        else:
            break

    queue = RabbitMQ(args.queue_server, args.queue_name, purge=True)

    #Delegate the work to workers
    workBook = xlrd.open_workbook(args.url_file)
    sheet = workBook.sheet_by_index(0)
    for rowIndex in range(sheet.nrows):
        if rowIndex == 0:
            continue

        url = str(sheet.cell(rowIndex, 0).value)
        msgToSend = {
                'command': "visit",
                'url': url,
                'depth': args.depth
                }
        queue.send(json.dumps(msgToSend))

    # Start the workers, assign them to the specified message queue.
    workers = Workers("./worker", args.nr_workers, args.queue_server, 
                        args.queue_name, args.db_server, args.db_name, 
                        args.depth, args.branch_factor,args.navigation_depth,
                        history_jump)
    workers.start()

    # In case we are abrupty terminateed...
    atexit.register(cleanup)

    # Check if work is complete; exit if so. Work is complete if queue
    # is empty: this assumes that workers ack dequeued messages only
    # after enqueuing more work (if any).
    try:
        while True:
            count = queue.msgCount()
            info("Message Count: " + str(count))
            if count == 0:
                info("Work is done.")
                break
            time.sleep(20)
    except KeyboardInterrupt:
        info("Caught keyboard interrupt.")
        pass
