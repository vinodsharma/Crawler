#!/usr/bin/env python
#
# vim:ts=4:sw=4:expandtab
######################################################################

"""Worker task: does the actual browsing work."""

import gtk
import sys
import pywebkitgtk as webkit
import random
from datetime import datetime
from datetime import timedelta
import time
import os
import couchdb
import getopt
import json
import argparse
import uuid
import logging
from logging import *
from queue import RabbitMQ

def randstr(l = 32):
    return "".join(["%.2x" % random.randint(0, 0xFF) for i in range(l/2)])

class DOMWalker:
    def __init__(self,rdepth,maxurl2add):
        self.__indent = 0
        self.__rdepth = rdepth
        self.__url2add = maxurl2add

    def __dump(self, node):
        i = 0
        #print >> sys.stderr,  " "*self.__indent, node.__class__.__name__
        if self.__rdepth > 0:
            if node.nodeName == "A" and self.__url2add > 0:
                #print "url2add= " ,self.__url2add
                #print >> sys.stderr,  " "*self.__indent, node.__class__.__name__
                if node.hasAttribute("href") and  node.__getattribute__("href").find("http") != -1:
                    #print >> sys.stderr,  "  "*self.__indent, node.__getattribute__("href")
                    urlval = node.__getattribute__("href")
                    udepth = str(self.__rdepth-1)
                    msgToSend = {
                            'command': "visit",
                            'url': urlval,
                            'depth': udepth
                            }

                    queue.send(json.dumps(msgToSend))


                    self.__url2add -= 1
                    #print >> sys.stderr,  "  "*self.__indent, "http://safly-beta.dyndns.org/?q="+node.__getattribute__("href")
                    #print >> sys.stderr,  "  "*self.__indent, node.nodeName

    def walk_node(self, node, callback = None, *args, **kwargs):
        if callback is None:
            callback = self.__dump

        callback(node, *args, **kwargs)
        self.__indent += 1
        children = node.childNodes
        for i in range(children.length):
            child = children.item(i)
            self.walk_node(child, callback, *args, **kwargs)
            self.__indent -= 1


class Browser():
    def __init__(self,maxurl2add):
        self.__maxurl2add = maxurl2add
        self.__rdepth = None
        self.__bid = randstr(16)
        self.__webkit = webkit.WebView()
        self.__webkit.SetDocumentLoadedCallback(self._DOM_ready)
        info("Spawned new browser " + str(self.__bid))

    def __del__(self):
        pass

    def visit(self, url, rdepth):
        #print >> sys.stderr,  "Visiting URL", url
        self.pageLoaded = False
        self.__rdepth = rdepth
        self.__webkit.LoadDocument(url)

    def url(self):
        window = self.__webkit.GetDomWindow()
        return window.location.href

    def _DOM_node_inserted(self, event):
        target = event.target
        # target can be: Element, Attr, Text, Comment, CDATASection,
        # DocumentType, EntityReference, ProcessingInstruction
        parent = event.relatedNode
        #print >> sys.stderr,  "NODE INSERTED", target, parent

    def _DOM_node_removed(self, event):
        target = event.target
        # target can be: Element, Attr, Text, Comment, CDATASection,
        # DocumentType, EntityReference, ProcessingInstruction
        parent = event.relatedNode
        #print >> sys.stderr,  "NODE REMOVED", target, parent

    def _DOM_node_attr_modified(self, event):
        target = event.target
        # target can be: Element
        name = event.attrName
        change = event.attrChange
        newval = event.newValue
        oldval = event.prevValue
        parent = event.relatedNode
        #print >> sys.stderr,  "NODE ATTR MODIFIED", target, name, change, newval, oldval, parent

    def _DOM_node_data_modified(self, event):
        target = event.target
        # target can be: Text, Comment, CDATASection, ProcessingInstruction
        parent = event.target.parentElement
        newval = event.newValue
        oldval = event.prevValue
        #print >> sys.stderr,  "NODE DATA MODIFIED", target, newval, oldval, parent
        #print >> sys.stderr,  dir(target)
        #print >> event.target.getElementsByTagName('div').nodeName
        #print >> event.target.attributes[0].nodeName
        node=event.target.parentElement
        #print target.textContent
        #print target.parentElement.attributes.length

        if node.attributes:
            for i in range(node.attributes.length):
                attribute = node.attributes.item(i)
                attrName = attribute.nodeName
                attrValue = attribute.nodeValue
                #print attrName, "-->", attrValue
                if attrName == "name" and attrValue == "is_loaded":
                    #print node.innerHTML;
                    #print target.textContent
                    if node.innerHTML == "1":
                        #print "page loaded"
                        self._is_Page_Loaded()

        #print dir(event.target)

    def _DOM_ready(self):
        document = self.__webkit.GetDomDocument()
        body = document.getElementsByTagName('body').item(0)
        if not body:
            return

        window = self.__webkit.GetDomWindow()
        document.addEventListener('DOMNodeInserted', self._DOM_node_inserted,
                                        False)
        document.addEventListener('DOMNodeRemoved', self._DOM_node_removed,
                                        False)
        document.addEventListener('DOMAttrModified', self._DOM_node_attr_modified,
                                        False)
        document.addEventListener('DOMCharacterDataModified', self._DOM_node_data_modified,
                                        False)
        #print >> sys.stderr,  "URL:", document.URL
        #print >> sys.stderr,  "Title:", document.title
        #print >> sys.stderr,  "Cookies:", document.cookie
        DOMWalker(self.__rdepth,self.__maxurl2add).walk_node(document)
        self.__rdepth = None
        gtk.mainquit()

    def _is_Page_Loaded(self):
        debug("_is_Page_Loaded")
        self.pageLoaded = True
        gtk.mainquit()

def callback(ch, method, properties, body):
    info("received %r" % (body,))
    message = json.loads(body)
    if message.get("command") == "quit":
        info("Got quit command, exiting...")
        queue.stoprecv()
        queue.close()
        sys.exit(77) # 77 indicates clean shutdown
    elif message.get("command") == "visit":
        url = message.get("url")
        rdepth = int(message.get("depth"))
        # visit url to fetch more urls
        if rdepth > 0:
            #print " [worker",wid,"] crawling: ", url
            browser.visit(url,rdepth)
            gtk.main()

        #browser = Browser()
        tstart = datetime.now()
        # visit url to fetech the response time
        if args.proxy:
            browser1 = Browser(0)
            target_url = "%s/?q="%(args.proxy) + url
            info("Visiting URL: " + target_url)
            browser1.visit(target_url, 0)
            gtk.main()
        tend = datetime.now()
        loadTime = tend-tstart
        diff = (loadTime.seconds+(float(loadTime.microseconds)/1000000))
        docname = url + str(randstr(16))
        db[docname] = {'url' : url, 'Response Time' : diff}
        #print loadTime.seconds, " " , loadTime.microseconds
        #print " [worker",wid,"] Response Time:","%.6f" % (diff)
    else:
        warn("invalid command")

    # Ack only after processing message, for two reasons: (1) avoids
    # a race with controller where he sees an empty queue and thinks
    # all works is done when in reality workers have yet to add more work,
    # and (2) fault-tolerance -- if a worker dies before sending an ack, 
    # we would like some other worker to pick up the task (RabbitMQ does
    # this automatically for us).
    ch.basic_ack(delivery_tag = method.delivery_tag)    

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.strip(),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            add_help=False)
    parser.add_argument("-h", "--help", action="help",
                        help="Show this help message and exit")
    parser.add_argument("-i", "--id", default=None,
            help="ID to assign to worker (for debugging)")
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
    parser.add_argument("-l", "--log-file", default="/tmp/worker",
            help="Name of database to store results in")
    parser.add_argument("-p", "--proxy", default=None,
            help="Proxy to use when generating load")

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_args()
    if args.id == None:
        args.id = str(uuid.uuid4())[0:2]
    logging.basicConfig(level=logging.INFO,
      format="[worker-%s] "%(args.id) + "%(levelname)s %(message)s",
      filename="%s-%s.log"%(args.log_file, args.id) if args.log_file else None,
      stream=sys.stderr,
      filemode='w')

    browser = Browser(args.branch_factor)
    info("Worker started")
    cdb = couchdb.Server(args.db_server)
    try:
        db = cdb[args.db_name]
    except:
        db = cdb.create(args.db_name)
    queue = RabbitMQ(args.queue_server, args.queue_name)
    queue.recv(callback)
