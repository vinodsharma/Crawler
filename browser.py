#!/usr/bin/env python
#
# vim:ts=4:sw=4:expandtab
######################################################################

import gtk
import sys
import pywebkitgtk as webkit
import random
from datetime import datetime
from datetime import timedelta
import time
import signal, os
import pika
from controller import RabbitMQ
import couchdb
import getopt
import json

SITE_URL="http://ec2-50-18-23-52.us-west-1.compute.amazonaws.com"
logfile = None

#global msg = None
def randstr(l = 32):
    return "".join(["%.2x" % random.randint(0, 0xFF) for i in range(l/2)])

class DOMWalker:
    def __init__(self,fastcrawl,rdepth,maxurl2add):
        self.__indent = 0
        self.__fastcrawl = fastcrawl
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
                    if self.__fastcrawl == "yes":
                        msg1.send(json.dumps(msgToSend))
                        logfile.write(" [worker"+str(wid)+"] Sent " + str(msgToSend)+"\n")

                    msg.send(json.dumps(msgToSend))
                    logfile.write(" [worker"+str(wid)+"] Sent " + str(msgToSend)+"\n")

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
    def __init__(self,fastcrawl,maxurl2add):
        self.__fastcrawl = fastcrawl
        self.__maxurl2add = maxurl2add
        self.__rdepth = None
        self.__bid = randstr(16)
        self.__webkit = webkit.WebView()
        self.__webkit.SetDocumentLoadedCallback(self._DOM_ready)
        logfile.write(" [worker"+str(wid)+"] Spawned new browser " + str(self.__bid)+"\n")
        #print >> sys.stderr,  "Spawned new browser", self.__bid

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
        logfile.write(" [worker"+str(wid)+"] URL: " + str(document.URL)+"\n")
        logfile.write(" [worker"+str(wid)+"] Title: " + str(document.title)+"\n")
        #print >> sys.stderr,  "URL:", document.URL
        #print >> sys.stderr,  "Title:", document.title
        #print >> sys.stderr,  "Cookies:", document.cookie
        DOMWalker(self.__fastcrawl,self.__rdepth,self.__maxurl2add).walk_node(document)
        self.__rdepth = None
        gtk.mainquit()

    def go_back(self,index):
        if self.__webkit.go_back(index) > 0:
            return True
        else:
            return False
    
    def go_forward(self,index):
        if self.__webkit.go_forward(index) > 0:
            return True
        else:
            return False

    def get_back_history_length(self):
        return self.__webkit.get_back_history_length()
    
    def get_forward_history_length(self):
        return self.__webkit.get_forward_history_length()
    
    def Crawl(self):
        document = self.__webkit.GetDomDocument()
        urlList = []
        self.GetUrlList(document,urlList)
        #print "NumUrl on this page= " , len(urlList)
        #for item in urlList:
        #   print item
        #print random.randint(0,len(urlList))
        urlListLen = len(urlList)
        if urlListLen <= 0:
            #no url to the page
            return False
        elif urlListLen == 1:
            # one url on the page 
            self.visit(urlList[0],0)
            gtk.main()
            return True
        else:
            self.visit(urlList[random.randint(0,urlListLen-1)],0)
            gtk.main()
            return True
    
    def GetUrlList(self, node,urllist):
        if node.nodeName is not None:
            if node.nodeName == "A":
                if node.hasAttribute("href") and  node.__getattribute__("href").find("http") != -1:
                    urlval = node.__getattribute__("href")
                    #print urlval
                    urllist.append(urlval)
    
        children = node.childNodes
        for i in range(children.length):
            child = children.item(i)
            if child is not None: 
                self.GetUrlList(child,urllist)

def doNavigationTest(browser,navigationDepth,jumpValue):
    # this function will first perform crawling & then do navigation
    browser.ndepth = navigationDepth;
    urlVisited = []
    logfile.write(" [worker"+str(wid)+"] -----Navigation Crawl Start-----\n")
    while browser.ndepth > 0:
        ret = browser.Crawl()
        if not ret:
            logfile.write(" [worker"+str(wid)+"] no Urls on the page: stopping the crawlURL\n")
            #print "no Urls on the page: stopping the crawl"
            break;
        browser.ndepth-=1
    logfile.write(" [worker"+str(wid)+"] -----Navigation Crawl End-----\n")

    if browser.ndepth == navigationDepth:
        logfile.write(" [worker"+str(wid)+"] Cannot start navigation test due to unsucessful crawl\n")
        #print "cannot start navigation test"
        return 0 # test cannot be started
    # test backward navigation          
    backcount = navigationDepth - browser.ndepth
    #print "backcount: ", backcount
    while backcount > 0:
        #print "Back History Len: ", browser.get_back_history_length()
        logfile.write(" [worker"+str(wid)+"] Going Back\n")
        if browser.go_back(1):
            gtk.main()
        else:
            logfile.write(" [worker"+str(wid)+"] Going Back Error\n")
            #print "Python Error in going back"
            return -1
        backcount-=1
    # test forward navigation
    forwardcount = navigationDepth - browser.ndepth
    while forwardcount > 0:
        #print "Forward History Len: ", browser.get_forward_history_length()
        logfile.write(" [worker"+str(wid)+"] Going Forward\n")
        if browser.go_forward(1):
            gtk.main()
        else:
            logfile.write(" [worker"+str(wid)+"] Going Forward Error\n")
            #print "Python Error in going forward"
            return -1
        forwardcount-=1
    
    # test jump back navigation 
    jumpbackcount = navigationDepth - browser.ndepth
    jumpCount = 0
    while jumpbackcount > jumpValue:
        #print "Back History Len: ", browser.get_back_history_length()
        logfile.write(" [worker"+str(wid)+"] Jumping Back by " + str(jumpValue)+"\n")
        if browser.go_back(jumpValue+1):
            gtk.main()
        else:
            logfile.write(" [worker"+str(wid)+"] Jumping Back Error: JumpValue=" + str(jumpValue)+"\n")
            #print "Python Error in jumping back"
            return -2
        jumpbackcount-=2
        jumpCount+=1
    
    # test jump forward navigation 
    while jumpCount > 0:
        #print "Forward History Len: ", browser.get_forward_history_length()
        logfile.write(" [worker"+str(wid)+"] Jumping Forward by " + str(jumpValue)+"\n")
        if browser.go_forward(jumpValue+1):
            gtk.main()
        else:
            #print "Python Error in jumping forward"
            logfile.write(" [worker"+str(wid)+"] Jumping forward Error: JumpValue=" + str(jumpValue)+"\n")
            return -2
        jumpCount-=1

    return 1;


def callback(ch, method, properties, body):
    msg.ack(ch,method)
    #print " [worker",wid,"] Received %r" % (body,)
    logfile.write(" [worker"+str(wid)+"] Received: " + str(body)+"\n")
    message = json.loads(body)
    if message.get("command") == quit:
        time.sleep(2);
        msg.stoprecv()
        msg.close()
        sys.exit()
    elif message.get("command") == visit:

        url = message.get("url")
        rdepth = int(message.get("depth"))
        # visit url to fetch more urls
        if fastcrawl == "no" and rdepth > 0:
            #print " [worker",wid,"] crawling: ", url
            browser.visit(url,rdepth)
            gtk.main()


        # visit url to fetech the response time
        #print " [worker",wid,"] visiting: ","%s/?q="%(SITE_URL)+url
        logfile.write(" [worker"+str(wid)+"] Visiting: " + str(SITE_URL)+"/q="+str(url)+"\n")
        #browser = Browser()
        tstart = datetime.now()
        browser1 = Browser("no",0)
        browser1.visit(url,0)
        #browser1.visit("%s/?q="%(SITE_URL)+url,0)
        #browser.visit("http://localhost/")
        gtk.main()
        tend = datetime.now()
        loadTime = tend-tstart
        diff = (loadTime.seconds+(float(loadTime.microseconds)/1000000))
        docname = url + str(randstr(16))
        db[docname] = {'url' : url, 'Response Time' : diff}
        
        logfile.write(" [worker"+str(wid)+"] ##########Navigation Test Start#########\n")
        #print "##########Navigation Test Started#############"
        ret = doNavigationTest(browser1,int(navigationDepth),int(historyJmpValue))
        if ret > 0:
            logfile.write(" [worker"+str(wid)+"] Navigation Test Passed\n")
            #print "Navigation test passed"
        elif ret == 0:
            logfile.write(" [worker"+str(wid)+"] Navigation Test Not performed\n")
            #print "Navigation test cannot be started as full depth not crawled"
        else:
            logfile.write(" [worker"+str(wid)+"] Navigation Test Failed\n")
            #print "Navigation test failed"

        logfile.write(" [worker"+str(wid)+"] ##########Navigation Test End#########\n")
        #print loadTime.seconds, " " , loadTime.microseconds
        #print " [worker",wid,"] Response Time:","%.6f" % (diff)
    else:
        logfile.write(" [worker"+str(wid)+"] Invalid Command Received\n")
        #print " [worker",wid,"] Invalid command"

def fastcrawlHandler(ch, method, properties, body):
    msg1.ack(ch,method)
    #print " [worker",wid,"] Received %r" % (body,)
    message = json.loads(body)
    if message.get("command") == quit:
        time.sleep(2);
        msg1.stoprecv()
    elif message.get("command") == visit:
        url = message.get("url")
        #print " [worker",wid,"] visiting: ", url
        tstart = datetime.now()
        rdepth = int(message.get("depth"))
        browser.visit(url,rdepth)
        gtk.main()
        tend = datetime.now()
        loadTime = tend-tstart
        diff = (loadTime.seconds+(float(loadTime.microseconds)/1000000))
        docname = url + str(randstr(16))
        db[docname] = {'url' : url, 'Response Time' : diff}
        #print loadTime.seconds, " " , loadTime.microseconds
        #print " [worker",wid,"] Response Time:","%.6f" % (diff)
    else:
        logfile.write(" [worker"+str(wid)+"] Invalid Command Received\n")
        #print " [worker",wid,"] Invalid command"

def usage():
    print 'python browser.py -i <wid> -r <rabbitMQ_server>' \
            '-q <rabbitMQ_queue> -s <couchdb_server> -b <dbName>' \
            '-d <crawl_depth> -f <fastcrawl> -m <maxurl>' \
            '-h <help> -v <navigation_depth> -j <history_Jmp_Value>'

if __name__ == '__main__':

    #Handle command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                        "hi:r:q:s:b:d:f:m:v:j:l: ",
                        ["help", "wid=", "rabbitMQServer=", 
                        "rabbitMQQueue=", "server=", "dbName=", 
                        "depth=", "fastcrawl=", "maxurl=", "navigationDepth=", 
                        "historyJmpValue=", "logFile="])
    except getopt.GetoptError, err:
        #print help information & exit
        print str(err)
        usage()
        sys.exit(2)

    wid = None
    rabbitMQServer = None
    rabbitMQQueue = None
    couchDbServer = None
    couchDbName = None
    depth = None
    fastcrawl = None
    maxurl = None
    navigationDepth = None
    historyJmpValue = None
    logFileName = None
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--wid"):
            wid = a
        elif o in ("-r", "--rabbitMQServer"):
            rabbitMQServer = a
        elif o in ("-q", "--rabbitMQQueue"):
            rabbitMQQueue = a
        elif o in ("-s", "--server"):
            couchDbServer = a
        elif o in ("-b", "--dbName"):
            couchDbName = str(a)
        elif o in ("-d", "--depth"):
            depth = str(a)
        elif o in ("-f", "--fastcrawl"):
            fastcrawl = str(a)
        elif o in ("-m", "--maxurl"):
            maxurl = a
        elif o in ("-v", "--navigationDepth"):
            navigationDepth = a
        elif o in ("-j", "--historyJmpValue"):
            historyJmpValue = a
        elif o in ("-l", "--logFile"):
             logFileName = a
        else:
            assert False, "unhandled option"
    if None in [wid, rabbitMQServer, rabbitMQQueue, couchDbServer, couchDbName, depth, fastcrawl, maxurl, navigationDepth, historyJmpValue, logFileName]:
        usage()
        sys.exit()

    quit = "quit"
    visit = "visit"

    #wid = sys.argv[1]
    #rabbitMQServer = sys.argv[2]
    #rabbitMQQueue = sys.argv[3]
    logfile = open(logFileName+"_worker"+str(wid)+".log","w")
    logfile.write(" [worker"+str(wid)+"] Started\n")
    #print " [worker",wid,"] Started"
    
    browser = Browser(fastcrawl,int(maxurl))
    
    cdb = couchdb.Server(couchDbServer)
    try:
        db = cdb[couchDbName]
    except:
        db = cdb.create(couchDbName)
    msg = None
    msg = RabbitMQ(rabbitMQServer,rabbitMQQueue)
    #if fastcrawl == "no":
    #       msg = RabbitMQ(rabbitMQServer,rabbitMQQueue)
    #       msg.recv(callback)
    if fastcrawl == "yes":
        msg1 = RabbitMQ(rabbitMQServer,"crawl"+rabbitMQQueue)
        msg1.recv(fastcrawlHandler)

    msg.recv(callback)
#browser.visit('http://www.google.com')
#gtk.main()
