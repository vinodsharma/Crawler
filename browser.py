#!/usr/bin/env python

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
						
					msg.send(json.dumps(msgToSend))
						

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
		print >> sys.stderr,  "Spawned new browser", self.__bid

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
		DOMWalker(self.__fastcrawl,self.__rdepth,self.__maxurl2add).walk_node(document)
		self.__rdepth = None
		gtk.mainquit()

	def _is_Page_Loaded(self):
		print >> sys.stderr,  "_is_Page_Loaded"
		self.pageLoaded = True
		gtk.mainquit()

def handler(signum, frame):
	print "handler called"

def callback(ch, method, properties, body):
	msg.ack(ch,method)
	print " [worker",wid,"] Received %r" % (body,)
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
		print " [worker",wid,"] visiting: ","http://safly-beta.dyndns.org/?q="+url
		#browser = Browser()
		tstart = datetime.now()
		browser1 = Browser("no",0)
		#browser1.visit(url,0)
		browser1.visit("http://safly-beta.dyndns.org/?q="+url,0)
		#browser.visit("http://localhost/")
		gtk.main()
		tend = datetime.now()
		loadTime = tend-tstart
		diff = (loadTime.seconds+(float(loadTime.microseconds)/1000000))
		docname = url + str(randstr(16))
		db[docname] = {'url' : url, 'Response Time' : diff}
		#print loadTime.seconds, " " , loadTime.microseconds
		#print " [worker",wid,"] Response Time:","%.6f" % (diff)
	else:
		print " [worker",wid,"] Invalid command"

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
		print " [worker",wid,"] Invalid command"

def usage():
	print "python browser.py -i <wid> -r <rabbitMQ_server> -q <rabbitMQ_queue> -s <couchdb_server> -b <dbName> -d <depth> -m <maxurl>-h <help>"
	
if __name__ == '__main__':

	#Handle command line arguments
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hi:r:q:s:b:d:f:m: ",["help", "wid=", "rabbitMQServer=", "rabbitMQQueue=", "server=", "dbName=", "depth=", "fastcrawl=" ,"maxurl="])
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
		else:
			assert False, "unhandled option"
	if None in [wid, rabbitMQServer, rabbitMQQueue, couchDbServer, couchDbName, depth, fastcrawl, maxurl]:
		usage()
		sys.exit()
	
	quit = "quit"
	visit = "visit"
	
	#wid = sys.argv[1]
	#rabbitMQServer = sys.argv[2]
	#rabbitMQQueue = sys.argv[3]
	
	browser = Browser(fastcrawl,int(maxurl))
	print " [worker",wid,"] Started"
	cdb = couchdb.Server(couchDbServer)
	db = cdb[couchDbName]
	msg = None
	msg = RabbitMQ(rabbitMQServer,rabbitMQQueue)
	#if fastcrawl == "no":
	#	msg = RabbitMQ(rabbitMQServer,rabbitMQQueue)
	#	msg.recv(callback)
	if fastcrawl == "yes":
		msg1 = RabbitMQ(rabbitMQServer,"crawl"+rabbitMQQueue)
		msg1.recv(fastcrawlHandler)
	
	msg.recv(callback)
    #browser.visit('http://www.google.com')
    #gtk.main()
