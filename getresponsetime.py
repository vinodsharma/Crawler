#!/use/bin/env python
import couchdb
import sys, os
import getopt
def usage():
	print "python getresponsetime.py -s <couchdb_server> -d <dbName> -h <help>"
try:
	opts, args = getopt.getopt(sys.argv[1:], "hs:d: ",["help", "server=", "dbName="])
except:
	#print help information & exit
	print str(err)
	usage()
	sys.exit(2)
run = None
couchDbServer = None
couchDbName = None
for o, a in opts:
	if o in ("-h", "--help"):
		usage()
		sys.exit()
	elif o in ("-s", "--server"):
		couchDbServer = a
	elif o in ("-d", "--dbName"):
		couchDbName = a
	else:
		assert False, "unhandled option"
if None in [couchDbServer, couchDbName]:
	usage()
	sys.exit()

try:
	cdb = couchdb.Server(couchDbServer)
except:
	print "server not found"
	sys.exit()
try:
	db = cdb[couchDbName]
except:
	print "db does not exist"
	sys.exit()	
#only_this_run = 'function(d) { if (d.run == 3) emit(d.run,d); }'
#for x in scdb.db.query(only_this_run):
#	print x.key
	#print x.value['run'], ":", x.value['url'], ":", x.value['Response Time']
for id in db:
	doc = db[id]
	print doc['url'], "::" , doc['Response Time']
