#!/usr/bin/python

import mysql.connector
import csv
import io
import sys
import getopt
import json

def help (me, message):
	if message != "":
		print (message)
		
	print (me + " -f <config file> -s <source nagios db host> -d <destination db host>")
	sys.exit(0)
	
def parse_args (argv):
	me = sys.argv[0]
	
	try:
		opts, args = getopt.getopt (argv, "hf:s:d:", ["help", "file=", "source=", "destination="])
		
	except getopt.GetoptError:
		help(me, "Missing required parameters (-f, -s, -d)")
		
	for opt, arg in opts:
		if opt in ("-f", "--file"):
			cfgfile = arg
		elif opt in ("-s", "--source"):
			srcdb = arg
		elif opt in ("-d", "--destination"):
			dstdb = arg
		elif opt in ("-h", "--help"):
			help (me, "")	
			
	# Ensure variable is defined
	try:
    		cfgfile
	except NameError:
    		help(me,  "Missing required parameters -f <config file>")
			
	try:
    		srcdb
	except NameError:
    		help(me,  "Missing required parameters -s <source nagios db host>")
			
	try:
    		dstdb
	except NameError:
    		help(me,  "Missing required parameters -d <destination db host>")
	
	return dict(cfgjson=cfgfile, source=srcdb, destination=dstdb)

def get_config_hndlr (cfgjson):
	# Load the configuration file
	with open(cfgjson) as f:
    		config = json.load(f)
	
	return config

def get_db_hdlr (config, section):
	dbhdlr = mysql.connector.connect(host=section, user=config[section]["user"], passwd=config[section]["passwd"], database=config[section]["database"])
		
	return dbhdlr
	 
def main(argv):
	params = parse_args (argv)
	
	cfgjson = params["cfgjson"]
	source = params["source"]
	destination = params["destination"]
	
	config = get_config_hndlr (cfgjson)
	
	dstdbhdlr = get_db_hdlr (config, destination)
	dstdbcursor = dstdbhdlr.cursor();
	
	host = {}
	hostgroup = {}
	
	# load hostgroup
	sql = " ".join(config["SQL"]["nghostgroups"])
	dstdbcursor.execute (sql)
	results = dstdbcursor.fetchall()
	
	for row in results:
		hostgroup[row[0]] = row[1]
		
	# load host
	sql = " ".join(config["SQL"]["nghosts"])
	dstdbcursor.execute (sql)
	results = dstdbcursor.fetchall()

	for row in results:
		host[row[0]] = row[1]	
		
	auditid = 1	
	
	srcdbhdlr = get_db_hdlr (config, source)
	srcdbcursor = srcdbhdlr.cursor()
	
	sql = " ".join(config["SQL"]["nagioshostgroup2hosts"])
		
	srcdbcursor.execute (sql)
	results = srcdbcursor.fetchall()
	
	srcset = set()
	for row in results:
		srcset.add(tuple([row[0], row[1]]))
		
	sql = " ".join(config["SQL"]["nghostgroup2hosts"])
	dstdbcursor.execute (sql)
	results = dstdbcursor.fetchall()
	
	dstset = set()
	for row in results:
		dstset.add(tuple([row[0], row[1]]))
		
	misset = ((srcset ^ dstset) - dstset)
	print('{0} records missing in dest (host hostgroup)'.format(len(misset)))
	
	# loop over the missing
	for val in misset:
		hostgroupname = val[0]
		hostname = val[1]

		if (hostgroupname not in hostgroup):
			sql = " ".join(config["SQL"]["insnghostgroups"]).format(hostgroupname, auditid)
			
			try :
				dstdbcursor.execute (sql)
				hostgroup[hostgroupname] = dstdbcursor.lastrowid 		
			except mysql.connector.Error as err:	
				print (sql)
				print("Something went wrong: {}".format(err))
				
		if (hostname not in host):
			#sql = " ".join(config["SQL"]["insnghosts"]).format(hostname, source, auditid)
			sql = " ".join(config["SQL"]["insnghosts"]).format(hostname, source.split('.')[0], auditid)
			
			try :
				dstdbcursor.execute (sql)
				host[hostname] = dstdbcursor.lastrowid
			except mysql.connector.Error as err:	
				print (sql)
				print("Something went wrong: {}".format(err))	
		
		sql = " ".join(config["SQL"]["mapnghost2nghostgroupid"]).format(hostgroup[hostgroupname], host[hostname])
		dstdbcursor.execute (sql)
		results = dstdbcursor.fetchall()
		rowcount = dstdbcursor.rowcount		
			
		if (rowcount == 0):	
			sql = " ".join(config["SQL"]["insmapnghost2nghostgroups"]).format(host[hostname], hostgroup[hostgroupname], auditid)
				
			try:
				dstdbcursor.execute (sql) 	 			
			except mysql.connector.Error as err:
				print (sql)
				print("Something went wrong: {}".format(err))
	
	dstdbhdlr.commit()
			
	########################
	# services section
	########################
	sql = " ".join(config["SQL"]["nagiosservices"])
		
	srcdbcursor.execute (sql)
	results = srcdbcursor.fetchall()
	
	srcset = set()
	for row in results:
		srcset.add(tuple([row[0], row[1]]))
		
	#sql = " ".join(config["SQL"]["ngservice2nghosts"]).format(source)
	sql = " ".join(config["SQL"]["ngservice2nghosts"]).format(source.split('.')[0])

	dstdbcursor.execute (sql)
	results = dstdbcursor.fetchall()
	
	dstset = set()
	for row in results:
		dstset.add(tuple([row[0], row[1]]))		
		
	misset = ((srcset ^ dstset) - dstset)	
	print('{0} records missing in dest (host services)'.format(len(misset)))
	
	service = {}
	
	# load service
	sql = " ".join(config["SQL"]["ngservices"])
	dstdbcursor.execute (sql)
	results = dstdbcursor.fetchall()
	
	for row in results:
		service[row[0]] = row[1]
		
	# loop over the missing
	for val in misset:
		servicename = val[0]
		hostname = val[1]
		#print(servicename + "," + hostname)

		if (servicename not in service):
			sql = " ".join(config["SQL"]["insngservices"]).format(servicename, auditid)
			
			try :
				dstdbcursor.execute (sql)
			except mysql.connector.Error as err:	
				print (sql)	
				print("Something went wrong: {}".format(err))		
			
			service[servicename] = dstdbcursor.lastrowid
		
		sql = " ".join(config["SQL"]["mapngservice2nghostid"]).format(service[servicename], host[hostname])
		dstdbcursor.execute (sql)
		results = dstdbcursor.fetchall()
		rowcount = dstdbcursor.rowcount		
			
		if (rowcount == 0):	
			sql = " ".join(config["SQL"]["insmapngservice2nghosts"]).format(host[hostname], service[servicename], auditid)
			
			try:
				dstdbcursor.execute (sql)
			except mysql.connector.Error as err:		
				print (sql) 	 			
				print("Something went wrong: {}".format(err))
				
	dstdbhdlr.commit()		
	
	##################
	# service status
	##################
	sql = """select name2 ngservice_name, name1 nghost_name, status_update_time, output, 
		case 
			when current_state = 0 then 'Success' 
			when current_state = 1 then 'Warning' 
			when current_state > 1 then 'Error' 
		end summary 
		from nagios_servicestatus a
  			inner join nagios_objects b on (a.service_object_id = b.object_id)"""
		
	srcdbcursor.execute (sql)
	results = srcdbcursor.fetchall()
	
	rowcnt = 0
	for val in results:
		sql = """update mapngservice2nghosts x
				inner join (
  					select mapngservice2nghost_id from mapngservice2nghosts a
  						inner join ngservices b on (a.ngservice_id = b.ngservice_id)
  						inner join nghosts c on (a.nghost_id = c.nghost_id)
  					where b.ngservice_id = {0}
  					and c.nghost_id = {1}
				) y on (x.mapngservice2nghost_id = y.mapngservice2nghost_id)
				set ngservicestatus_time = '{2}', ngservicestatus_text = '{3}', ngservicestatus_summary = '{4}'""".format(service[val[0]],host[val[1]],val[2],val[3].replace("'","''"),val[4])
		dstdbcursor.execute (sql)
	dstdbhdlr.commit()		
				
	srcdbcursor.close()
	dstdbcursor.close()
	
	srcdbhdlr.close()
	dstdbhdlr.close()
		
	return

if __name__ == "__main__":	
	main(sys.argv[1:])
