#!/usr/bin/python

import getopt
import sys
import json
import os
import logging.config
from lib.helper import dbhelper


def usage(message):
    if message != "":
        print(message)

    print(me + " -f <config file>")
    sys.exit(0)


def get_config_hndlr(argv):
    cfgfile = None

    try:
        opts, args = getopt.getopt(argv, "hf:", ["help", "file="])

        for opt, arg in opts:
            if opt in ("-h", "--help"):
                help(me, "")
            elif opt in ("-f", "--file"):
                cfgfile = arg
    except getopt.GetoptError:
        usage("Missing required parameters (-f|--file=) <config file>")

    if cfgfile is None:
        usage("Missing required parameter (-f|--file=) <config file>")

    with open(cfgfile) as f:
        config = json.load(f)

    return config


def main(argv):
	logging.config.fileConfig('conf/logging.conf')
	logger = logging.getLogger(me)

	config = get_config_hndlr(argv)
	dbh = None

	dblkup = "destination"
	elem = config.get("DB").get(dblkup)
	dbh = dbhelper(elem["host"], elem["user"], elem["passwd"], elem["database"])

	sql = """
	select c.nghostgroup_name, d.nghost_name, e.nghostinstance_name
	from mapnghost2nghostgroups a
	inner join mapnghost2nghostinstances b on (a.mapnghost2nghostinstance_id = b.mapnghost2nghostinstance_id)
	inner join nghostgroups c on (a.nghostgroup_id = c.nghostgroup_id)
	inner join nghosts d on (b.nghost_id = d.nghost_id)
	inner join nghostinstances e on (b.nghostinstance_id = e.nghostinstance_id)
	where a.is_active = 1
	order by c.nghostgroup_name, d.nghost_name
	"""
		
	results = dbh.get_query_results(sql)
	lstmissing = []

	for row in results:
		sql = """
		select fs.* from (
			select hg.hostgroup_name, s.service_name, sg.servicegroup_name, s.service_id, s.altservice_name
			from mapservice2hostgroups shg
				inner join hostgroups hg on (shg.hostgroup_id = hg.hostgroup_id and hg.is_active = 1)
				inner join (
					select service_id, servicegroup_id, ifnull(altservice_name, service_name) altservice_name, service_name, sx.is_active
					from services sx
						left outer join altservices sa on (sx.altservice_id = sa.altservice_id and sa.is_active = 1)
				) s on (shg.service_id = s.service_id and s.is_active = 1)
				inner join servicegroups sg on (s.servicegroup_id = sg.servicegroup_id and sg.is_active = 1)
			where shg.is_active = 1
			and hg.hostgroup_name = '{0}'
		) fs
		left outer join (
			select c.ngservice_id, d.nghost_id, d.nghost_name, c.ngservice_name, g.nghostinstance_name, f.nghostgroup_name
			from mapngservice2nghosts a
				inner join mapnghost2nghostinstances b on (a.mapnghost2nghostinstance_id = b.mapnghost2nghostinstance_id)
				inner join ngservices c on (a.ngservice_id = c.ngservice_id)
				inner join nghosts d on (b.nghost_id = d.nghost_id and b.nghost_id = d.nghost_id)
				inner join mapnghost2nghostgroups e on (b.mapnghost2nghostinstance_id = e.mapnghost2nghostinstance_id)
				inner join nghostgroups f on (e.nghostgroup_id = f.nghostgroup_id)
				inner join nghostinstances g on (b.nghostinstance_id = g.nghostinstance_id)
			where 1=1
			and f.nghostgroup_name = '{0}'
			and d.nghost_name = '{1}'
		) nfs on (fs.altservice_name like concat('%', nfs.ngservice_name, '%') and nfs.nghostgroup_name = fs.hostgroup_name)
		where 1=1
		and nfs.ngservice_name is null
		""".format(row[0], row[1])
		
		res = dbh.get_query_results(sql)
		if len(res) > 0:
			outstr = "{0},{1},{2}".format(row[0], row[1], row[2])
						
			for irow in res:
				tpl = (row[0], row[1], row[2], irow[1], irow[2], "REP1")
				lstmissing.append(tpl)
				#print("{0},{1},{2}".format(outstr, irow[1], irow[2]))

		reports = {"REP2": ['CPU Utilization', 'Memory Utilization'], "REP3": ['File_system']}
		for report in reports:
			services = reports[report]
			for service in services:
				sql = """
				select a.mapngservice2nghost_id from mapngservice2nghosts a
					inner join ngservices b on (a.ngservice_id = b.ngservice_id)
					inner join mapnghost2nghostinstances c on (a.mapnghost2nghostinstance_id = c.mapnghost2nghostinstance_id)
					inner join nghosts d on (c.nghost_id = d.nghost_id)
				where d.nghost_name = '{0}'
				and b.ngservice_name like '{1}%';
				""".format(row[1], service)

				res = dbh.get_query_results(sql)
				if len(res) == 0:
					tpl = (row[0], row[1], row[2], service, "System Checks", report)
					lstmissing.append(tpl)

	cnt = 0
	for row in lstmissing:
		if cnt == 0:
			sql = "truncate table repngmissingservice2hosts"
			dbh.execute_query(sql)

		sql = """
		insert into repngmissingservice2hosts 
		(nghostgroup_name, nghost_name, nghostinstance_name, ngservice_name, ngservicegroup_name, report_type)
		values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')
		""".format(row[0], row[1], row[2], row[3], row[4], row[5])
		dbh.execute_query(sql)

		if logger is not None:
			logger.debug(row)
		else:
			print(row)
		cnt += 1

	return


if __name__ == "__main__":
	me = os.path.basename(__file__)
	main(sys.argv[1:])
