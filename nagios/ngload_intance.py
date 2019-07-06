#!/usr/bin/env python

import sys
import getopt
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
        opts, args = getopt.getopt (argv, "hf:", ["help", "file="])

        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage ("")
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
    dblkup = "source"
    hosts = None
    services = None
    keyobj = config.get("DB").get(dblkup)

    for elem in keyobj:
        dbh = dbhelper(keyobj.get(elem)["host"], keyobj.get(elem)["user"], keyobj.get(elem)["passwd"], keyobj.get(elem)["database"])
        sql = """
        select hgx.hostgroup_name, hx.host_name, '{0}' instance_name 
        from nagios_hostgroup_members hgm 
        inner join ( 
            select alias hostgroup_name, hostgroup_id, hostgroup_object_id 
            from nagios_hostgroups hg 
            inner join nagios_objects o on (hg.hostgroup_object_id = o.object_id and o.is_active = 1)
        ) hgx on (hgm.hostgroup_id = hgx.hostgroup_id) 
        inner join (
            select h.alias host_name, h.host_id, h.host_object_id 
            from nagios_hosts h 
            inner join nagios_objects o on (h.host_object_id = o.object_id and o.is_active = 1) 
        ) hx on (hgm.host_object_id = hx.host_object_id)""".format(elem)
        hosts = dbh.get_query_results(sql, results=hosts)

        sql = """
        select display_name service_name, host_name, '{0}' instance_name 
        from nagios_services sn 
        inner join nagios_objects nox on (sn.service_object_id = nox.object_id and nox.is_active = 1)
        inner join (
            select host_object_id, alias host_name 
            from nagios_hosts nh 
            inner join nagios_objects no on (nh.host_object_id = no.object_id and no.is_active = 1) 
        ) hn on (sn.host_object_id = hn.host_object_id)""".format(elem)
        services = dbh.get_query_results(sql, results=services)

    srchostset = dbh.format_results(hosts, returntype="Set")
    srcserviceset = dbh.format_results(services, returntype="Set")

    if logger is not None:
        logger.debug('{0} records in {1} (host hostgroup)'.format(len(srchostset), dblkup))
        logger.debug('{0} records in {1} (service host)'.format(len(srcserviceset), dblkup))
    else:
        print('{0} records in {1} (host hostgroup)'.format(len(srchostset), dblkup))
        print('{0} records in {1} (service host)'.format(len(srcserviceset), dblkup))

    dblkup = "destination"
    auditid = 1
    elem = config.get("DB").get(dblkup)
    dbh = dbhelper(elem["host"], elem["user"], elem["passwd"], elem["database"])

    sql = """
    insert into audit (caller, run_type, run_time) values ('{0}','load',CURRENT_TIMESTAMP)
    """.format(me)
    auditid =dbh.execute_query(sql, returnkey=True)

    sql = "select nghostgroup_name, nghostgroup_id from nghostgroups"
    hostgroupdict = dbh.format_results(dbh.get_query_results(sql), returntype="Dict")

    sql = "select nghost_name, nghost_id from nghosts"
    hostdict = dbh.format_results(dbh.get_query_results(sql), returntype="Dict")

    sql = "select ngservice_name, ngservice_id from ngservices"
    servicedict = dbh.format_results(dbh.get_query_results(sql), returntype="Dict")

    sql = "select nghostinstance_name, nghostinstance_id from nghostinstances"
    hostinstancedict = dbh.format_results(dbh.get_query_results(sql), returntype="Dict")

    sql = """
    select d.nghostgroup_name, c.nghost_name, e.nghostinstance_name
    from mapnghost2nghostgroups a
    inner join mapnghost2nghostinstances b on (a.mapnghost2nghostinstance_id = b.mapnghost2nghostinstance_id)
    inner join nghosts c on (b.nghost_id = c.nghost_id)
    inner join nghostgroups d on (a.nghostgroup_id = d.nghostgroup_id)
    inner join nghostinstances e on (b.nghostinstance_id = e.nghostinstance_id)
    where a.is_active = 1"""
    dsthostset = dbh.format_results(dbh.get_query_results(sql), returntype="Set")

    sql = """
    select s.ngservice_name, h.nghost_name, hi.nghostinstance_name
    from mapngservice2nghosts m
    inner join ngservices s on (s.ngservice_id = m.ngservice_id)
    inner join mapnghost2nghostinstances ms2hi on (m.mapnghost2nghostinstance_id = ms2hi.mapnghost2nghostinstance_id)
    inner join nghosts h on (h.nghost_id = ms2hi.nghost_id)
    inner join nghostinstances hi on (ms2hi.nghostinstance_id = hi.nghostinstance_id)
    where m.is_active = 1"""
    dstserviceset = dbh.format_results(dbh.get_query_results(sql), returntype="Set")

    if logger is not None:
        logger.debug('{0} records in {1} (host hostgroup)'.format(len(dsthostset), dblkup))
        logger.debug('{0} records in {1} (service host)'.format(len(dstserviceset), dblkup))
    else:
        print('{0} records in {1} (host hostgroup)'.format(len(dsthostset), dblkup))
        print('{0} records in {1} (service host)'.format(len(dstserviceset), dblkup))

    ###################
    ## Host 2 Hostgroup
    ###################
    mngset = ((srchostset ^ dsthostset) - dsthostset)

    if logger is not None:
        logger.debug('{0} records missing in {1} (host hostgroup)'.format(len(mngset), dblkup))
    else:
        print('{0} records missing in {1} (host hostgroup)'.format(len(mngset), dblkup))

    for elem in mngset:
        print(elem)
        hostgroupname = elem[0]
        hostname = elem[1]
        hostinstancename = elem[2]

        if hostinstancename not in hostinstancedict:
            sql = """
            insert into nghostinstances (nghostinstance_name, audit_id) 
            values ('{0}', {1})""".format(hostinstancename, auditid)
            inskey = dbh.execute_query(sql, returnkey=True)

            if inskey is not None:
                hostinstancedict[hostinstancename] = inskey

        if hostgroupname not in hostgroupdict:
            sql = """
            insert into nghostgroups (nghostgroup_name, audit_id) 
            values ('{0}', {1})""".format(hostgroupname, auditid)
            inskey = dbh.execute_query(sql, returnkey=True)

            if inskey is not None:
                hostgroupdict[hostgroupname] = inskey

        if hostname not in hostdict:
            sql = """
            insert into nghosts (nghost_name, audit_id) 
            values ('{0}', '{1}')""".format(hostname, auditid)
            inskey = dbh.execute_query(sql, returnkey=True)

            if inskey is not None:
                hostdict[hostname] = inskey

        sql = """
        select a.mapnghost2nghostgroup_id from mapnghost2nghostgroups a
        inner join nghostgroups b on (a.nghostgroup_id = b.nghostgroup_id)
        inner join mapnghost2nghostinstances c on (a.mapnghost2nghostinstance_id = c.mapnghost2nghostinstance_id)
        inner join nghosts d on (c.nghost_id = d.nghost_id)
        inner join nghostinstances e on (c.nghostinstance_id = e.nghostinstance_id)
        where b.nghostgroup_name = '{0}'
        and d.nghost_name = '{1}'
        and e.nghostinstance_name = '{2}'""".format(hostgroupname, hostname, hostinstancename)
        resmaphost2hostgroup = dbh.get_query_results(sql)

        if len(resmaphost2hostgroup) == 0:
            sql = """
            select mapnghost2nghostinstance_id from mapnghost2nghostinstances a
            inner join nghosts b on (a.nghost_id = b.nghost_id)
            inner join nghostinstances c on (a.nghostinstance_id = c.nghostinstance_id)
            where b.nghost_name = '{0}'
            and c.nghostinstance_name = '{1}'""".format(hostname, hostinstancename)
            resmaphost2hostgroupid = dbh.get_query_results(sql)

            inskey = None

            if len(resmaphost2hostgroupid) == 0:
                sql = """
                insert into mapnghost2nghostinstances (nghost_id, nghostinstance_id, audit_id) 
                values ({0}, {1}, {2})""".format(hostdict[hostname], hostinstancedict[hostinstancename], auditid)
                inskey = dbh.execute_query(sql, returnkey=True)
            else:
                for ielem in resmaphost2hostgroupid:
                    inskey = ielem[0]

            if inskey is not None:
                sql = """
                insert into mapnghost2nghostgroups (nghostgroup_id, mapnghost2nghostinstance_id, audit_id) 
                values ({0}, {1}, {2})""".format(hostgroupdict[hostgroupname], inskey, auditid)
                dbh.execute_query(sql)

    mngset = ((srchostset ^ dsthostset) - srchostset)

    if logger is not None:
        logger.debug('{0} records extra in {1} (host hostgroup)'.format(len(mngset), dblkup))
    else:
        print('{0} records extra in {1} (host hostgroup)'.format(len(mngset), dblkup))

    for elem in mngset:
        if logger is not None:
            logger.debug(elem)
        else:
            print(elem)

        sql = """
        update mapnghost2nghostgroups x
        inner join (
            select c.nghost_name, d.nghostgroup_name, e.nghostinstance_name, a.mapnghost2nghostgroup_id
            from mapnghost2nghostgroups a
            inner join mapnghost2nghostinstances b on (a.mapnghost2nghostinstance_id = b.mapnghost2nghostinstance_id)
            inner join nghosts c on (b.nghost_id = c.nghost_id)
            inner join nghostgroups d on (a.nghostgroup_id = d.nghostgroup_id)
            inner join nghostinstances e on (b.nghostinstance_id = e.nghostinstance_id)
            where a.is_active = 1
            and d.nghostgroup_name = '{0}'
            and c.nghost_name = '{1}'
            and e.nghostinstance_name = '{2}'
        ) y on (x.mapnghost2nghostgroup_id = y.mapnghost2nghostgroup_id)
        set is_active = 0""".format(elem[0], elem[1], elem[2])
        dbh.execute_query(sql)

        sql = """
        update mapnghost2nghostinstances x
        inner join (
            select a.mapnghost2nghostinstance_id
            from mapnghost2nghostinstances a
            inner join nghosts b on (a.nghost_id = b.nghost_id)
            inner join nghostinstances c on (a.nghostinstance_id = c.nghostinstance_id)
            where b.nghost_name = '{0}'
            and c.nghostinstance_name = '{1}' 
        ) y on (x.mapnghost2nghostinstance_id = y.mapnghost2nghostinstance_id)
        set is_active = 0""".format(elem[1], elem[2])
        dbh.execute_query(sql)

    ###################
    ## Service 2 Host
    ###################
    mngset = ((srcserviceset ^ dstserviceset) - dstserviceset)

    if logger is not None:
        logger.debug('{0} records missing in {1} (service host)'.format(len(mngset), dblkup))
    else:
        print('{0} records missing in {1} (service host)'.format(len(mngset), dblkup))

    for elem in mngset:
        if logger is not None:
            logger.debug(elem)
        else:
            print(elem)

        servicename = elem[0]
        hostname = elem[1]
        hostinstancename = elem[2]

        if servicename not in servicedict:
            inskey = None

            sql = """
            insert into ngservices (ngservice_name, audit_id) 
            values ('{0}', {1}) """.format(servicename, auditid)
            inskey = dbh.execute_query(sql, returnkey=True)

            if inskey is not None:
                servicedict[servicename] = inskey

        sql = """
        select m.mapngservice2nghost_id, m.is_active, m.ngservice_id, m1.mapnghost2nghostinstance_id
        from mapngservice2nghosts m
        inner join ngservices s on (m.ngservice_id = s.ngservice_id)
        inner join mapnghost2nghostinstances m1 on (m.mapnghost2nghostinstance_id = m1.mapnghost2nghostinstance_id)
        inner join nghosts h on (m1.nghost_id = h.nghost_id)
        inner join nghostinstances hi on (m1.nghostinstance_id = hi.nghostinstance_id)
        where s.ngservice_name = '{0}' 
        and h.nghost_name = '{1}' 
        and hi.nghostinstance_name = '{2}'""".format(servicename, hostname, hostinstancename)
        mapservice2host = dbh.get_query_results(sql)

        if len(mapservice2host) == 0:
            sql = """
            select mapnghost2nghostinstance_id, nghost_id, nghostinstance_id 
            from mapnghost2nghostinstances 
            where nghost_id = {0} 
            and nghostinstance_id = {1}""".format(hostdict[hostname], hostinstancedict[hostinstancename])

            maphost2hostgroup = dbh.get_query_results(sql)
            for row in maphost2hostgroup:
                sql = """
                insert into mapngservice2nghosts (mapnghost2nghostinstance_id, ngservice_id, audit_id) 
                values ({0}, {1}, {2})""".format(row[0], servicedict[servicename], auditid)
                dbh.execute_query(sql)
        else:
            for row in mapservice2host:
                is_active = row[1]

                if is_active == 0:
                    sql = "update mapngservice2nghosts set is_active=1 where mapngservice2nghost_id = {0}".format(row[0])
                    dbh.execute_query(sql)

    mngset = ((srcserviceset ^ dstserviceset) - srcserviceset)

    if logger is not None:
        logger.debug('{0} records extra in {1} (service host)'.format(len(mngset), dblkup))
    else:
        print('{0} records extra in {1} (service host)'.format(len(mngset), dblkup))

    for elem in mngset:
        if logger is not None:
            logger.debug(elem)
        else:
            print(elem)

        sql = """
        update mapngservice2nghosts x
        inner join (
            select a.mapngservice2nghost_id
            from mapngservice2nghosts a
            inner join mapnghost2nghostinstances b on (a.mapnghost2nghostinstance_id = b.mapnghost2nghostinstance_id)
            inner join nghosts c on (b.nghost_id = c.nghost_id)
            inner join ngservices d on (a.ngservice_id = d.ngservice_id)
            inner join nghostinstances e on (b.nghostinstance_id = e.nghostinstance_id)
            where a.is_active = 1
            and d.ngservice_name = '{0}'
            and c.nghost_name = '{1}'
            and e.nghostinstance_name = '{2}'
        ) y on (x.mapngservice2nghost_id = y.mapngservice2nghost_id)
        set is_active = 0""".format(elem[0], elem[1], elem[2])
        dbh.execute_query(sql)

    return


if __name__ == "__main__":
    me = os.path.basename(__file__)
    main(sys.argv[1:])
