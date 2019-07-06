#!/usr/bin/env python

import sys
import getopt
import json
import os
import logging.config
from lib.helper import dbhelper


def usage(message):
    logger.debug(locals())

    if message != "":
        print(message)

    print(me + " -h|--help -d|--confdir=<config directory>")
    sys.exit(0)


def parseargs(argv):
    logger.debug(locals())
    global cfgdir

    try:
        opts, args = getopt.getopt(argv, "hd:", ["help", "confdir="])

        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage("")
            elif opt in ("-d", "--confdir"):
                cfgdir = arg
    except getopt.GetoptError as err:
        usage("Error: Invalid parameter(s) -- {0}".format(err))

    return


def getconfighndlr(cfgfile):
    logger.debug(locals())

    config = None

    cfile = "{0}/{1}".format(cfgdir, cfgfile)
    logger.debug("Config file: {0}".format(cfile))

    if os.path.exists(cfile):
        with open(cfile) as f:
            config = json.load(f)
    else:
        logger.error("Error: Not a valid path")

    return config


def main(argv):
    logger.debug(locals())

    parseargs(argv)
    logger.debug(cfgdir)

    dbconfig = getconfighndlr("dbconfig.json")

    if dbconfig is None:
        logger.error("Can't continue, will exit")
        sys.exit(0)

    sqlconfig = getconfighndlr("{}.json".format(os.path.splitext(me)[0]))

    if sqlconfig is None:
        logger.error("Can't continue, will exit")
        sys.exit(0)

    logger.debug(sqlconfig)

    dbh = None
    jsonkey = "source"
    keyobj = dbconfig.get("DB").get(jsonkey)
    servicestatus = None

    for elem in keyobj:
        dbh = dbhelper(keyobj.get(elem)["host"], keyobj.get(elem)["user"], keyobj.get(elem)["passwd"],
                       keyobj.get(elem)["database"])
        sql = " ".join(sqlconfig.get(jsonkey).get("selservicestatus")).format(elem)
        servicestatus = dbh.get_query_results(sql, results=servicestatus)

    if logger is not None:
        logger.debug('{0} records need updates'.format(len(servicestatus)))
    else:
        print('{0} records need updates'.format(len(servicestatus)))

    jsonkey = "destination"
    elem = dbconfig.get("DB").get(jsonkey)
    dbh = dbhelper(elem["host"], elem["user"], elem["passwd"], elem["database"])

    sql = " ".join(sqlconfig.get(jsonkey).get("insaudit")).format(me)
    auditid = dbh.execute_query(sql, returnkey=True)

    for row in servicestatus:
        sql = " ".join(sqlconfig.get(jsonkey).get("updservicestatus")).format(row[0], row[1], row[2], row[3], row[4].replace("'", "''"), row[5], auditid)
        dbh.execute_query(sql)

    return


if __name__ == "__main__":
    me = os.path.basename(__file__)
    mex = os.path.splitext(me)[0]

    path = os.path.dirname(os.path.realpath(__file__))
    cfgdir = "{0}/conf".format(path)

    logging.config.fileConfig("{0}/logging.conf".format(cfgdir))
    logger = logging.getLogger(me)

    main(sys.argv[1:])
