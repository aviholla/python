import mysql.connector
import os
import logging

class dbhelper:
    db = None
    cursor = None
    pcursor = None
    logger = None

    def __init__(self, host, user, passwd, database):
        me = os.path.basename(__file__)
        self.logger = logging.getLogger(me)

        self.host = host
        self.user = user
        self.passwd = passwd
        self.database = database

        try:
            self.db = mysql.connector.connect(host=host, user=user, passwd=passwd, database=database)
            self.pcursor = self.db.cursor(prepared=True)
            self.cursor = self.db.cursor()
        except mysql.connector.Error as err:
            logger.error("Establish DB Connection Error: {0}".format(err))
            logger.error("DB Connection parameter: Host = {0}, User = {1}, Database = {2}".format(self.host, self.user,
                                                                                                  self.database))

    def get_query_results(self, sql, params=None, results=None):
        if results is None:
            self.logger.debug(locals())
            results = []

        try:
            if params is None:
                self.cursor.execute(sql)
            else:
                self.pcursor.execute(sql, params)

            results += self.cursor.fetchall()
        except mysql.connector.Error as err:
            print("Query execution Error: {}".format(err))
            print("SQL: {0}".format(sql))

            if params is not None:
                print(params)

        return results

    def format_results(self, results, returntype="List"):
        self.logger.debug("Input object length = {}".format(len(results)))

        res = None

        if returntype == "Set":
            res = set()
        elif returntype == "Dict":
            res = {}
        else:
            res = results

        if returntype != "List":
            for row in results:
                if returntype == "Set":
                    res.add(row)
                elif returntype == "Dict":
                    res[row[0]] = row[1]

        self.logger.debug("Return object length = {}".format(len(res)))

        return res

    def execute_query(self, sql, params=None, returnkey=False):
        self.logger.debug(locals())

        inskey = None

        try:
            if params is None:
                self.cursor.execute(sql)
            else:
                self.pcursor.execute(sql, params)

            if returnkey:
                inskey = self.cursor.lastrowid

            self.db.commit()
        except mysql.connector.Error as err:
            print("Query execution Error: {}".format(err))
            print("SQL: {0}".format(sql))

            if params is not None:
                print(params)

        return inskey
