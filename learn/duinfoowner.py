import os
import sys
import getopt
import logging
import threading
import time


def usage(message):
    if message != "":
        print(message)

    print(me + " -h|--help -f|--file=<file containing paths>")
    sys.exit(0)


def parse_args(argv):
    logging.debug(locals())
    global txtfile
    global runflag
    global olduid

    try:
        opts, args = getopt.getopt(argv, "hrf:o:", ["help", "pathfile="])

        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage("")
            elif opt in ("-r", "--run"):
                runflag = True
            elif opt in ("-o", "--olduid"):
                olduid = arg
            elif opt in ("-f", "--pathfile"):
                txtfile = arg

                if not os.path.exists(txtfile):
                    logging.error("File {0} is not accessible, please provide correct file".format(txtfile))
                    sys.exit(1)
    except getopt.GetoptError as err:
        usage("Error: Invalid parameter(s) -- {0}".format(err))

    return


def main(argv):
    logger.debug(locals())

    parse_args(argv)

    if txtfile is not None:
        if not os.path.exists(txtfile):
            logger.error("File {0} is not accessible, please provide correct file".format(txtfile))
            sys.exit(-1)
    else:
        usage("Missing mandatory paramaeter -f|--file")

    if olduid is None:
        usage("Missing mandatory Old Uid -o|--olduid")

    threads = list()
    fh = open(txtfile)
    index = 0

    for path in fh:
        logger.info("Create and start thread %d.", index)
        x = threading.Thread(target=ownership_change, args=(index, path))
        threads.append(x)
        x.start()
    fh.close()

    for index, thread in enumerate(threads):
        logger.info("Before joining thread %d.", index)
        thread.join()
        logger.info("Thread %d done", index)

    return


def ownership_change(name, path):
    logger.info("Thread %s: starting", name)

    myCmd = "cat {0}/.duinfo | grep Owner | awk '{{print $2}}'".format(path.strip('\n'))
    usr = os.popen(myCmd).read().strip('\n')

    myCmd = "/tools/sysadm/scripts/adphone -a {0} | grep {0} | awk '{{print $(NF-1)}}'".format(usr)
    usr = os.popen(myCmd).read().strip('\n')

    myCmd = "id -u {0}".format(usr)
    uid = os.popen(myCmd).read().strip('\n')

    logger.info("path = {0}, uid = {1}".format(path.strip('\n'), uid))

    myCmd = "find -H {0} -uid {1} -exec chown -h ".format(path.strip('\n'), olduid) + uid + " {} +;"
    if runflag:
        # execute the command
        # os.system(myCmd)
        logger.info(myCmd)

    logger.info("Thread %s: finishing", name)


if __name__ == "__main__":
    me = os.path.basename(__file__)
    txtfile = None
    runflag = False
    olduid = None

    format = "%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(lineno)d - %(message)s"
    logging.basicConfig(format=format, level=logging.INFO)
    logger = logging.getLogger(me)

    main(sys.argv[1:])
