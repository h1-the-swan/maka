import sys, os, time
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__').getChild(__name__)

DOTENV_PATH = '../.env'

from dotenv import load_dotenv
load_dotenv(DOTENV_PATH)

from db_connect_mag_201710 import get_db_connection
db = get_db_connection()

def add_multiple_records_papersinno(items, tablename):
    start = timer()
    logger.debug("adding {} rows to {} table".format(len(items), tablename))
    db.engine.execute(db.tables[tablename].insert(), items)
    logger.debug("done. {}".format(format_timespan(timer()-start)))

def add_records(items, tablename, last_added, i, num_added):
    start = timer()
    logger.debug("adding {} records (records {} to {})".format(len(items), last_added, i))
    add_multiple_records_papersinno(items, tablename)
    num_added += len(items)
    logger.debug("done adding {} records to the tables. Time for these inserts: {}".format(len(items), format_timespan(timer()-start)))
    # last_added = i
    logger.debug("")
    return num_added

def main(args):
    fname = args.input
    threshold_to_add = args.add_threshold
    sep = args.sep
    tablename = args.tablename

    num_added = 0
    items = []
    logger.debug("threshold_to_add: {}. this many paper records will be added at a time".format(threshold_to_add))
    logger.debug("table to add to: {}".format(tablename))
    last_added = 0
    logger.debug("reading from {}".format(fname))
    with open(fname, 'r') as f:
        start = timer()
        for i, line in enumerate(f):
            line = line.strip().split(sep)
            if i == 0:  # header row
                header = line
                continue

            if len(line) != len(header):
                logger.warn("len(line) != len(header) ({} vs {}), i=={}. last_added=={}".format(len(line), len(header), i, last_added))
            row = {}
            for pos in range(len(line)):
                d = line[pos]
                if line[pos] == 'NULL':
                    d = None

                field_name = header[pos]
                row[field_name] = d
            items.append(row)
            if len(items) == threshold_to_add:
                logger.debug("threshold reached ({}). adding to database table...".format(format_timespan(timer()-start)))
                num_added = add_records(items, tablename, last_added, i, num_added)
                last_added = i
                items = []
                start = timer()
        # add the final ones
        num_added = add_records(items, tablename, last_added, i, num_added)
    logger.debug("all done. num_added: {}".format(num_added))

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="load papers from tsv")
    parser.add_argument("input", help="TSV filename")
    parser.add_argument("--sep", default='\t', help="delimiter to separate fields")
    parser.add_argument("--tablename", default='Papers', help="database table name to load into")
    parser.add_argument("--add-threshold", default=100000, help="this many records will be added at a time")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    else:
        logger.setLevel(logging.INFO)
    main(args)
    total_end = timer()
    logger.info('all finished. total time: {}'.format(format_timespan(total_end-total_start)))

