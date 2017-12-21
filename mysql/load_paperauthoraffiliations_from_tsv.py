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

def add_multiple_records_to_table(items, tablename):
    start = timer()
    logger.debug("adding {} rows to {} table".format(len(items), tablename))
    db.engine.execute(db.tables[tablename].insert(), items)
    logger.debug("done. {}".format(format_timespan(timer()-start)))

def add_records(items, tablename, last_added, i, num_added):
    start = timer()
    logger.debug("adding {} records (records {} to {})".format(len(items), last_added, i))
    add_multiple_records_to_table(items, tablename)
    num_added += len(items)
    logger.debug("done adding {} records to the tables. Time for these inserts: {}".format(len(items), format_timespan(timer()-start)))
    # last_added = i
    logger.debug("")
    return num_added

def main(args):
    fname = args.input
    threshold_to_add = args.add_threshold
    sep = args.sep
    header = True  # input tsv file contains a header row
    tablename = args.tablename

    num_added = 0
    items = []
    logger.debug("input file: {}".format(fname))
    logger.debug("starting on line {}".format(args.startline))
    if args.endline > 0:
        logger.debug("ending on line {}".format(args.endline))
    else:
        logger.debug("no ending line specified. will go until end of file")
    logger.debug("threshold_to_add: {}. this many paper records will be added at a time".format(threshold_to_add))
    logger.debug("table to add to: {}".format(tablename))
    last_added = 0
    logger.debug("reading from {}".format(fname))
    with open(fname, 'r') as f:
        start = timer()
        for i, line in enumerate(f):
            if i == 0 and header is True:
                continue
            if args.endline > 0 and i == args.endline:
                logger.debug("reached line {}. stopping...".format(i))
                break
            if i < args.startline:
                continue
            line = line.strip().split(sep)
            for line_idx in range(len(line)):
                if line[line_idx] == 'NULL':
                    line[line_idx] = None
            row = {
                    'Paper_ID': line[0],
                    'Author_ID': line[1],
                    'Affiliation_ID': line[2],
                    'Affiliation_name': line[3],
                    'Author_sequence_number': line[4],
                    'Author_name': line[5]
            }
            items.append(row)
            if len(items) == threshold_to_add:
                logger.debug("threshold reached ({}). adding to database table...".format(format_timespan(timer()-start)))
                num_added = add_records(items, tablename, last_added, i, num_added)
                last_added = i
                items = []
                start = timer()
        # add the final ones
        if items:
            num_added = add_records(items, tablename, last_added, i, num_added)
    logger.debug("all done. num_added: {}".format(num_added))

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="load PaperAuthorAffiliations from tsv")
    parser.add_argument("input", help="TSV filename")
    parser.add_argument("--sep", default='\t', help="delimiter to separate fields")
    parser.add_argument("--tablename", default='PaperAuthorAffiliations', help="database table name to load into")
    parser.add_argument("--add-threshold", type=int, default=100000, help="this many records will be added at a time (default: 100000)")
    parser.add_argument("--startline", type=int, default=0, help="start at this line number in the input file (default 0)")
    parser.add_argument("--endline", type=int, default=-1, help="end at this line number in the input file (default: end of file)")
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

