import sys, os, time, json
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)

from dotenv import load_dotenv
load_dotenv('../.env')

from db_connect_mag_201710 import get_db_connection
from sqlalchemy.orm import Session
from sqlalchemy import exists

db = get_db_connection()

JSON_FNAME = os.path.abspath('/home/jporteno/mag_data_201710/papers-2016_4.json')

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__').getChild(__name__)

def get_subset_not_in_table(tbl, data, colname="Paper_ID"):
    subset = []
    for item in data:
        paper_id = item['Id']
        e = session.query(exists().where(tbl.c[colname]==paper_id)).scalar()
        if not e:
            subset.append(item)
    return subset

def write_subset_to_file(subset, outfname):
    with open(outfname, 'w') as outf:
        json.dump(subset, outf)

def main(args):
    global session
    session = Session(db.engine)

    with open(JSON_FNAME, 'r') as f:
        logger.debug("loading data from {}".format(JSON_FNAME))
        start = timer()
        data = json.load(f)
        logger.debug("done loading {} records. took {:.3f} seconds".format(len(data), timer()-start))

    for tablename in ['PaperReferences', 'PaperAuthorAffiliations']:
        logger.debug("getting subset for {} table".format(tablename))
        tbl = db.tables[tablename]
        outfname = 'papers-2016_4-missing_from_{}.json'.format(tablename)

        start = timer()
        subset = get_subset_not_in_table(tbl, data)
        logger.debug("done. took {:.3f} seconds".format(timer()-start))

        start = timer()
        logger.debug("writing {} records to {}".format(len(subset), outfname))
        write_subset_to_file(subset, outfname)
        logger.debug("done. took {:.3f} seconds".format(timer()-start))

    session.close()

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="get the subset of the data that is missing from the two tables")
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
