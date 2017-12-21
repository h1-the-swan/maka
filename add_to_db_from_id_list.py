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

from paperscraper import make_short_query
sys.path.insert(1, './mysql')
from mysql.db_connect_mag_201710 import get_db_connection
from mysql.mysql_load_from_papers_json_fast import add_multiple_records_core_method, process_paper_core_method
from sqlalchemy.orm import Session
from sqlalchemy import exists
from sqlalchemy import Table, Column, BigInteger
from sqlalchemy.exc import IntegrityError
db = get_db_connection()
# session = Session(db.engine)

from multiprocessing.pool import ThreadPool

LOGFILE_ADDED_PAPERS = 'missing_Paper_ID_from_references_added.txt'
LOGFILE_API_MISSING_PAPERS = 'missing_Paper_ID_from_references_apimissing.txt'
THRESHOLD_TO_ADD = 1000
NUM_CONCURRENT_PROCESSES = 3

def get_pids_from_file(fname):
    pids = []
    with open(fname, 'r') as f:
        for line in f:
            line = line.strip()
            pids.append(int(line))
    return pids

def log_integrityerrors(records):
    # items = []
    for r in records:
        pid = r['Id']
        item = {'Paper_ID': pid}
        try:
            db.engine.execute(db.tables['unexpected_integrity_errors'].insert(), item)
        except IntegrityError:
            pass
        # items.append({'Paper_ID': pid})
    # db.engine.execute(db.tables['unexpected_integrity_errors'].insert(), items)

def add_records_to_db(records):
    items = []
    for i, record in enumerate(records):
        p, prs, paas = process_paper_core_method(record)
        items.append( (p, prs, paas) )
    try:
        add_multiple_records_core_method(items)
    except IntegrityError:
        logger.warn("encountered IntegrityError. logging all offending {} paper ids to database table and skipping".format(len(records)))
        log_integrityerrors(records)
        return 1
    return 0

def query_pids_and_add_to_db(pids, i_range=[0,0], threshold_to_add=THRESHOLD_TO_ADD):
    num_added = 0
    records = []
    last_added = 0
    last_added_global = i_range[0]
    debug_interval = 600  # output debugging info every x seconds
    time_at_last_debug = timer()
    num_missing_from_api = 0
    for i, pid in enumerate(pids):
        # if i < start_index:
        #     continue
        if i > 0 and i % 10000 == 0:
            # take a break every 10000
            time.sleep(100)

        curr_time = timer()
        time_diff = curr_time - time_at_last_debug
        if time_diff >= debug_interval:
            logger.debug("current status: len(records): {}. num_added: {}. last_added: {}. last_added_global: {}. num_missing_from_api: {}.".format(len(records), num_added, last_added, last_added_global, num_missing_from_api))
            time_at_last_debug = curr_time
        global_index = i_range[0] + i
        this_records = make_short_query("Id={}".format(pid))
        if len(this_records) == 0:
            logger.warn("pid {} not found (index {})".format(pid, global_index))
            num_missing_from_api += 1
            with open(LOGFILE_API_MISSING_PAPERS, 'a') as outf:
                outf.write("{}\n".format(pid))
            continue
        elif len(this_records) > 1:
            logger.warn("multiple records found when querying for Id={} (index {}) using the first one".format(pid, global_index))
        records.append(this_records[0])
        time.sleep(.1)
        if len(records) == threshold_to_add or i == len(pids)-1:
            start = timer()
            logger.debug("adding {} records (records {} to {})".format(len(records), last_added_global, global_index))
            status = add_records_to_db(records)
            if status == 0:  # successful
                num_added += len(records)
                logger.debug("done adding {} records to the tables (records {} to {}). Time for these inserts: {}".format(len(records), last_added_global, global_index, format_timespan(timer()-start)))
                with open(LOGFILE_ADDED_PAPERS, 'a') as outf:
                    for _pid in [r['Id'] for r in records]:
                        outf.write("{}\n".format(_pid))
            records = []
            last_added = i
            last_added_global = global_index
            logger.debug("")


def unpack_args(arg_dict):
    query_pids_and_add_to_db(**arg_dict)

def remove_skip_ids(pids, skipfilename):
    skip = get_pids_from_file(skipfilename)
    logger.debug("skipping paper ids in file {}. {} paper ids found".format(args.skip, len(skip)))
    skip = set(skip)
    pids = set(pids).difference(skip)
    pids = list(pids)
    return pids

def main(args):
    pids = get_pids_from_file(args.idfile)
    logger.debug("loaded paper ids from file {}. there are {} paper ids".format(args.idfile, len(pids)))

    if args.skip:
        pids = remove_skip_ids(pids, args.skip)
        logger.debug("{} paper ids remain after removing the ones to skip".format(len(pids)))

    if args.api_missing:
        pids = remove_skip_ids(pids, args.api_missing)
        logger.debug("{} paper ids remain after removing the ones to skip".format(len(pids)))

    start_i = args.start_index
    # end_index = args.end_index
    # if not end_index:
    #     end_index = len(pids)
    end_index = len(pids)  # this isn't really implemented right now
    if (not start_i) and (end_index == len(pids)):
        logger.debug("looping through all {} paper ids".format(len(pids)))
    else:
        logger.debug("looping through paper ids, starting from index {}, ending at index {}".format(start_i, end_index))

    # threshold_to_add = 100000
    # threshold_to_add = 20000
    threshold_to_add = THRESHOLD_TO_ADD
    logger.debug("threshold_to_add: {}. this many paper records will be added at a time".format(threshold_to_add))

    # split the data into bins for parallel processing
    step = threshold_to_add
    num_pids = len(pids)
    arg_dicts = []
    while True:
        end_i = min(start_i + step, num_pids)
        pids_subset = pids[start_i:end_i]
        i_range = [start_i, end_i]
        this_arg_dict = {
                'pids': pids_subset,
                'i_range': i_range,
                'threshold_to_add': threshold_to_add
        }
        arg_dicts.append(this_arg_dict)

        if end_i == num_pids:
            break
        start_i = end_i

    logger.debug("starting a pool of workers with {} processes".format(NUM_CONCURRENT_PROCESSES))
    pool = ThreadPool(processes=NUM_CONCURRENT_PROCESSES)  # use threadpool because this is an io-bound process
    logger.debug("mapping {} processes to the pool".format(len(arg_dicts)))
    pool.map(unpack_args, arg_dicts)


        

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="given a list of paper ids, query the api and add them to the database, without checking whether they exist first")
    parser.add_argument("idfile", help="file (newline separated) with the IDs to search for")
    parser.add_argument("--skip", help="file (newline separated) with the IDs to skip")
    parser.add_argument("--api-missing", help="file (newline separated) with IDs that have already been tried, but were not found using the API")
    parser.add_argument("--start-index", type=int, default=0, help="start searching journals from this index in the file")
    # parser.add_argument("--end-index", type=int, default=0, help="stop before this index in the file")
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

