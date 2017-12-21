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

from paperscraper_journals import make_repeated_queries_by_journal
sys.path.insert(1, './mysql')
from mysql.db_connect_mag_201710 import get_db_connection
from mysql.mysql_load_from_papers_json_fast import add_multiple_records_core_method, process_paper_core_method
from sqlalchemy.orm import Session
from sqlalchemy import exists
from sqlalchemy import Table, Column, BigInteger
db = get_db_connection()
session = Session(db.engine)

LOG_TABLENAME = 'paperscrape_journals'
JOURNALS_FNAME = '/home/jporteno/code/maka/mysql/Journal_IDs_20171107.tsv'

def paper_exists(paper_id):
    return session.query(exists().where(db.tables['Papers'].c.Paper_ID==paper_id)).scalar()

def get_journals_data_from_file(journals_fname, sep='\t', skip_header=True):
    journals_data = []
    with open(journals_fname, 'r') as f:
        for i, line in enumerate(f):
            if i == 0 and skip_header is True:
                continue
            line = line.strip().split(sep)
            jid = int(line[0])
            num_papers = int(line[1])
            journals_data.append( (jid, num_papers) )
    return journals_data

def main(args):
    logtbl = db.tables.get(LOG_TABLENAME)


    journals_data = get_journals_data_from_file(args.idfile)
    logger.debug("loaded journal ids from tsv file {}. there are {} journal ids".format(args.idfile, len(journals_data)))

    start_index = args.start_index
    end_index = args.end_index
    if not end_index:
        end_index = len(journals_data)
    if (not start_index) and (end_index == len(journals_data)):
        logger.debug("looping through all {} journal ids".format(len(journals_data)))
    else:
        logger.debug("looping through journal ids, starting from index {}, ending at index {}".format(start_index, end_index))
    idx = 0
    # begin logging the details of this loading session to database tablje
    logtbl = db.tables.get(LOG_TABLENAME)
    for jid, num_papers in journals_data:
        if idx < start_index:
            idx += 1
            continue
        elif idx >= end_index:
            logger.debug("end index ({}) reached. ending...".format(end_index))
            break
        idx += 1

        exists = db.engine.execute(logtbl.select(logtbl.c['Journal_ID']==jid))
        # check if this input file already exists in the log table
        if exists.rowcount > 0:
            # if the query returned something, the input file is already in the log table, and we have done this one already
            logger.info("this journal id --- {} --- already exists in the database table {}, which means this data has already been loaded. Skipping...".format(jid, LOG_TABLENAME))
            continue

        db_log = {
            'Journal_ID': jid,
            'load_start': datetime.now(),
        }
        logger.debug('looking for papers with Journal_ID {} (the tsv file says there already exist {} papers)'.format(jid, num_papers))
        start = timer()
        all_results, offset = make_repeated_queries_by_journal(jid)
        logger.debug("found {} results in {}".format(len(all_results), format_timespan(timer()-start)))

        new_papers = []
        logger.debug("checking for papers that don't exist in the database...")
        start = timer()
        # for paper in all_results:
        #     paper_id = paper['Id']
        #     if not paper_exists(paper_id):
        #         new_papers.append(paper)
        # logger.debug("identified {} new papers in {}".format(len(new_papers), format_timespan(timer()-start)))
        tmp_tbl = Table("tmp_tbl_journal_{}".format(jid), db.metadata, Column("Paper_ID", BigInteger, primary_key=True))
        tmp_tbl.create()
        pids = [{'Paper_ID': x['Id']} for x in all_results]
        db.engine.execute(tmp_tbl.insert(), pids)

        stmt = "SELECT A.Paper_ID, B.Paper_ID FROM {} AS A LEFT JOIN Papers AS B ON A.Paper_ID=B.Paper_ID WHERE B.Paper_ID IS NULL".format(tmp_tbl.name)
        r = db.engine.execute(stmt)
        r = r.fetchall()
        r = [x[0] for x in r]
        for paper in all_results:
            paper_id = paper['Id']
            if paper_id in r:
                new_papers.append(paper)
        logger.debug("identified {} new papers in {}. adding them to papers, references, and authors table".format(len(new_papers), format_timespan(timer()-start)))
        items = []
        for i, record in enumerate(new_papers):
            p, prs, paas = process_paper_core_method(record)
            items.append( (p, prs, paas) )
        add_multiple_records_core_method(items)

        tmp_tbl.drop(db.engine)
        db_log['load_end'] = datetime.now()
        db_log['num_papers_total'] = len(all_results)
        db_log['num_added'] = len(new_papers)
        db_log['num_papers_exists'] = db_log['num_papers_total'] - db_log['num_added']
        try:
            # logger.debug("adding the details of this loading session to database table {}".format(LOG_TABLENAME))
            db.engine.execute(logtbl.insert(db_log))
        except:
            logger.warn("Something went wrong when logging the details of this loading session to database table {}. Skipping this.".format(LOG_TABLENAME))

        logger.debug("")

        

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="for each journal, get papers, check if they're already in the database, add the missing ones")
    parser.add_argument("--idfile", default=JOURNALS_FNAME, help="file (tsv) with the IDs to search for")
    parser.add_argument("--start-index", type=int, default=0, help="start searching journals from this index in the file")
    parser.add_argument("--end-index", type=int, default=0, help="stop before this index in the file")
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
