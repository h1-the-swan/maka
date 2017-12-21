# a crash while loading the database means some records are missing information
# for the PaperReferences and PaperAuthorAffiliations table
# This script loads those in while handling IntegrityErrors if they're already present

import sys, os, time, json
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import exists

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__').getChild(__name__)

if sys.version_info[0] >= 3:  #if python 3:
    basestring = str

DOTENV_PATH = '../.env'
# keep track of loading these records in a database table
LOG_TABLENAME = 'paperjson_loaded'

from dotenv import load_dotenv
load_dotenv(DOTENV_PATH)

from db_connect_mag_201710 import get_db_connection, prepare_base
db = get_db_connection()
Base = prepare_base(db)
Paper = Base.classes.Papers
PaperAuthorAffiliation = Base.classes.PaperAuthorAffiliations
PaperReference = Base.classes.PaperReferences

INTEGRITY_ERRORS = [0, 0]


def record_exists(obj, tbl):
    if isinstance(tbl, basestring):
        tbl = db.tables[tbl]
    try:
        paper_id = obj.Paper_ID
    except AttributeError:
        paper_id = obj
    return session.query(exists().where(tbl.c.Paper_ID==paper_id)).scalar()

def process_paper(record):
    # only need references and authors
    # p = Paper()
    # p.Paper_ID = record.get('Id')
    # p.title = record.get('Ti')
    # p.date = record.get('D')
    # p.year = record.get('Y')
    # journal = record.get('J')
    # if journal:
    #     p.Journal_ID = journal.get('JId')
    # conference = record.get('C')
    # if conference:
    #     p.Conference_series_ID = conference.get('CId')
    # extended = record.get('E')
    # if extended:
    #     p.DOI = extended.get('DOI')
    # p.language = record.get('L')
        
    paper_id = record.get('Id')
    prs = []
    # exists = 0
    # if record_exists(paper_id, 'PaperReferences'):
    #     exists += 1
    # else:
    references = record.get('RId')
    if references:
        for rid in references:
            pr = PaperReference()
            # pr.Paper_ID = p.Paper_ID
            pr.Paper_ID = paper_id
            pr.Paper_reference_ID = rid
            prs.append(pr)
    
    paas = []
    authors = record.get('AA')
    # if record_exists(paper_id, 'PaperAuthorAffiliations'):
    #     exists += 1
    # else:
    if authors:
        for a in authors:
            paa = PaperAuthorAffiliation()
            paa.Author_ID = a.get('AuId')
            # paa.Paper_ID = p.Paper_ID
            paa.Paper_ID = paper_id
            paa.Author_name = a.get('AuN')
            paa.Affiliation_ID = a.get('AfId')
            paas.append(paa)

    # if exists > 0:
    #     logger.debug("Paper_ID {} already exists in {} of the 2 tables. skipping".format(paper_id, exists))
    
    return prs, paas


def add_record(record, session):
    # p, prs, paas = process_paper(record)
    prs, paas = process_paper(record)
    # session.add(p)
    if prs:
        for pr in prs:
            try:
                session.add(pr)
                session.commit()
            except IntegrityError:
                INTEGRITY_ERRORS[0] += 1
                session.rollback()
            except:  # any other error
                session.rollback()
                raise
    if paas:
        for paa in paas:
            try:
                session.add(paa)
                session.commit()
            except IntegrityError:
                INTEGRITY_ERRORS[1] += 1
                session.rollback()
            except:  # any other error
                session.rollback()
                raise
    return


def main(args):
    fname = os.path.abspath(args.input)
    fname_base = os.path.basename(fname)
    logger.debug("loading input file: {}".format(fname))
    start = timer()
    with open(fname, 'r') as f:
        records = json.load(f)
    logger.debug("done loading {} records. took {}".format(len(records), format_timespan(timer()-start)))

    global session
    session = Session(db.engine)

    # keep track of loading these records in a database table
    if Base.classes.has_key(LOG_TABLENAME):
        DBLog = Base.classes[LOG_TABLENAME]
        # check if this input file already exists in the log table
        exists = session.query(DBLog).filter(DBLog.json_fname==fname_base).scalar()
        if exists is not None:
            # if the query returned something, the input file is already in the log table, and we have done this one already
            logger.error("this input file --- {} --- already exists in the database table {}, which means this data has already been loaded. Exiting...".format(fname_base, LOG_TABLENAME))
            sys.exit(1)

        # begin logging the details of this loading session to database table
        db_log = DBLog()
        db_log.json_fname = fname_base
        db_log.load_start = datetime.now()
        db_log.num_records = len(records)
        logger.debug("logging the details of this loading session to database table {}".format(LOG_TABLENAME))
        
    else:
        db_log = None
        logger.debug("database table {} not found. not logging the details of this loading session to a database table".format(LOG_TABLENAME))
        

    num_added = 0
    for i, record in enumerate(records):
        if i in [0,1,5,10,100,500,1000,5000] or i % 10000 == 0:
            logger.debug("i=={}. {} integrity errors for references table; {} integrity errors for authors table".format(i, INTEGRITY_ERRORS[0], INTEGRITY_ERRORS[1]))
        try:
            if i in [0,1,5,10,100,500,1000,5000] or i % 10000 == 0:
                logger.debug("adding record {} (Id: {}))".format(i, record.get('Id')))
            add_record(record, session)
            session.commit()
            num_added += 1
        except IntegrityError:
            logger.warn("record {} (Id: {}) encountered an IntegrityError (means it's already in the database). Skipping".format(i, record.get('Id')))
            session.rollback()
        except:  # any other exception
            session.rollback()
            raise
    logger.info("done adding papers. {} added total (out of {} records)".format(num_added, len(records)))

    if db_log:
        db_log.load_end = datetime.now()
        db_log.num_added = num_added
        try:
            logger.debug("adding the details of this loading session to database table {}".format(LOG_TABLENAME))
            session.add(db_log)
            session.commit()
        except:
            session.rollback()
            logger.warn("Something went wrong when logging the details of this loading session to database table {}. Skipping this.".format(LOG_TABLENAME))

    session.close()
        

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="load the data from a json file (created using paperscraper.py) into mysql")
    parser.add_argument("input", help="input filename")
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

