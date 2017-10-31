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

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__').getChild(__name__)

DOTENV_PATH = '../.env'
# keep track of loading these records in a database table
LOG_TABLENAME = 'paperjson_loaded'

from dotenv import load_dotenv
load_dotenv(DOTENV_PATH)

from db_connect_mag_201710 import get_db_connection
def prepare_base(db):
    # http://docs.sqlalchemy.org/en/latest/orm/extensions/automap.html
    from sqlalchemy.ext.automap import automap_base
    Base = automap_base()
    Base.prepare(db.engine, reflect=True)
    return Base
db = get_db_connection()
Base = prepare_base(db)
Paper = Base.classes.Papers
PaperAuthorAffiliation = Base.classes.PaperAuthorAffiliations
PaperReference = Base.classes.PaperReferences


def process_paper(record):
    p = Paper()
    p.Paper_ID = record.get('Id')
    p.title = record.get('Ti')
    p.date = record.get('D')
    p.year = record.get('Y')
    journal = record.get('J')
    if journal:
        p.Journal_ID = journal.get('JId')
    conference = record.get('C')
    if conference:
        p.Conference_series_ID = conference.get('CId')
    extended = record.get('E')
    if extended:
        p.DOI = extended.get('DOI')
    p.language = record.get('L')
        
    prs = []
    references = record.get('RId')
    if references:
        for rid in references:
            pr = PaperReference()
            pr.Paper_ID = p.Paper_ID
            pr.Paper_reference_ID = rid
            prs.append(pr)
    
    paas = []
    authors = record.get('AA')
    if authors:
        for a in authors:
            paa = PaperAuthorAffiliation()
            paa.Author_ID = a.get('AuId')
            paa.Paper_ID = p.Paper_ID
            paa.Author_name = a.get('AuN')
            paa.Affiliation_ID = a.get('AfId')
            paas.append(paa)
    
    return p, prs, paas


def add_record(record, session):
    p, prs, paas = process_paper(record)
    session.add(p)
    for pr in prs:
        session.add(pr)
    for paa in paas:
        session.add(paa)
    return


def main(args):
    fname = os.path.abspath(args.input)
    fname_base = os.path.basename(fname)
    logger.debug("loading input file: {}".format(fname))
    start = timer()
    with open(fname, 'r') as f:
        records = json.load(f)
    logger.debug("done loading {} records. took {}".format(len(records), format_timespan(timer()-start)))

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
