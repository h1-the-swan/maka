import sys, os, time
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)
from sqlalchemy.orm import Session

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
    logger.debug("loading input file: {}".format(fname))
    sys.exit(0)
    start = timer()
    with open(fname, 'r') as f:
        records = json.load(f)
    logger.debug("done loading {} records. took {}".format(len(records), format_timespan(timer()-start)))

    session = Session(db.engine)
    for i, record in enumerate(records):
        try:
            logger.debug("adding record {} (Id: {}))".format(i, record.get('Id'))
            add_record(record, session)
            session.commit()
        except:
            session.rollback()
            raise

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
