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
start = timer()
print("getting db connection")
db = get_db_connection()
print("done. {:.5f}".format(timer()-start))
start = timer()
print("preparing automapped Base")
Base = prepare_base(db)
Paper = Base.classes.Papers
PaperAuthorAffiliation = Base.classes.PaperAuthorAffiliations
PaperReference = Base.classes.PaperReferences
print("done. {:.5f}".format(timer()-start))
sys.stdout.flush()


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

def process_paper_core_method(record):
    # gets the data for the Papers table, the PaperReferences table and the PaperAuthorAffiliations table
    # as dictionaries, to use with sqlalchemy core methods
    paper = {
        'Paper_ID': record.get('Id'),
        'title': record.get('Ti'),
        'date': record.get('D'),
        'year': record.get('Y'),
        'language': record.get('L')
    }
    journal = record.get('J')
    if journal:
        paper['Journal_ID'] = journal.get('JId')
    else:
        paper['Journal_ID'] = None
    conference = record.get('C')
    if conference:
        paper['Conference_series_ID'] = conference.get('CId')
    else:
        paper['Conference_series_ID'] = None
    extended = record.get('E')
    if extended:
        paper['DOI'] = extended.get('DOI')
    else:
        paper['DOI'] = None
    
    prs = []
    references = record.get('RId')
    if references:
        for rid in references:
            pr = {
                'Paper_ID': paper['Paper_ID'],
                'Paper_reference_ID': rid
            }
            prs.append(pr)
    
    paas = []
    authors = record.get('AA')
    if authors:
        for a in authors:
            paa = {
                'Author_ID': a.get('AuId'),
                'Paper_ID': paper['Paper_ID'],
                'Author_name': a.get('AuN'),
                'Affiliation_ID': a.get('AfId')
            }
            paas.append(paa)
            
    return paper, prs, paas

def add_record(record, session):
    p, prs, paas = process_paper(record)
    session.add(p)
    for pr in prs:
        session.add(pr)
    for paa in paas:
        session.add(paa)
    return

def add_record_core_method(record):
    p, prs, paas = process_paper_core_method(record)
    db.engine.execute(db.tables['Papers'].insert(p))
    for pr in prs:
        db.engine.execute(db.tables['PaperReferences'].insert(pr))
    for paa in paas:
        db.engine.execute(db.tables['PaperAuthorAffiliations'].insert(paa))
    return

def add_multiple_records_core_method(items):
    bulk_papers = []
    bulk_pr = []
    bulk_paa = []
    for p, prs, paas in items:
        bulk_papers.append(p)
        bulk_pr.extend(prs)
        bulk_paa.extend(paas)
    logger.debug("adding {} rows to Papers table".format(len(bulk_papers)))
    db.engine.execute(db.tables['Papers'].insert(), bulk_papers)
    logger.debug("adding {} rows to PaperReferences table".format(len(bulk_pr)))
    db.engine.execute(db.tables['PaperReferences'].insert(), bulk_pr)
    logger.debug("adding {} rows to PaperAuthorAffiliations table".format(len(bulk_paa)))
    db.engine.execute(db.tables['PaperAuthorAffiliations'].insert(), bulk_paa)
    logger.debug("done")
    return

def core_method_single(records):
    num_added = 0
    for record in records:
        add_record_core_method(record)
        num_added += 1
    return num_added

def core_method_multiple(records, threshold_to_add=10000):
    num_added = 0
    items = []
    last_added = 0
    for i, record in enumerate(records):
        p, prs, paas = process_paper_core_method(record)
        items.append( (p, prs, paas) )
        if len(items) == threshold_to_add:
            # logger.debug("adding {} records (records {} to {})".format(len(items), last_added, i))
            add_multiple_records_core_method(items)
            num_added += len(items)
            items = []
            last_added = i
    return num_added

def orm_method(records):
    session = Session(db.engine)
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
    return num_added


def delete_records(pids):
    start = timer()
    logger.debug("attempting to delete {} records".format(len(pids)))
    for tbl in [db.tables[x] for x in ['Papers', 'PaperReferences', 'PaperAuthorAffiliations']]:
        this_start = timer()
        stmt = tbl.delete().where(tbl.c.Paper_ID.in_(pids))
        r = db.engine.execute(stmt)
        logger.debug("deleted {} rows from {} in {:.4f} seconds".format(r.rowcount, tbl.name, timer()-this_start))
    logger.debug("done deleting. took {}".format(format_timespan(timer()-start)))
    logger.debug("")
    return


def main(args):
    fname = os.path.abspath(args.input)
    fname_base = os.path.basename(fname)
    logger.debug("loading input file: {}".format(fname))
    start = timer()
    with open(fname, 'r') as f:
        records = json.load(f)
    logger.debug("done loading {} records. took {}".format(len(records), format_timespan(timer()-start)))


    num_test_papers = 10000
    logger.debug("using the first {} papers to test".format(num_test_papers))
    records = records[:num_test_papers]

    records_pids = [x['Id'] for x in records]

    delete_records(records_pids)

    logger.debug("using orm method")
    start = timer()
    num_added = orm_method(records)
    logger.debug("added {} records in {:.4f} seconds".format(num_added, timer()-start))
    logger.debug("")

    delete_records(records_pids)

    threshold_to_add = 10000
    logger.debug("using core method: bulk (with threshold_to_add: {})".format(threshold_to_add))
    start = timer()
    num_added = core_method_multiple(records, threshold_to_add)
    logger.debug("added {} records in {:.4f} seconds".format(num_added, timer()-start))
    logger.debug("")

    delete_records(records_pids)

    logger.debug("using core method: one at a time")
    start = timer()
    num_added = core_method_single(records)
    logger.debug("added {} records in {:.4f} seconds".format(num_added, timer()-start))
    logger.debug("")
    
    delete_records(records_pids)


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

