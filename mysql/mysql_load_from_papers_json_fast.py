# When loading many records, it is much faster to bypass sqlalchemy's ORM and use its core instead
# http://docs.sqlalchemy.org/en/latest/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow
import sys, os, time, json
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)
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

TABLENAMES = [
        'Papers',
        'PaperReferences',
        'PaperAuthorAffiliations'
]

from dotenv import load_dotenv
load_dotenv(DOTENV_PATH)

from db_connect_mag_201710 import get_db_connection
# def prepare_base(db):
#     # http://docs.sqlalchemy.org/en/latest/orm/extensions/automap.html
#     from sqlalchemy.ext.automap import automap_base
#     Base = automap_base()
#     Base.prepare(db.engine, reflect=True)
#     return Base
db = get_db_connection()
# Base = prepare_base(db)
# Paper = Base.classes.Papers
# PaperAuthorAffiliation = Base.classes.PaperAuthorAffiliations
# PaperReference = Base.classes.PaperReferences


# def process_paper(record):
#     p = Paper()
#     p.Paper_ID = record.get('Id')
#     p.title = record.get('Ti')
#     p.date = record.get('D')
#     p.year = record.get('Y')
#     journal = record.get('J')
#     if journal:
#         p.Journal_ID = journal.get('JId')
#     conference = record.get('C')
#     if conference:
#         p.Conference_series_ID = conference.get('CId')
#     extended = record.get('E')
#     if extended:
#         p.DOI = extended.get('DOI')
#     p.language = record.get('L')
#         
#     prs = []
#     references = record.get('RId')
#     if references:
#         for rid in references:
#             pr = PaperReference()
#             pr.Paper_ID = p.Paper_ID
#             pr.Paper_reference_ID = rid
#             prs.append(pr)
#     
#     paas = []
#     authors = record.get('AA')
#     if authors:
#         for a in authors:
#             paa = PaperAuthorAffiliation()
#             paa.Author_ID = a.get('AuId')
#             paa.Paper_ID = p.Paper_ID
#             paa.Author_name = a.get('AuN')
#             paa.Affiliation_ID = a.get('AfId')
#             paas.append(paa)
#     
#     return p, prs, paas

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

# def add_record(record, session):
#     p, prs, paas = process_paper(record)
#     session.add(p)
#     for pr in prs:
#         session.add(pr)
#     for paa in paas:
#         session.add(paa)
#     return

def add_record_core_method(record):
    p, prs, paas = process_paper_core_method(record)
    db.engine.execute(db.tables['Papers'].insert(p))
    for pr in prs:
        db.engine.execute(db.tables['PaperReferences'].insert(pr))
    for paa in paas:
        db.engine.execute(db.tables['PaperAuthorAffiliations'].insert(paa))
    return

def add_multiple_records_core_method(items, with_only_table=None):
    bulk_papers = []
    bulk_pr = []
    bulk_paa = []
    for p, prs, paas in items:
        bulk_papers.append(p)
        bulk_pr.extend(prs)
        bulk_paa.extend(paas)
    
    if with_only_table is None or with_only_table == 'Papers':
        start = timer()
        logger.debug("adding {} rows to Papers table".format(len(bulk_papers)))
        if bulk_papers:
            db.engine.execute(db.tables['Papers'].insert(), bulk_papers)
        logger.debug("done. {}".format(format_timespan(timer()-start)))

    if with_only_table is None or with_only_table == 'PaperReferences':
        start = timer()
        logger.debug("adding {} rows to PaperReferences table".format(len(bulk_pr)))
        if bulk_pr:
            db.engine.execute(db.tables['PaperReferences'].insert(), bulk_pr)
        logger.debug("done. {}".format(format_timespan(timer()-start)))

    if with_only_table is None or with_only_table == 'PaperAuthorAffiliations':
        start = timer()
        logger.debug("adding {} rows to PaperAuthorAffiliations table".format(len(bulk_paa)))
        if bulk_paa:
            db.engine.execute(db.tables['PaperAuthorAffiliations'].insert(), bulk_paa)
        logger.debug("done. {}".format(format_timespan(timer()-start)))
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

    # session = Session(db.engine)

    if args.tablename is not None:
        LOG_TABLENAME = "paperjson_loaded_{}_only".format(args.tablename)
    # keep track of loading these records in a database table
    logtbl = db.tables.get(LOG_TABLENAME)
    if LOG_TABLENAME:
        exists = db.engine.execute(logtbl.select(logtbl.c['json_fname']==fname_base))
        # check if this input file already exists in the log table
        if exists.rowcount > 0:
            # if the query returned something, the input file is already in the log table, and we have done this one already
            logger.error("this input file --- {} --- already exists in the database table {}, which means this data has already been loaded. Exiting...".format(fname_base, LOG_TABLENAME))
            sys.exit(1)

        # begin logging the details of this loading session to database table
        db_log = {
            'json_fname': fname_base,
            'load_start': datetime.now(),
            'num_records': len(records)
        }
        logger.debug("logging the details of this loading session to database table {}".format(LOG_TABLENAME))

    else:
        db_log = None
        logger.debug("database table {} not found. not logging the details of this loading session to a database table".format(LOG_TABLENAME))


    with_only_table = args.tablename

    num_added = 0
    items = []
    # threshold_to_add = 100000
    threshold_to_add = 20000
    logger.debug("threshold_to_add: {}. this many paper records will be added at a time".format(threshold_to_add))
    last_added = 0
    for i, record in enumerate(records):
        p, prs, paas = process_paper_core_method(record)
        items.append( (p, prs, paas) )
        if len(items) == threshold_to_add or i == len(records)-1:
            start = timer()
            logger.debug("adding {} records (records {} to {})".format(len(items), last_added, i))
            add_multiple_records_core_method(items, with_only_table=with_only_table)
            num_added += len(items)
            logger.debug("done adding {} records to the tables. Time for these inserts: {}".format(len(items), format_timespan(timer()-start)))
            items = []
            last_added = i
            logger.debug("")

    #     try:
    #         if i in [0,1,5,10,100,500,1000,5000] or i % 10000 == 0:
    #             logger.debug("adding record {} (Id: {}))".format(i, record.get('Id')))
    #         add_record_core_method(record)
    #         num_added += 1
    #     except IntegrityError:
    #         logger.warn("record {} (Id: {}) encountered an IntegrityError (means it's already in the database). Skipping".format(i, record.get('Id')))
    #     except:  # any other exception
    #         raise
    # logger.info("done adding papers. {} added total (out of {} records)".format(num_added, len(records)))

    if db_log:
        db_log['load_end'] = datetime.now()
        db_log['num_added'] = num_added
        try:
            logger.debug("adding the details of this loading session to database table {}".format(LOG_TABLENAME))
            db.engine.execute(logtbl.insert(db_log))
        except:
            logger.warn("Something went wrong when logging the details of this loading session to database table {}. Skipping this.".format(LOG_TABLENAME))
        

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="load the data from a json file (created using paperscraper.py) into mysql")
    parser.add_argument("input", help="input filename")
    parser.add_argument("--tablename", choices=set(TABLENAMES), help="if you only want to add data to one table, specify it with this option. Otherwise the data will be added to all 3 tables")
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
