import sys, os, time, json, requests
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)

import classes
import inquirer
from inquirer import AcademicQueryType as querytype
from dotenv import load_dotenv
dotenv_path = os.path.abspath('./.env')
load_dotenv(dotenv_path)

from paperscraper import generic_evaluate_query_from_querier, get_querier, check_type, process_entities

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__').getChild(__name__)

# def reinquire(q_args, offset):
#     new_payload = q_args['payload'].copy()
#     new_payload['offset'] = offset
#     new_payload['count'] = 1000
#     new_args = {
#                 'query_type': q_args['query_type'],
#                 'payload': new_payload,
#                 'parent': q_args['parent']
#             }
#     q = inquirer.AcademicQuerier(new_args['query_type'], new_args['payload'])
#     results = q.post()
#     return new_args, results

# def process_entities(entities):
#     processed = []
#     for e in entities:
#         # JSON encode extended metadata
#         e['E'] = json.loads(e['E'])
#
#         p = {
#             'Id': e['Id'],
#             'Ti': e['Ti'],
#             'L': e.get('L'),
#             'Y': e['Y'],
#             'D': e.get('D'),
#             'RId': e.get('RId'),
#             'AA': [],
#             'F': [],
#             'J': e.get('J'),
#             'C': e.get('C'),
#             'E': {'DOI': e['E'].get('DOI')}
#         }
#         authors = e.get('AA', [])
#         for a in authors:
#             p['AA'].append({
#                     'AuId': a['AuId'],
#                     'AuN': a.get('AuN'),
#                     'AfId': a.get('AfId')
#             })
#         fields = e.get('F', [])
#         for f in fields:
#             p['F'].append({'FId': f['FId']})
#         processed.append(p)
#     return processed

def query_with_error_handling(querier, max_attempts=4):
    attempts = 0
    while True:
        try:
            j = generic_evaluate_query_from_querier(querier)
            if attempts > 0:
                logger.info('query succeeded after {} attempts'.format(attempts+1))
            success = True
            break
        except classes.Error as e:
            attempts += 1
            if attempts >= max_attempts:
                logger.error('max attempts exceeded with query_offset=={}. giving up: writing what we have to json and exiting...'.format(querier.query.offset))
                j = None
                success = False
                break
            delay = min(60 * (attempts + 1), 300)
            querier.query.count = 50
            logger.warn("bad request: {}. sleeping {} seconds and trying again with query count {} (attempt {})".format(e, delay, querier.query.count, attempts))
            time.sleep(delay)
            continue
    return j, success

def make_repeated_queries_by_journal(jid, id_str='J.JId', offset_start=0, offset_thresh=0):
    all_results = []
    # num_results = 0
    querier = get_querier("Composite({}={})".format(id_str, jid))
    querier.query.offset = offset_start
    logger.debug('making first query with args: {}'.format(querier.query.get_body()))
    # j = generic_evaluate_query_from_querier(querier)
    j, success = query_with_error_handling(querier)
    if not success:
        return all_results, querier.query.offset
    logger.debug('query done')
    entities = j['entities']
    check_type(entities)
    processed = process_entities(entities)
    all_results.extend(processed)
    # processed = process_results(results, args.attributes.split(','))
    # all_results.extend(processed)
    # num_results += len(results)

    if len(all_results) == 0:
        return all_results, querier.query.offset

    i = 0
    querier.query.offset = querier.query.offset + querier.query.count
    while True:
        querier.query.count = 1000
        logger.debug('making query {} with args: {}'.format(i+1, querier.query.get_body()))
        # NOTE: when the offset gets too high (somewhere between 2e6 and 3e6) we will start getting a lot of timeout errors (these are returned as 200 but the entities are empty and it has an 'aborted' attribute
        # it looks like we'll have to give up on this method when this starts happening.
        j, success = query_with_error_handling(querier)
        if not success:
            return all_results, querier.query.offset
        entities = j['entities']
        if not entities:
            break
        check_type(entities)
        processed = process_entities(entities)
        all_results.extend(processed)
        # processed = process_results(results, args.attributes.split(','))
        time.sleep(1)
        i += 1
        if i in [20, 50] or i % 100 == 0:
            logger.debug("{} queries completed. num_results: {}".format(i+1, len(all_results)))
        querier.query.offset = querier.query.offset + querier.query.count
        if (offset_thresh != 0) and (querier.query.offset >= offset_thresh):
            break
    return all_results, querier.query.offset

def main(args):
    outdir = os.path.abspath(args.outdir)
    if args.offset_thresh == 0:
        if not args.out:
            outfname = 'papers_byjournal-{}.json'.format(args.jid)
        else:
            outfname = args.out
        outfpath = os.path.join(outdir, outfname)
        if os.path.exists(outfpath):
            logger.error("file {} exists. exiting.".format(outfpath))
            sys.exit(1)

    outfile_index = 0
    offset = 0
    i = 0
    delay = 15
    while True:
        i += 1
        offset_thresh = args.offset_thresh * i
        all_results, offset = make_repeated_queries_by_journal(args.jid, offset_start=offset, offset_thresh=offset_thresh)
        if len(all_results) == 0:
            break

        if args.offset_thresh != 0:
            outfile_index += 1
            if not args.out:
                outfname = 'papers-{}_{}.json'.format(args.jid, outfile_index)
            else:
                outfname = "{}.{}".format(args.out, outfile_index)
            outfpath = os.path.join(outdir, outfname)
        logger.debug('writing {} records to {}'.format(len(all_results), outfpath))
        with open(outfpath, 'w') as outf:
            json.dump(all_results, outf, cls=classes.AcademicEncoder, indent=4)

        if args.offset_thresh == 0:
            break

        if outfile_index >=5 and len(all_results) < 1000:
            logger.debug("At this point we've saved {} json files. The last one only had {} records, so we're giving up.".format(outfile_index, len(all_results)))
            break

        logger.debug('sleeping {} seconds'.format(delay))
        time.sleep(delay)

    #
    #


if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="get all papers for a journal")
    parser.add_argument("jid", type=int, help="Journal_ID to query")
    parser.add_argument("-o", "--out", help="output filename (json)")
    parser.add_argument("--outdir", default="paperscrape_journals/", help="directory for the output")
    parser.add_argument("--offset-thresh", type=int, default=0, help="every time the offset hits a multiple of this threshold, save the results to json. (zero means don't use threshold)")
    # parser.add_argument("--attributes", default='Id,Y,D', help="comma separated list of attributes to return")  # return these attributes: paper ID, Year, Date
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


