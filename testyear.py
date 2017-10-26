import sys, os, time, json
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

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__').getChild(__name__)

def reinquire(q_args, offset):
    new_payload = q_args['payload'].copy()
    new_payload['offset'] = offset
    new_payload['count'] = 1000
    new_args = {
                'query_type': q_args['query_type'],
                'payload': new_payload,
                'parent': q_args['parent']
            }
    q = inquirer.AcademicQuerier(new_args['query_type'], new_args['payload'])
    results = q.post()
    return new_args, results

def process_results(results, attributes):
    processed = []
    for r in results:
        p = {a: r[a] for a in attributes}
        processed.append(p)
    return processed

def main(args):
    all_results = []
    num_results = 0
    q_args = {
                'query_type': querytype.EVALUATE,
                'payload': {
                        'expr': 'Y={}'.format(args.year),
                        'attributes': args.attributes
                    },
                'parent': None
            }
    q = inquirer.AcademicQuerier(q_args['query_type'], q_args['payload'])
    results = q.post()
    processed = process_results(results, args.attributes.split(','))
    all_results.extend(processed)
    num_results += len(results)

    i = 0
    while True:
        q_args, results = reinquire(q_args, num_results)
        num_results += len(results)
        if not results:
            break
        processed = process_results(results, args.attributes.split(','))
        all_results.extend(processed)
        time.sleep(.0001)
        i += 1
        if i in [20, 50] or i % 100 == 0:
            logger.debug("{} queries completed. num_results: {}".format(i+1, num_results))

    outfname = 'testyear-{}.json'.format(args.year)
    outfpath = os.path.abspath(outfname)
    logger.debug('writing {} records to {}'.format(len(all_results), outfpath))
    with open(outfpath, 'w') as outf:
        json.dump(all_results, outf, cls=classes.AcademicEncoder, indent=4)



if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="get all papers in a year")
    parser.add_argument("year", type=int, default=1999, help="year to query")
    parser.add_argument("--attributes", default='Id,Y,D', help="comma separated list of attributes to return")  # return these attributes: paper ID, Year, Date
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
