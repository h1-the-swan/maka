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

def process_entities(entities):
    processed = []
    for e in entities:
        # JSON encode extended metadata
        e['E'] = json.loads(e['E'])

        p = {
            'Id': e['Id'],
            'Ti': e['Ti'],
            'L': e.get('L'),
            'Y': e['Y'],
            'D': e.get('D'),
            'RId': e.get('RId'),
            'AA': [],
            'F': [],
            'J': e.get('J'),
            'C': e.get('C'),
            'E': {'DOI': e['E'].get('DOI')}
        }
        authors = e.get('AA', [])
        for a in authors:
            p['AA'].append({
                    'AuId': a['AuId'],
                    'AuN': a.get('AuN'),
                    'AfId': a.get('AfId')
            })
        fields = e.get('F', [])
        for f in fields:
            p['F'].append({'FId': f['FId']})
        processed.append(p)
    return processed

def get_querier(expr):
    q_args = {
        'query_type': querytype.EVALUATE,
        'payload': {
            'expr': expr,
            'attributes': '*'
        },
        'parent': None
    }
    q = inquirer.AcademicQuerier(q_args['query_type'], q_args['payload'])
    return q

def generic_evaluate_query_from_querier(querier, return_json=True):
    url = querier.query.get_url()
    headers = {'Ocp-Apim-Subscription-Key': os.environ['MAKA_SUBSCRIPTION_KEY']}
    data = querier.query.get_body()
    request = requests.post(url, data=data, headers=headers)
    if request.status_code >= 300:
        raise classes.Error('An error ocurred while processing the request. Code: {}'.format(request.status_code))
    j = request.json()
    if j.get('aborted'):
        raise classes.Error('Request timeout')
    if return_json:
        return j
    else:
        return request

def generic_evaluate_query(expr):
    querier = get_querier(expr)
    return generic_evaluate_query_from_querier(querier)

def generic_id_query(id):
    expr = "Id={}".format(id)
    return generic_evaluate_query(expr)

def check_type(entities):
    for entity in entities:
        if entity.get('Ty') not in ['0', 0]:
            logger.warn("entity Id={} is not paper (Ty 0)".format(entity.get('Id')))

def main(args):
    outdir = os.path.abspath(args.outdir)
    if not args.out:
        outfname = 'papers-{}.json'.format(args.year)
    else:
        outfname = args.out
    outfpath = os.path.join(outdir, outfname)
    if os.path.exists(outfpath):
        logger.error("file {} exists. exiting.".format(outfpath))
        sys.exit(1)


    all_results = []
    # num_results = 0
    querier = get_querier("Y={}".format(args.year))
    logger.debug('making first query with args: {}'.format(querier.query.get_body()))
    j = generic_evaluate_query_from_querier(querier)
    logger.debug('query done')
    entities = j['entities']
    check_type(entities)
    processed = process_entities(entities)
    all_results.extend(processed)
    # processed = process_results(results, args.attributes.split(','))
    # all_results.extend(processed)
    # num_results += len(results)

    i = 0
    querier.query.count = 1000
    while True:
        querier.query.offset = len(all_results)
        logger.debug('making query {} with args: {}'.format(i+1, querier.query.get_body()))
        j = generic_evaluate_query_from_querier(querier)
        entities = j['entities']
        if not entities:
            break
        check_type(entities)
        processed = process_entities(entities)
        all_results.extend(processed)
        # processed = process_results(results, args.attributes.split(','))
        time.sleep(.01)
        i += 1
        if i in [20, 50] or i % 100 == 0:
            logger.debug("{} queries completed. num_results: {}".format(i+1, len(all_results)))


    #
    logger.debug('writing {} records to {}'.format(len(all_results), outfpath))
    with open(outfpath, 'w') as outf:
        json.dump(all_results, outf, cls=classes.AcademicEncoder, indent=4)
    #


if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="get all papers in a year")
    parser.add_argument("year", type=int, default=1999, help="year to query")
    parser.add_argument("-o", "--out", help="output filename (json)")
    parser.add_argument("--outdir", default="paperscrape/", help="directory for the output")
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

