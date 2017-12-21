import os
from glob import glob

json_dir = '/home/jporteno/mag_data_201710/'
fnames = glob(os.path.join(json_dir, '*201*.json'))
fnames.sort()

with open('mysql_load_multiple_papers_json_paperauthorstableonly.sh', 'w') as outf:
    for fname in fnames:
        b = os.path.basename(fname)
        b = os.path.splitext(b)[0]
        line = "nohup python mysql_load_from_papers_json_fast.py {} --tablename PaperAuthorAffiliations --debug >& logs_PaperAuthorAffiliations_only/PaperAuthorAffilations_only_{}.log".format(fname, b)
        outf.write(line + "\n")

