import os
from glob import glob

json_dir = '/home/jporteno/mag_data_201710/'
fnames = glob(os.path.join(json_dir, '*.json'))
fnames.sort()

with open('mysql_load_multiple_papers_json_paperstableonly.sh', 'w') as outf:
    for fname in fnames:
        b = os.path.basename(fname)
        b = os.path.splitext(b)[0]
        line = "nohup python mysql_load_from_papers_json_fast.py {} --debug >& mysql_load_{}.log".format(fname, b)
        line = "nohup python mysql_load_only_papers_table_from_json.py {} --debug >& logs_papers_table_only/mysql_load_{}.log".format(fname, b)
        outf.write(line + "\n")
