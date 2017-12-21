import os=`=jedi=0, =`=   (a, *_**p*_*) =`=jedi=`=
from glob i=`=jedi=0, mport glob=`= (path, *_**paths*_*) =`=jedi=`=
=`=jedi=0, =`=            (a, *_**p*_*) =`=jedi=`=
json_dir = =`=jedi=1, '/home/jporteno/mag_=`= (path, *_**paths*_*) =`=jedi=`='data_201710/'
fnames = glob(os.path.join(json_dir, '*.json'))
fnames.sort()

with open('mysql_load_multiple_papers_json_referencestableonly.sh', 'w') as outf:
    for fname in fnames:
        b = os.path.basename(fname)
        b = os.path.splitext(b)[0]
        line = "nohup python mysql_load_from_papers_json_fast.py {} --tablename PaperReferences --debug >& logs_restore_PaperReferences/restore_PaperReferences_{}.log".format(fname, b)
        outf.write(line + "\n")
