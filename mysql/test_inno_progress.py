
# coding: utf-8

# In[1]:


import sys, os, time
from timeit import default_timer as timer
from humanfriendly import format_timespan


# In[2]:


from dotenv import load_dotenv
load_dotenv('../.env')


# In[3]:


from db_connect_mag_201710 import get_db_connection


# In[4]:


db = get_db_connection()


# In[5]:


fname = '/home/jporteno/mag201710_PaperReferences_20171125.tsv'


# In[9]:

outfname = 'test_inno_progress.txt'
outf = open(outfname, 'w')
start = timer()
lineno_to_find = 799900000
print("starting to go through {}, looking for line number {}. outputting to {}".format(fname, lineno_to_find, outfname))
sys.stdout.flush()
with open(fname, 'r') as f:
    for i, line in enumerate(f):
        if i % 1e6 == 0:
            print("currently on line {} ({} so far)".format(i, format_timespan(timer()-start)))
            sys.stdout.flush()
        if i < lineno_to_find:
            continue
        elif i > lineno_to_find + 500000:
            break
        else:
            line = line.strip().split('\t')
            s = "SELECT * FROM PaperReferences_inno WHERE Paper_ID = {} AND Paper_reference_ID = {}".format(line[0], line[1])
            r = db.engine.execute(s)
            if r.fetchone():
                found_row = 1
            else:
                found_row = 0
            outf.write("{},{}".format(i, found_row))
            outf.write("\n")
print("total time: {}".format(format_timespan(timer()-start)))

outf.close()

