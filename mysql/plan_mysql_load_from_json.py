
# coding: utf-8

# In[22]:


import os, json, time
from datetime import datetime
from timeit import default_timer as timer
from humanfriendly import format_timespan


# In[2]:


from dotenv import load_dotenv
dotenv_path = '../.env'
load_dotenv(dotenv_path)


# In[3]:


from db_connect_mag_201710 import get_db_connection


# In[4]:


db = get_db_connection()


# In[5]:


tbl = db.tables['Papers']


# In[6]:


# http://docs.sqlalchemy.org/en/latest/orm/extensions/automap.html
from sqlalchemy.ext.automap import automap_base
Base = automap_base()
Base.prepare(db.engine, reflect=True)


# In[7]:


Paper = Base.classes.Papers
PaperAuthorAffiliation = Base.classes.PaperAuthorAffiliations
PaperReference = Base.classes.PaperReferences


# In[8]:


pr = PaperReference()


# In[9]:


from sqlalchemy.orm import Session


# In[10]:


session = Session(db.engine)


# In[29]:


fname = '../paperscrape4/papers-1960.json'
with open(fname, 'r') as f:
    j = json.load(f)


# In[30]:


j[0]


# In[31]:


from collections import Counter
c = Counter()
for record in j:
    journal = record.get('C')
    if not journal:
        journal = []
    if len(journal) == 2:
        break
    c[len(journal)] += 1
c


# In[32]:


journal


# In[33]:


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


# In[34]:


for record in j:
    p, prs, paas = process_paper(record)
    if prs:
        break


# In[35]:


paas[0].Author_ID


# In[36]:


paas[0].Author_name


# In[37]:


record


# In[38]:


for pr in prs:
    print(pr.Paper_reference_ID)


# In[39]:


# try loading this one
session.add(p)
for pr in prs:
    session.add(pr)
for paa in paas:
    session.add(paa)
session.commit()


# In[11]:


fname = '../paperscrape4/papers-1960.json'


# In[12]:


json_load = Base.classes['paperjson_loaded']()


# In[13]:


json_load.json_fname = os.path.basename(fname)


# In[14]:


json_load.load_start = datetime.now()
time.sleep(2)
json_load.load_end = datetime.now()


# In[15]:


json_load.num_records = 55555
json_load.num_added = 55555


# In[16]:


session.add(json_load)
session.commit()


# In[17]:


Base.classes.has_key('paperjson_loaded')


# In[21]:


DBLog = Base.classes['paperjson_loaded']


# In[22]:


q = session.query(DBLog).filter(DBLog.json_fname=='papers-1960.json')
# q = q.exists()
q = q.scalar()


# In[23]:


q is not None


# In[24]:


session.close()


# In[9]:


# try sqlalchemy core to improve speed


# In[31]:


get_ipython().run_cell_magic('time', '', "json_fname = '/home/jporteno/mag_data_201710/papers-2016_1.json'\nwith open(json_fname) as f:\n    j = json.load(f)")


# In[12]:


def process_paper_core_method(record):
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
    conference = record.get('C')
    if conference:
        paper['Conference_series_ID'] = conference.get('CId')
    extended = record.get('E')
    if extended:
        paper['DOI'] = extended.get('DOI')
    
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


# In[32]:


p, prs, paas = process_paper_core_method(j[0])


# In[28]:


from sqlalchemy.exc import IntegrityError


# In[34]:


start = timer()
tbl = db.tables['Papers']
sq = tbl.insert(p)
try:
    db.engine.execute(sq)
except IntegrityError as e:
    print(e)
print("{:.5f} seconds".format(timer()-start))


# In[35]:


p['Paper_ID']


# In[37]:


start = timer()
tbl = db.tables['PaperReferences']
for pr in prs:
    sq = tbl.insert(pr)
    try:
        db.engine.execute(sq)
    except IntegrityError as e:
        print(e)
print("{:.5f} seconds".format(timer()-start))


# In[38]:


start = timer()
tbl = db.tables['PaperAuthorAffiliations']
for paa in paas:
    sq = tbl.insert(paa)
    try:
        db.engine.execute(sq)
    except IntegrityError as e:
        print(e)
print("{:.5f} seconds".format(timer()-start))


# In[62]:


tbl = db.tables['paperjson_loaded']
sq = tbl.select(tbl.c.json_fname=='papers-1960_1.json')
r = db.engine.execute(sq)


# In[63]:


x = r.rowcount


# In[64]:


x

