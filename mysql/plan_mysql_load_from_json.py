
# coding: utf-8

# In[19]:


import os, json


# In[20]:


from dotenv import load_dotenv
dotenv_path = '../.env'
load_dotenv(dotenv_path)


# In[21]:


from db_connect_mag_201710 import get_db_connection


# In[22]:


db = get_db_connection()


# In[23]:


tbl = db.tables['Papers']


# In[24]:


# http://docs.sqlalchemy.org/en/latest/orm/extensions/automap.html
from sqlalchemy.ext.automap import automap_base
Base = automap_base()
Base.prepare(db.engine, reflect=True)


# In[25]:


Paper = Base.classes.Papers
PaperAuthorAffiliation = Base.classes.PaperAuthorAffiliations
PaperReference = Base.classes.PaperReferences


# In[26]:


pr = PaperReference()


# In[27]:


from sqlalchemy.orm import Session


# In[28]:


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

