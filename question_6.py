#!/usr/bin/env python
# coding: utf-8

# In[167]:


# 6. Get an answer to the given question. Question sentence should be in natural language,
# follow the necessary steps to get the answer from the graph db.


# In[ ]:


#USING A DIFFERENT LIBRARY
import nltk


# In[3]:


#Downloading the packages
nltk.download()


# In[4]:


#Here I am using neo4j instead of networkx and it's query language 'cypher'

# 1>Here to answer the question between Subject and object , I have choosen [NEEDS] as the relationship as per the output from previous question
# 2>For a specific query say : who/what needs balance?----answer--->Party

#These are cypher queries 
# Code: MATCH(a:Subject)-[NEEDS->(b:Object) where a.ent='balance' RETURN a,b
# Ans : It return a directed graph that shows Subject 'party' is the one that 'needs' object i.e'balance'
# Code: MATCH(a:Subject)-[NEEDS]->(b:Object{ent:'balance'}) RETURN a,b #Here query can also be generated using specific attributes


# In[6]:


import pandas as pd
data_qa1={'sentence':["other olis side needs 15 days","Party needs election","who needs adulteration","It needs intellecutal warriors","Party needs balance","What does oli side needs?","What does party needs?","What does who needs?","what does It needs?","what does Party needs?"],
      'answer':['','','','','','15 days','election','adulteration','intellectual warriors','balance']}
data_qa1=pd.DataFrame(data_qa1)


# In[7]:


data_qa1


# In[8]:


#Tagging each sentence as a statement of question(q)
from nltk import tokenize
from nltk import word_tokenize
#tagging each sentence with S and Q
tag_sentence=lambda row: 'S' if row.answer=='' else 'Q'
data_qa1['type']=data_qa1.apply(tag_sentence,axis=1)

#using NLTK to tokenize the sentence into array o words
tokenize=lambda row: nltk.word_tokenize(row.sentence)
data_qa1.sentence=data_qa1.apply(tokenize,axis=1)


# In[9]:


data_qa1


# In[10]:


#creating a df with just the statements
def statements(df):
  return df[df.type=='S']   .reset_index(drop=True)   .drop('answer',axis=1)   .drop('type',axis=1)

#creating a df with just the question
def questions(df):
  return df[df.type=='Q']   .reset_index(drop=True)   .drop('type',axis=1)


# In[11]:


statements(data_qa1)[:4]


# In[12]:


questions(data_qa1)[:2]


# In[15]:


# Tagging each token as a pos 
pos_tag=lambda row: nltk.pos_tag(row.sentence)
data_qa1['tag']=data_qa1.apply(pos_tag,axis=1)


# In[16]:


data_qa1[['sentence','tag']][:5]


# In[17]:


#extracting the tiplets now

def extract_statement(tags):
  #It'll extract (subject,relation,obect) triple from each statement based on the pos tags
  subject,relation,obj='','',''
  for word ,tag in tags:
    if tag == 'NNP':
      subject=word
    elif tag=='VBD' or word=='needs':
      relation= word
    if tag=='NNP' or tag=='NN':
      obj=word
  return(subject,relation,obj)


# In[18]:


#Extracting question as well

def extract_question(tags):  #extracts the entities under discussion from each question based on the POS tags
  eud=""
  for word,tag in tags:
    if tag=='NNP' or tag=='NN':
      eud=word
  return eud


# In[19]:


#Now we'll call the function to extract the correct info

def extract(row):
  #It'll extract the appropriate data given in a processed DataFrame row
  if row.type=='S':
    return extract_statement(row.tag)
  else:
    return extract_question(row.tag)


# In[20]:


data_qa1['extracted']=data_qa1.apply(extract,axis=1)


# In[21]:


#to check if our extraction is completed
data_qa1[['sentence','extracted']]


# In[127]:


#defining our debugging function
#this will find out what a specific subject needs

def subject_statements(subject):
  #gets all the satements that refers to the specific subject/person
  stat=statements(data_qa1)
  return stat[stat.extracted.map(lambda t: t[0]==subject)]
subject_statements('Party')


# In[128]:


#Advanced version of the above function

def subject_statements_recent(subject,n=5):
  #It'll get the most recent answer in our case party needs election and also balance so it'll give the most recent update
  
  return subject_statements(subject)[-n:].iloc[::-1]


# In[129]:


subject_statements_recent('Party',n=1)


# In[26]:


# pip install neo4j


# In[27]:


from neo4j import GraphDatabase, basic_auth 


# In[ ]:


driver = GraphDatabase.driver('bolt://localhost:7687', auth=basic_auth('neo4j', 'Josemourinho11?'))


# In[ ]:





# In[66]:


def reset_db():
  #to remove previous db
    session=driver.session()
    session.run("MATCH (n) DETACH DELETE(n)")
    


# In[130]:


def create(query,n=0):
  #This will create a graph if we are given a query; based on each triplet in the extracted statements
    session=driver.session()
    stat=statements(data_qa1)
    n=len(stat) if n<=0 else n
    for subject,relation,obj in stat[:n].extracted:
        session.run(query, subject=subject, relation=relation, obj=obj)


# In[91]:


#Creating direct relationships
reset_db


# In[131]:


#creating a direct realtionship between subject and object
v1_query='''
    MERGE (s:SUBJECT {name: $subject})
    MERGE (o:OBJECT {name: $obj})
    MERGE (s)-[r:RELATION {name: $relation}]->(o)
    '''

create(v1_query)


# In[100]:


#Nodes for relationship
reset_db()
#representing each relation as a node
v2_query='''
    MERGE (s:SUBJECT {name: $subject})
    MERGE (o:OBJECT {name: $obj})
    MERGE (s)-[:R0]->(r:RELATION {name: $relation})-[:R1]->(o)
'''
create(v2_query)


# In[143]:


v3_query='''
MERGE (s:SUBJECT {name: $subject})
MERGE (o:OBJECT {name: $obj})

WITH s,o

//Creating a new relation betn subject and object
CREATE (s)-[:R0]->(r:RELATION {name: $relation})-[:R1]->(o)
CREATE (s)-[h:HEAD]->(r) //here we are updating to make sure that the newly created relation is the head of the list

WITH s,r,o,h

//Here, We are finding the prrevious head of the list (if none exists our query will end here)
MATCH (s)-[h_prev:HEAD]->(r_prev:RELATION)
WHERE h_prev <> h

//Completing the link and removing the previous head pointer

CREATE (r_prev)-[:NEXT]->(r)
DELETE h_prev
'''


# In[144]:


create(v3_query)


# In[156]:


session=driver.session()
session.run('CREATE INDEX on :SUBJECT(name)')
session.run('CREATE INDEX on :RELATION(name)')
session.run('CREATE INDEX ON :OBJECT(name)')
create(v3_query)


# In[146]:


#QUERYING FROM THE GRAPH
def find_subject(subject):
    '''To find our answers'''
    query='''
        MATCH (s:SUBJECT {name:$name})-[:HEAD]->(r:RELATION)--(o:OBJECT)
        RETURN s AS subject,r AS relation, o AS obj
    
    '''
    return session.run(query, name=subject)


# In[164]:


a=find_subject('Party?')


# In[165]:

#Getting our answers
session=driver.session()
record=find_subject('Party').single()
print(record['obj'].get('name'))

#Here 'Party' 'needs' 'balance' # hence it'll give the answer balance
#since I was having issues during indexing I couldn't print out the FULL answer,I could only print out the object, The issue seems unique to 4.x but not in 3.x.
# I even joined the neo4j community but couldn't find proper answers to the same.

