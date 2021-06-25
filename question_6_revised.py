#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# 6. Get an answer to the given question. Question sentence should be in natural language,
# follow the necessary steps to get the answer from the graph db.

#Here I am using neo4j instead of networkx to show my familiarity with another method 

# 1>Here to answer the question between Subject and object , I have choosen [NEEDS] as the relationship as per the output from previous question
# 2>For a specific query say : Party?----answer--->Party needs balance

#These are our basic cypher queries but since we'll be linking our notebook to neo4j server;we'll run a diffrent query
# Code: MATCH(a:Subject)-[NEEDS->(b:Object) where a.ent='balance' RETURN a,b
# Ans : It return a directed graph that shows Subject 'party' is the one that 'needs' object i.e'balance'
# Code: MATCH(a:Subject)-[NEEDS]->(b:Object{ent:'balance'}) RETURN a,b #Here query can also be generated using specific attributes


# In[104]:


#USING NLTK this time and following up to the answers from question 5
import nltk
import pandas as pd
import pandas as pd
data_qa1={'sentence':["other olis side needs 15 days","Party needs election","who needs adulteration","It needs intellecutal warriors","Party needs balance","What does oli side needs?","What does party needs?","What does who needs?","what does It needs?","what does Party needs?"],
      'answer':['','','','','','15 days','election','adulteration','intellectual warriors','balance']}
data_qa1=pd.DataFrame(data_qa1)
data_qa1


# In[105]:



#Tagging each sentence as a statement of question(q)
from nltk import tokenize
from nltk import word_tokenize
#tagging each sentence with S and Q
tag_sentence=lambda row: 'S' if row.answer=='' else 'Q'
data_qa1['type']=data_qa1.apply(tag_sentence,axis=1)

#using NLTK to tokenize the sentence into array o words
tokenize=lambda row: nltk.word_tokenize(row.sentence)
data_qa1.sentence=data_qa1.apply(tokenize,axis=1)


# In[106]:


data_qa1


# In[107]:


#creating a df with just the statements
def statements(df):
  return df[df.type=='S']   .reset_index(drop=True)   .drop('answer',axis=1)   .drop('type',axis=1)

#creating a df with just the question
def questions(df):
  return df[df.type=='Q']   .reset_index(drop=True)   .drop('type',axis=1)


# In[108]:


statements(data_qa1)[:4]


# In[109]:


questions(data_qa1)[:2]


# In[110]:


# Tagging each token as a pos 
pos_tag=lambda row: nltk.pos_tag(row.sentence)
data_qa1['tag']=data_qa1.apply(pos_tag,axis=1)


# In[111]:


data_qa1[['sentence','tag']][:5]


# In[112]:


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


# In[113]:


#Extracting question as well

def extract_question(tags):  #extracts the entities under discussion from each question based on the POS tags
  eud=""
  for word,tag in tags:
    if tag=='NNP' or tag=='NN':
      eud=word
  return eud


# In[114]:


#Now we'll call the function to extract the correct info

def extract(row):
  #It'll extract the appropriate data given in a processed DataFrame row
  if row.type=='S':
    return extract_statement(row.tag)
  else:
    return extract_question(row.tag)


# In[115]:


data_qa1['extracted']=data_qa1.apply(extract,axis=1)


# In[117]:


#to check if our extraction is completed
data_qa1[['sentence','extracted']]


# In[121]:


#defining our debugging function
#this will find out what a specific subject needs

def subject_statements(subject):
  #gets all the satements that refers to the specific subject/person
  stat=statements(data_qa1)
  return stat[stat.extracted.map(lambda t: t[0]==subject)]
subject_statements('Party')


# In[21]:


from neo4j import GraphDatabase, basic_auth 


# In[22]:


driver = GraphDatabase.driver('bolt://localhost:7687', auth=basic_auth('neo4j', 'Josemourinho11?'))


# In[23]:


def reset_db():
  #to remove previous db
    session=driver.session()
    session.run("MATCH (n) DETACH DELETE(n)")
    


# In[24]:


reset_db()


# In[28]:


def create(query,n=0):
    '''Given a query , This will create a triplet of each statement'''
    session=driver.session()
    stat=statements(data_qa1)
    n=len(stat) if n<=0 else n
    for subject,relation,obj in stat[:n].extracted:
        session.run(query,subject=subject,relation=relation,obj=obj)


# In[29]:


#creating a direct realtionship between subject and object

#Here I have created queries using 3 different methods ; each ascending interms off complexity. I have chosen v3_query as my final method

v1_query='''
    MERGE (s:SUBJECT {name: $subject})
    MERGE (o:OBJECT {name: $obj})
    MERGE (s)-[r:RELATION {name: $relation}]->(o)
    '''

create(v1_query)


# In[30]:


#Nodes for relationship

reset_db()
#representing each relation as a node
v2_query='''
    MERGE (s:SUBJECT {name: $subject})
    MERGE (o:OBJECT {name: $obj})
    MERGE (s)-[:R0]->(r:RELATION {name: $relation})-[:R1]->(o)
'''
create(v2_query)


# In[31]:


#Our selected query method
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


# In[33]:


session=driver.session()
session.run('CREATE INDEX on :SUBJECT(name)')
session.run('CREATE INDEX on :RELATION(name)')
session.run('CREATE INDEX ON :OBJECT(name)')
create(v3_query)


# In[283]:


#QUERYING FROM THE GRAPH
def find_subject(subject):
    '''To find our answers'''
    query='MATCH (s:SUBJECT {name:$name})-[:HEAD]->(r:RELATION)--(o:OBJECT)RETURN s AS sub,r as rel,o As obj'  
   
    return session.run(query, name=subject)
session=driver.session()
record=find_subject('Party').single()
print(record['sub'].get('name'),record['rel'].get('name'),record['obj'].get('name'))

#Output:Party needs balance


# In[315]:


def answer_question(question):
    question=nltk.word_tokenize(question)
    question=nltk.pos_tag(question)
    for word,tag in question:
        if tag=='NNP' or tag=='NN':
            question=word
            return(question)
question=answer_question('What does the Party needs?')    
session=driver.session()
record=find_subject(question).single()
print(record['sub'].get('name'),record['rel'].get('name'),record['obj'].get('name'))

#Output : Party needs balance


# In[335]:


def answer_question(question):
    question=get_question(question)
    session=driver.session()
    record=find_subject(question).single()
    print(record['sub'].get('name'),record['rel'].get('name'),record['obj'].get('name'))

    
question=answer_question('What does the Party needs?')
#Output : Party needs balance


# In[327]:


#Function to build graph
def build_graph_v3(start=0, end=0):
    reset_db()
    session=driver.session()
#     session.run('SUBJECT(name)')
#     session.run('Relation(name)')
#     session.run('OBJECT(name)')
    create(v3_query)
build_graph_v3(start=2,end=2)
    

