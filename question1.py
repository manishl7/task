#!/usr/bin/env python
# coding: utf-8

# In[525]:


#1. Scrape text data from some selected articles from above link. You can use beautiful soup, newspapers or any data scraping libraries.
# soup, newspapers or any data scraping libraries.

#using Selenium

#Here I have chosen Selenium as most of the sites nowadays are built on js(e-commerce site)to be more specific.Beautiful soup and scrappy aren't on par
# when it comes to scraping js data


from selenium import webdriver
driver=webdriver.Chrome('C:\Program Files (x86)\chromedriver.exe')
driver.get('https://english.onlinekhabar.com/')


# In[529]:


#Getting links of politics section for the first 5 page
#since datascraping is one of the important task of an nlp engineer, Instead of scraping just a article ,
#I have scraped articles from politics section
links=[]
count=20
while count !=0:
    count-=1
    common_class=driver.find_elements_by_class_name('ok-post-contents')
    for i in common_class:
        a=i.find_elements_by_tag_name('h2')[-1]
        b=a.find_element_by_tag_name('a')
        link=b.get_property('href')
        links.append(link)
    try:
        links=links[0:-22] #as various outlier datas were present in the last 22 links of every page
        driver.find_elements_by_class_name('page-numbers')[-1].click()  

    except:
        condition=False


# In[530]:


len(links)


# In[532]:


#getting data from all the links
from tqdm import tqdm
all_news_politics=[]
for i in tqdm(links):
    driver.get(i)
    headlines=driver.find_element_by_xpath('//*[@id="primary"]/div/div/div[1]/div[1]/h1').text[10:] #skipping the headings
    contents=driver.find_element_by_class_name("post-content-wrap").text[:-43]
    politics_news={'headlines':headlines,'contents':contents}
    all_news_politics.append(politics_news)
#Saving to dataframe
import pandas as pd
df=pd.DataFrame(all_news_politics)
df.to_csv('d:/Desktop/politics_onlinekhabar.csv')
    
    


# In[2]:


df=pd.read_csv('d:/Desktop/politics_onlinekhabar.csv')


# In[3]:


df.head()

