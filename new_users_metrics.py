#!/usr/bin/env python
# coding: utf-8

# In[20]:


import pandas as pd
import matplotlib.pyplot as plt

#Get the number of new users per month and the accumulative number of users each month
df = pd.read_csv('/home/laura/venv/WikiChron/data/cocktails.csv', delimiter=';', quotechar='|', index_col='revision_id')
#df = pd.read_csv('/home/laura/venv/WikiChron/data/200movies.wikia.com.csv',header=0,delimiter=';',sep='\t', engine='python')
#transform the timestamp format into datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])
#Drop duplicated contributor_ids in order to have only different IDs (we don't need more, as we want NEW users)
df_no_duplicates= df.drop_duplicates('contributor_id')
#group by month and get the count for each contributor ID
new_users_per_month= df_no_duplicates.groupby(pd.Grouper(key='timestamp', freq='MS')).count()
#plot the number of new users each month
new_users_per_month.plot(y='contributor_id', title= 'New Users Per Month')
#get the accumulated value of new users each month
new_users_per_month['accum_users'] = new_users_per_month['contributor_id'].cumsum()
#plot it
new_users_per_month.plot(y='accum_users', title= 'New Users Per Month Accumulated')


# In[16]:


new_users_per_month


# In[13]:


df_no_duplicates


# In[ ]:




