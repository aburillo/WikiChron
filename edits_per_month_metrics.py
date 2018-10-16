#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
#import plotly as py
import matplotlib.pyplot as plt

df = pd.read_csv('/home/laura/venv/WikiChron/data/cocktails.csv', delimiter=';', quotechar='|', index_col='revision_id')
df['timestamp']=pd.to_datetime(df['timestamp'],format='%Y-%m-%dT%H:%M:%SZ') #poner en condiciones el forato de fech y hora
df.set_index(df['timestamp'], inplace=True) #el indice es timestamp
#df['timestamp'].hist(bins=50)
print(df.info())
monthly = df.groupby(pd.Grouper(key='timestamp',freq= 'MS'))
#print(monthly.head())
edites_pages_monthly = monthly.count()
#print(edites_pages_monthly.as_matrix()[:,0])
#print(edites_pages_monthly.types)
#print(edites_pages_monthly.head())
edites_pages_monthly.plot(y='page_id')
acumulado=edites_pages_monthly.page_id.cumsum()
aux2=pd.DataFrame(acumulado)
aux2.plot()


# In[ ]:




