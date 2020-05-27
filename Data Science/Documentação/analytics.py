#!/usr/bin/env python
# coding: utf-8

# In[79]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import psycopg2
from collections import Counter as ct
import datetime


# In[61]:

## Cria Conexão com o banco
con=psycopg2.connect("dbname=personal-project host=redshift-cluster-1.cnbwpnt3uapm.us-east-1.redshift.amazonaws.com port=5439 user=awsuser password=*******")


# In[31]:


# Vendors Ranking
cur = con.cursor()
cur.execute("select * from datasprints.vendors_ranking where vendor_id != 'TS' order by vendor_revenue desc;")
data = cur.fetchall()
print(type(data))
vendors_ranking = (pd.DataFrame(np.array(data)))
vendors_ranking = vendors_ranking.rename(columns={ 0:'revenue', 1:'vendor'})
vendors_ranking['revenue'] = (vendors_ranking['revenue']).astype(float)
print(type(vendors_ranking['revenue']))
print(vendors_ranking)

fig, ax = plt.subplots(figsize=(20,10))
x = vendors_ranking['vendor']
y = vendors_ranking['revenue']
plt.suptitle('Dinheiro arrecadado nos 4 anos')
plt.xlabel('Vendors')
plt.ylabel('Valor arreacado em dinheiro em milhões')
plt.bar(x,y, color='crimson')
plt.yticks()
plt.show()
plt.savefig('vendor_ranking.png', dpi=300, bbox_inches='tight')


# In[32]:


fig, ax = plt.subplots(figsize=(20,10))
x = vendors_ranking['vendor']
y = vendors_ranking['revenue']
ax.set_title('Dinheiro arrecadado nos 4 anos', fontsize=20)
plt.xlabel('Vendors', fontsize=15)
plt.ylabel('Valor arreacado em dinheiro em milhões', fontsize=15)
plt.bar(x,y, color='crimson')
plt.yticks()
plt.savefig('vendor_ranking.png', dpi=300, bbos_inche='tight')
plt.show()


# Distribuição de gorjeta
cur = con.cursor()
cur.execute("""
    select * from datasprints.amount_tips
    where pickup_date between '2012-10-01' and '2012-12-31'
    order by pickup_date
    """)
data = cur.fetchall()
print(type(data))
tips_amount = (pd.DataFrame(np.array(data)))
tips_amount = tips_amount.rename(columns={ 0:'qtd_gorjeta', 1:'date'})

fig, ax = plt.subplots(figsize=(25,10))
ax.set_title("Qtd de gorjetas por dia", fontsize=20)
x = tips_amount['date']
y = tips_amount['qtd_gorjeta']
plt.xlabel('Dias', fontsize=15)
plt.ylabel('Qquantidade de gorjetas', fontsize=15)
plt.savefig('gorjetas.png', dpi=300, bbos_inche='tight')
plt.plot(x,y, linewidth=2,color='red', marker='o' )
ax.set_xlim([datetime.date(2012, 10, 1), datetime.date(2012, 10, 31)])
plt.grid()


## Histograma 2009
cur = con.cursor()
cur.execute("""
    select
    (case when payment_lookup = 'Cash' then extract(month from pickup_date) end) as cash_month
from datasprints.travel_data
where year = '2009' and payment_lookup = 'Cash'
""")
data = cur.fetchall()
print(type(data))
trip_2009 = (pd.DataFrame(np.array(data)))
trip_2009 = trip_2009.rename(columns={ 0:'qtd_corridas'})
trip_2009['qtd_corridas'] = pd.to_numeric(trip_2009['qtd_corridas'])

print(trip_2009)

plt.savefig('hisograma_2009.png')
trip_2009.hist()


## ## Histograma 2010
cur = con.cursor()
cur.execute("""
    select
    (case when payment_lookup = 'Cash' then extract(month from pickup_date) end) as cash_month
from datasprints.travel_data
where year = '2010' and payment_lookup = 'Cash'
""")
data = cur.fetchall()
print(type(data))
trip_2010 = (pd.DataFrame(np.array(data)))
trip_2010 = trip_2010.rename(columns={ 0:'qtd_corridas'})
trip_2010['qtd_corridas'] = pd.to_numeric(trip_2010['qtd_corridas'])

print(trip_2010)

plt.savefig('hisograma_2010.png')
trip_2010.hist()



## ## Histograma 2011
cur = con.cursor()
cur.execute("""
    select
    (case when payment_lookup = 'Cash' then extract(month from pickup_date) end) as cash_month
from datasprints.travel_data
where year = '2011' and payment_lookup = 'Cash'
""")
data = cur.fetchall()
print(type(data))
trip_2011 = (pd.DataFrame(np.array(data)))
trip_2011 = trip_2011.rename(columns={ 0:'qtd_corridas'})
trip_2011['qtd_corridas'] = pd.to_numeric(trip_2011['qtd_corridas'])

print(trip_2011)

plt.savefig('hisograma_2011.png')
trip_2011.hist()

#### Histograma 2012
cur = con.cursor()
cur.execute("""
    select
    (case when payment_lookup = 'Cash' then extract(month from pickup_date) end) as cash_month
from datasprints.travel_data
where year = '2012' and payment_lookup = 'Cash'
""")
data = cur.fetchall()
print(type(data))
trip_2012 = (pd.DataFrame(np.array(data)))
trip_2012 = trip_2012.rename(columns={ 0:'qtd_corridas'})
trip_2012['qtd_corridas'] = pd.to_numeric(trip_2012['qtd_corridas'])
plt.savefig('hisograma_2012.png', dpi=300, bbos_inche='tight')
trip_2012.hist()
plt.show()