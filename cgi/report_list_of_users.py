#!../.venv3/bin/python
# coding: utf-8

# # List of users report

# ### Imports

# In[1]:


import sys
import psycopg2
import pandas as pd
from datetime import datetime


# ### Initial setup

# In[2]:


conn = psycopg2.connect(host='localhost',
                        port='5432',
                        user='mffais',
                        password='pass',
                        database='bigquery')


# ### Fetch users

# In[3]:


cursor = conn.cursor()
query = '''
    SELECT user_pseudo_id AS user_id,
           geo ->> 'country' AS country,
           traffic_source ->> 'source' AS source,
           traffic_source ->> 'name' AS campaign,
           DATE(user_first_touch_timestamp) AS install_date
    FROM events
    WHERE geo ->> 'country' <> ''
      AND user_first_touch_timestamp IS NOT NULL
    GROUP BY user_pseudo_id, country, source, campaign, user_first_touch_timestamp
    ORDER BY country
'''
cursor.execute(query)
list_of_users = pd.read_sql(query, con=conn)
cursor.close()
list_of_users.rename(columns={ 'user_id':'User ID',
                               'country':'Country',
                                'source':'Source',
                              'campaign':'Campaign',
                          'install_date':'Install date'}, inplace=True)
list_of_users.head(20)


# ### Calculate uninstalls

# In[4]:


cursor = conn.cursor()
query = '''
    SELECT user_pseudo_id AS user_id,
           DATE(event_timestamp) AS uninstall_date
    FROM events
    WHERE event_name='app_remove'
'''
cursor.execute(query)
uninstall = pd.read_sql(query, con=conn)
cursor.close()
uninstall.rename(columns={ 'user_id':'User ID', 'uninstall_date':'Uninstall date' }, inplace=True)
uninstall.head(20)


# ### Calculate days installed

# In[5]:


list_of_users = pd.merge(list_of_users, uninstall, on='User ID', how='left')
list_of_users.head(20)


# In[6]:


today = datetime.date( datetime.now() )

def cleanNaN(row):
    uninstall_date = row['Uninstall date']
    if str( uninstall_date ) == 'nan':
        return ''
    return uninstall_date

def date_diff(row):
    install_date = row['Install date']
    if str( row['Uninstall date'] ) == '':
        used_date = today
    else:
        used_date = row['Uninstall date']
    days_installed = int( int( ( used_date - install_date ).total_seconds() ) / 24 / 60 / 60 + 1 )
    return days_installed

list_of_users['Uninstall date'] = list_of_users.apply(cleanNaN, axis=1)
list_of_users['Days installed'] = list_of_users.apply(date_diff, axis=1)

list_of_users.head(20)


# ### Calculate sessions since installed

# In[7]:


cursor = conn.cursor()
query = '''
    SELECT user_pseudo_id AS user_id,
           COUNT(*) AS sessions
    FROM (
      SELECT event_date,
             user_pseudo_id
      FROM events
      GROUP BY event_date, user_pseudo_id
    ) AS users_by_day
    GROUP BY user_id
'''
cursor.execute(query)
sessions = pd.read_sql(query, con=conn)
sessions.rename(columns={ 'user_id':'User ID', 'sessions':'Sessions' }, inplace=True)
sessions.head(20)


# In[8]:


list_of_users = pd.merge(list_of_users, sessions, on='User ID', how='left')
list_of_users.head(20)


# ### Output HTTP Header

# In[9]:


print('Content-type: text/csv')
print('Content-Disposition: attachment; filename="list_of_users.csv"')
print()


# ### Output variables

# In[10]:


print('# Title: List of users report')


# ### Ouput result

# In[11]:


str = list_of_users.to_csv(index=False)
print(str.encode('ascii','xmlcharrefreplace').decode('utf-8'))


# ### Release resources

# In[12]:


conn.close()

