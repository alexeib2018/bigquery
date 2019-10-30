#!../.venv3/bin/python
# coding: utf-8

# # Campaigns report

# ### Imports

# In[1]:


import sys
import psycopg2
import pandas as pd


# ### Initial setup

# In[2]:


conn = psycopg2.connect(host='localhost',
                        port='5432',
                        user='mffais',
                        password='pass',
                        database='bigquery')


# ### Calculate installs

# In[3]:


cursor = conn.cursor()
query = '''
    SELECT campaign,
           COUNT(*) AS install
    FROM (
      SELECT user_pseudo_id,
             traffic_source ->> 'name' AS campaign
      FROM events
      GROUP BY user_pseudo_id, traffic_source ->> 'name'
    ) AS installs_table
    GROUP BY campaign
    ORDER BY campaign
'''
cursor.execute(query)
install = pd.read_sql(query, con=conn)
cursor.close()
install.rename(columns={ 'campaign':'Campaign', 'install':'Install' }, inplace=True)
install


# ### Calculate uninstalls

# In[4]:


cursor = conn.cursor()
query = '''
    SELECT traffic_source ->> 'name' AS campaign,
           COUNT(*) AS uninstall
    FROM events
    WHERE event_name='app_remove'
    GROUP BY campaign
'''
cursor.execute(query)
uninstall = pd.read_sql(query, con=conn)
cursor.close()
uninstall.rename(columns={ 'campaign':'Campaign', 'uninstall':'Uninstall' }, inplace=True)
uninstall


# ### Calculate usage

# In[5]:


cursor = conn.cursor()
query = '''
    SELECT campaign AS campaign,
           AVG(days_used) AS usage
    FROM (
      SELECT user_pseudo_id,
             campaign,
             days_used
      FROM (
        SELECT user_pseudo_id,
               COUNT(*) AS days_used
        FROM (
          SELECT event_date,
                 user_pseudo_id
          FROM events
          GROUP BY event_date, user_pseudo_id
        ) AS users_by_day
        GROUP BY user_pseudo_id
      ) AS days_used
      INNER JOIN (
        SELECT user_pseudo_id AS user_pseudo_idc,
               traffic_source ->> 'name' AS campaign
        FROM events
        GROUP BY user_pseudo_idc, campaign
      ) AS user_campaign
      ON days_used.user_pseudo_id = user_campaign.user_pseudo_idc
      ORDER BY days_used DESC
    ) AS days_used_report
    GROUP BY campaign
    ORDER BY campaign
'''
cursor.execute(query)
usage = pd.read_sql(query, con=conn)
usage.rename(columns={ 'campaign':'Campaign', 'usage':'Usage' }, inplace=True)
usage['Usage'] = usage['Usage'].apply(lambda value: '%.2f' % value)
usage


# ### Merge install, uninstall and average to result table

# In[6]:


result = pd.merge(install, uninstall, on='Campaign')
result['Net new install'] = result['Install'] - result['Uninstall']
result = pd.merge(result, usage, on='Campaign')
result


# ### Output HTTP Header

# In[7]:


print('Content-type: text/csv')
print('Content-Disposition: attachment; filename="campaigns.csv"')
print()


# ### Output variables

# In[8]:


print('# Title: Campaign report')


# ### Ouput result

# In[9]:


result.to_csv(sys.stdout, index=False)


# ### Release resources

# In[10]:


conn.close()

