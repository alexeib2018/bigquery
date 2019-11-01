#!../.venv3/bin/python
# coding: utf-8

# # Countries report

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
    SELECT country,
           COUNT(*) AS install
    FROM (
          SELECT user_pseudo_id,
                 geo ->> 'country' AS country
          FROM events
          WHERE geo ->> 'country' <> ''
          GROUP BY user_pseudo_id, geo ->> 'country'
    ) AS installs_table
    GROUP BY country
    ORDER BY country
'''
cursor.execute(query)
install = pd.read_sql(query, con=conn)
cursor.close()
install.rename(columns={ 'country':'Country', 'install':'Install' }, inplace=True)
install.head(20)


# ### Calculate uninstalls

# In[4]:


cursor = conn.cursor()
query = '''
    SELECT country,
           uninstall
    FROM (  
        SELECT geo ->> 'country' AS country,
               COUNT(*) AS uninstall
        FROM events
        WHERE event_name='app_remove'
          AND geo ->> 'country' <> ''
        GROUP BY country
    ) uninstall
    ORDER BY country
'''
cursor.execute(query)
uninstall = pd.read_sql(query, con=conn)
cursor.close()
uninstall.rename(columns={ 'country':'Country', 'uninstall':'Uninstall' }, inplace=True)
uninstall.head(20)


# ### Calculate usage

# In[5]:


cursor = conn.cursor()
query = '''
    SELECT country,
           AVG(days_used) AS usage
    FROM (
      SELECT user_pseudo_id,
             country,
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
               geo ->> 'country' AS country
        FROM events
        WHERE geo ->> 'country' <> ''
        GROUP BY user_pseudo_idc, country
      ) AS user_country
      ON days_used.user_pseudo_id = user_country.user_pseudo_idc
    ) AS days_used_report
    GROUP BY country
    ORDER BY country
'''
cursor.execute(query)
usage = pd.read_sql(query, con=conn)
usage.rename(columns={ 'country':'Country', 'usage':'Usage' }, inplace=True)
usage['Usage'] = usage['Usage'].apply(lambda value: '%.2f' % value)
usage.head(20)


# ### Merge install, uninstall and average to result table

# In[6]:


result = pd.merge(install, uninstall, on='Country')
result['Net new install'] = result['Install'] - result['Uninstall']
result = pd.merge(result, usage, on='Country')
result.head(20)


# ### Output HTTP Header

# In[7]:


print('Content-type: text/csv')
print('Content-Disposition: attachment; filename="countries.csv"')
print()


# ### Output variables

# In[8]:


print('# Title: Countries report')


# ### Ouput result

# In[9]:


str = result.to_csv(index=False)
print(str.encode('ascii','xmlcharrefreplace').decode('utf-8'))


# ### Release resources

# In[10]:


conn.close()

