#!../.venv3/bin/python

import json
import psycopg2
from datetime import datetime


conn = psycopg2.connect(host='localhost',
                        port='5432',
                        user='mffais',
                        password='pass',
                        database='bigquery')

cursor = conn.cursor()

query = '''
	SELECT user_pseudo_id,
	       user_first_touch_timestamp,
	       geo->>'country' AS country
	FROM events_import_bigquery
	GROUP BY user_pseudo_id,
	         user_first_touch_timestamp,
	         geo->>'country'
	ORDER BY user_first_touch_timestamp
'''

cursor.execute(query)


def timestamp2datetime(ts):
    if ts is None:
        return None
    seconds = int(int(ts) / 1000000)
    result = datetime.utcfromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S')
    return result


result = []
for row in cursor:
    result.append({ 'user_pseudo_id': row[0],
                    'user_first_touch_timestamp': timestamp2datetime( row[1] ),
                    'country': row[2] })

cursor.close()
conn.close()

print("Content-type: application/json\n\n")
print(json.dumps(result))
