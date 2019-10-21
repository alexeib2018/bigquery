import sys
import json
import psycopg2
from datetime import datetime
from google.cloud import bigquery

conn = psycopg2.connect(host='localhost',
                        port='5432',
                        user='mffais',
                        password='pass',
                        database='bigquery')

cursor = conn.cursor()


client = bigquery.Client()

query = "SELECT * FROM `track-money-bank-balance-lite.analytics_187332759.events_20191017` LIMIT 10" % sys.argv[1]

query_job = client.query(
    query,
    location="US",  # Location must match that of the dataset(s) referenced in the query.
)  # API request - starts the query


def convert(value, type):
    if value is None:
        return 'null'
    if type == 'number':
        return value
    if type == 'string':
        return "'%s'" % value
    if type == 'timestamp':
        seconds = int(value) / 1000000
        result = datetime.utcfromtimestamp(seconds).strftime('%Y-%m-%dT%H:%M:%S')
        return "'%s'" % result
    if type == 'json':
        sjson = json.dumps(value)
        sjson = sjson.replace("'", "''")
        return "'%s'" % sjson
    return "'%s'" % value


success = errors = 0
for row in query_job:  # API request - fetches results
    event_date           = convert( row['event_date'     ], 'string' )
    event_timestamp      = convert( row['event_timestamp'], 'number' )
    postgres_timestamp   = convert( row['event_timestamp'], 'timestamp' )
    postgres_timestamptz = convert( row['event_timestamp'], 'timestamp' )

    query  = 'INSERT INTO events_testtz'
    query += '  (event_date, event_timestamp, postgres_timestamp, postgres_timestamptz)'
    query += 'VALUES ('
    query += '  %s,' % event_date
    query += '  %s,' % event_timestamp
    query += '  %s,' % postgres_timestamp
    query += '  %s)' % postgres_timestamptz

    try:
        cursor.execute(query)
        success += 1
    except Exception as e:
        print(query)
        print(e)
        errors += 1
    cursor.execute("COMMIT")

cursor.execute("COMMIT")
cursor.close()
conn.close()

print('Script %s finished imported %s records (success:%i, errors:%i)\n' % (sys.argv[1], success+errors, success, errors))
