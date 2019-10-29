#!../.venv3/bin/python

import os
import sys
import json
import psycopg2
from datetime import datetime
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../bigquery_key.json'
import_date = sys.argv[1]

conn = psycopg2.connect(host='localhost',
                        port='5432',
                        user='mffais',
                        password='pass',
                        database='bigquery')

cursor = conn.cursor()


client = bigquery.Client()

query = "SELECT * FROM `track-money-bank-balance-lite.analytics_187332759.events_%s`" % import_date

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


recno = 0
success = duplicates = errors = 0
try:
    for row in query_job:  # API request - fetches results
        event_date                    = convert( row['event_date'                   ], 'string' )
        event_timestamp               = convert( row['event_timestamp'              ], 'number' )
        event_name                    = convert( row['event_name'                   ], 'string' )
        event_params                  = convert( row['event_params'                 ], 'json'   )
        event_previous_timestamp      = convert( row['event_previous_timestamp'     ], 'number' )
        event_value_in_usd            = convert( row['event_value_in_usd'           ], 'number' )
        event_bundle_sequence_id      = convert( row['event_bundle_sequence_id'     ], 'number' )
        event_server_timestamp_offset = convert( row['event_server_timestamp_offset'], 'number' )
        user_id                       = convert( row['user_id'                      ], 'string' )
        user_pseudo_id                = convert( row['user_pseudo_id'               ], 'string' )
        user_properties               = convert( row['user_properties'              ], 'json'   )
        user_first_touch_timestamp    = convert( row['user_first_touch_timestamp'   ], 'number' )
        user_ltv                      = convert( row['user_ltv'                     ], 'string' )
        device                        = convert( row['device'                       ], 'json'   )
        geo                           = convert( row['geo'                          ], 'json'   )
        app_info                      = convert( row['app_info'                     ], 'json'   )
        traffic_source                = convert( row['traffic_source'               ], 'json'   )
        stream_id                     = convert( row['stream_id'                    ], 'string' )
        platform                      = convert( row['platform'                     ], 'string' )
        event_dimensions              = convert( row['event_dimensions'             ], 'string' )

        query_duplicates  = 'SELECT FROM events_import_bigquery'
        query_duplicates += ' WHERE'
        query_duplicates += "   event_name=%s AND" % event_name
        query_duplicates += "   event_timestamp=%s AND" % event_timestamp
        query_duplicates += "   user_pseudo_id=%s" % user_pseudo_id
        cursor_duplicates = conn.cursor()
        cursor_duplicates.execute(query_duplicates)
        record_duplicates = cursor_duplicates.rowcount
        cursor_duplicates.close()
        if (record_duplicates):
            duplicates += 1
            continue

        query  = 'INSERT INTO events_import_bigquery'
        query += '  (event_date, event_timestamp, event_name, event_params, event_previous_timestamp, event_value_in_usd,'
        query += '   event_bundle_sequence_id, event_server_timestamp_offset, user_id, user_pseudo_id, user_properties,'
        query += '   user_first_touch_timestamp, user_ltv, device, geo, app_info, traffic_source, stream_id, platform, event_dimensions)'
        query += ' VALUES ('
        query += '   %s,' % event_date
        query += '   %s,' % event_timestamp
        query += '   %s,' % event_name
        query += '   %s,' % event_params
        query += '   %s,' % event_previous_timestamp
        query += '   %s,' % event_value_in_usd
        query += '   %s,' % event_bundle_sequence_id
        query += '   %s,' % event_server_timestamp_offset
        query += '   %s,' % user_id
        query += '   %s,' % user_pseudo_id
        query += '   %s,' % user_properties
        query += '   %s,' % user_first_touch_timestamp
        query += '   %s,' % user_ltv
        query += '   %s,' % device
        query += '   %s,' % geo
        query += '   %s,' % app_info
        query += '   %s,' % traffic_source
        query += '   %s,' % stream_id
        query += '   %s,' % platform
        query += '   %s)' % event_dimensions
        # query += ' ON CONFLICT DO NOTHING'

        try:
            cursor.execute(query)
            success += 1
        except Exception as e:
            # print(query)
            print(e)
            errors += 1
        # cursor.execute("COMMIT")

        recno += 1
        if (recno % 1000 == 0):
            print('Imported %s records' % recno)
    print('Importted %s records' % recno)
except Exception:
    print('Table %s was not found' % import_date)


cursor.execute("COMMIT")
cursor.close()
conn.close()

print('Script %s finished, imported %s records (success:%i, duplicates:%i, errors:%i)\n' % (import_date, success+duplicates+errors, success, duplicates, errors))
