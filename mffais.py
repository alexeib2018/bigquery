#!.venv3/bin/python

import os
import sys
import json
import psycopg2
from datetime import datetime
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'mffais-test-7c795d6e409e.json'

conn = psycopg2.connect(host='localhost',
                        port='5432',
                        user='mffais',
                        password='pass',
                        database='bigquery')

cursor = conn.cursor()


client = bigquery.Client()

query = "SELECT * FROM `track-money-bank-balance-lite.analytics_187332759.events_%s`" % sys.argv[1]

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

    query  = 'INSERT INTO events'
    query += '  (event_date, event_timestamp, event_name, event_params, event_previous_timestamp, event_value_in_usd,'
    query += '   event_bundle_sequence_id, event_server_timestamp_offset, user_id, user_pseudo_id, user_properties,'
    query += '   user_first_touch_timestamp, user_ltv, device, geo, app_info, traffic_source, stream_id, platform, event_dimensions)'
    query += 'VALUES ('
    query += '  %s,' % event_date
    query += '  %s,' % event_timestamp
    query += '  %s,' % event_name
    query += '  %s,' % event_params
    query += '  %s,' % event_previous_timestamp
    query += '  %s,' % event_value_in_usd
    query += '  %s,' % event_bundle_sequence_id
    query += '  %s,' % event_server_timestamp_offset
    query += '  %s,' % user_id
    query += '  %s,' % user_pseudo_id
    query += '  %s,' % user_properties
    query += '  %s,' % user_first_touch_timestamp
    query += '  %s,' % user_ltv
    query += '  %s,' % device
    query += '  %s,' % geo
    query += '  %s,' % app_info
    query += '  %s,' % traffic_source
    query += '  %s,' % stream_id
    query += '  %s,' % platform
    query += '  %s)' % event_dimensions

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
