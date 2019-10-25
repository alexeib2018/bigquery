#!../.venv3/bin/python

import psycopg2
from datetime import datetime

conn = psycopg2.connect(host='localhost',
                        port='5432',
                        user='mffais',
                        password='pass',
                        database='bigquery')

cursor = conn.cursor()
insert = conn.cursor()

query = '''
    SELECT *
    FROM events_import_bigquery
    ORDER BY id
'''

cursor.execute(query)

colnames = {desc.name: desc.table_column-1 for desc in cursor.description}


def timestamp2datetime(ts):
    if ts is None:
        return 'null'
    seconds = int(int(ts) / 1000000)
    microseconds = int(ts) - seconds * 1000000
    result = datetime.utcfromtimestamp(seconds).strftime('%Y-%m-%dT%H:%M:%S') + '.%i' % microseconds
    return "'%s'" % result


for row in cursor:
    id                            = row[ colnames['id'] ]
    event_date                    = row[ colnames['event_date'] ]
    event_timestamp               = row[ colnames['event_timestamp'] ]
    event_name                    = row[ colnames['event_name'] ]
    event_params                  = 'null' # row[ colnames['event_params'] ]
    event_previous_timestamp      = 'null' # row[ colnames['event_previous_timestamp'] ]
    event_value_in_usd            = 'null' # row[ colnames['event_value_in_usd'] ]
    event_bundle_sequence_id      = 'null' # row[ colnames['event_bundle_sequence_id'] ]
    event_server_timestamp_offset = 'null' # row[ colnames['event_server_timestamp_offset'] ]
    user_id                       = 'null' # row[ colnames['user_id'] ]
    user_pseudo_id                = row[ colnames['user_pseudo_id'] ]
    user_properties               = 'null' # row[ colnames['user_properties'] ]
    user_first_touch_timestamp    = row[ colnames['user_first_touch_timestamp'] ]
    user_ltv                      = 'null' # row[ colnames['user_ltv'] ]
    device                        = 'null' # row[ colnames['device'] ]
    geo                           = 'null' # row[ colnames['geo'] ]
    app_info                      = 'null' # row[ colnames['app_info'] ]
    traffic_source                = 'null' # row[ colnames['traffic_source'] ]
    stream_id                     = 'null' # row[ colnames['stream_id'] ]
    platform                      = 'null' # row[ colnames['platform'] ]
    event_dimensions              = 'null' # row[ colnames['event_dimensions'] ]

    query  = "INSERT INTO events"
    query += "  (events_import_bigquery_fk, event_date, event_timestamp, event_name, event_params, event_previous_timestamp,"
    query += "   event_value_in_usd, event_bundle_sequence_id, event_server_timestamp_offset, user_id, user_pseudo_id,"
    query += "   user_properties, user_first_touch_timestamp, user_ltv, device, geo, app_info, traffic_source,"
    query += "   stream_id, platform, event_dimensions) "
    query += "VALUES ("
    query += "    %s ," % id
    query += "   '%s'," % event_date
    query += "    %s, " % timestamp2datetime( event_timestamp )
    query += "   '%s'," % event_name
    query += "   '%s'," % event_params
    query += "    %s, " % event_previous_timestamp
    query += "    %s, " % event_value_in_usd
    query += "    %s, " % event_bundle_sequence_id
    query += "    %s, " % event_server_timestamp_offset
    query += "    %s, " % user_id
    query += "   '%s'," % user_pseudo_id
    query += "    %s, " % user_properties
    query += "    %s, " % timestamp2datetime( user_first_touch_timestamp )
    query += "    %s, " % user_ltv
    query += "    %s, " % device
    query += "    %s, " % geo
    query += "    %s, " % app_info
    query += "    %s, " % traffic_source
    query += "    %s, " % stream_id
    query += "    %s, " % platform
    query += "    %s) " % event_dimensions

    insert.execute(query)

    if (id % 1000 == 0):
        print('Processed %s records' % id)

insert.execute('COMMIT')
insert.close()
cursor.close()
conn.close()
