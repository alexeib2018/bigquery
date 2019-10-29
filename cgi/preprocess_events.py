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
insert = conn.cursor()

query = '''
    SELECT *
    FROM events_import_bigquery
    ORDER BY id
'''

cursor.execute(query)

colnames = {desc.name: desc.table_column-1 for desc in cursor.description}


def timestamp2datetime(value):
    if value is None:
        return 'null'
    seconds = int(int(value) / 1000000)
    microseconds = int(value) - seconds * 1000000
    result = datetime.utcfromtimestamp(seconds).strftime('%Y-%m-%dT%H:%M:%S') + '.%i' % microseconds
    return "'%s'" % result


def timestamp2string(value):
    if value is None:
        return None
    seconds = int(int(value) / 1000)
    microseconds = int(value) - seconds * 1000
    if microseconds:
        sfract = str(microseconds)
        if sfract[-1] == 0: sfract=sfract[:-1]
        if sfract[-1] == 0: sfract=sfract[:-1]
        result = datetime.utcfromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S') + '.%s' % sfract
    else:
        result = datetime.utcfromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S')
    return "%s" % result


def copy(value):
    if value is None:
        return 'null'
    return value


def quote(value):
    if value is None:
        return 'null'
    return "'%s'" % value


def copy_json(value):
    sjson = json.dumps(value)
    sjson = sjson.replace("'", "''")
    return "'%s'" % sjson


def parse_user_properties(arr):
    result = {}

    for row in arr:
        key = row['key']
        value = row['value']
        if key == 'first_open_time':
            result[ 'first_open_time' ]           = timestamp2string( value[ 'int_value' ] )
            result[ 'first_open_time_set' ]       = value[ 'set_timestamp_micros' ]
        elif key == 'ga_session_id':
            result[ 'ga_session_id' ]             = value[ 'int_value' ]
            result[ 'ga_session_id_set' ]         = value[ 'set_timestamp_micros' ]
        elif key == 'ga_session_number':
            result[ 'ga_session_number' ]         = value[ 'int_value' ]
            result[ 'ga_session_number_set' ]     = value[ 'set_timestamp_micros' ]
        elif key == 'backup_enabled':
            result[ 'backup_enabled' ]            = value[ 'string_value' ]
            result[ 'backup_enabled_set' ]        = value[ 'set_timestamp_micros' ]
        elif key == 'notifications_enabled':
            result[ 'notifications_enabled' ]     = value[ 'string_value' ]
            result[ 'notifications_enabled_set' ] = value[ 'set_timestamp_micros' ]
        elif key == 'default_bill_cushion':
            result[ 'default_bill_cushion' ]      = value[ 'string_value' ]
            result[ 'default_bill_cushion_set' ]  = value[ 'set_timestamp_micros' ]
        elif key == 'default_pay_cushion':
            result[ 'default_pay_cushion' ]       = value[ 'string_value' ]
            result[ 'default_pay_cushion_set' ]   = value[ 'set_timestamp_micros' ]
        elif key == 'currency':
            result[ 'currency' ]                  = value[ 'string_value' ]
            result[ 'currency_set' ]              = value[ 'set_timestamp_micros' ]
        elif key == 'saving_entry_name':
            result[ 'saving_entry_name' ]         = value[ 'string_value' ]
            result[ 'saving_entry_name_set' ]     = value[ 'set_timestamp_micros' ]
        else:
            print('Unknown user properties key (%s)' % key)

    sjson = json.dumps(result)
    sjson = sjson.replace("'", "''")
    return "'%s'" % sjson


def parse_event_params(arr):
    result = {}

    for row in arr:
        key = row['key']
        value = row['value']
        if   key == 'firebase_screen_class':
            result[ 'firebase_screen_class'     ] = value['string_value']
        elif key == 'firebase_event_origin':
            result[ 'firebase_event_origin'     ] = value['string_value']
        elif key == 'firebase_screen_id':
            result[ 'firebase_screen_id'        ] = value['int_value']
        elif key == 'firebase_screen':
            result[ 'firebase_screen'           ] = value['string_value']
        elif key == 'firebase_previous_screen':
            result[ 'firebase_previous_screen'  ] = value['string_value']
        elif key == 'firebase_previous_class':
            result[ 'firebase_previous_class'   ] = value['string_value']
        elif key == 'firebase_previous_id':
            result[ 'firebase_previous_id'      ] = value['int_value']
        elif key == 'engagement_time_msec':
            result[ 'engagement_time_msec'      ] = value['int_value']
        elif key == 'system_app_update':
            result[ 'system_app_update'         ] = value['int_value']
        elif key == 'update_with_analytics':
            result[ 'update_with_analytics'     ] = value['int_value']
        elif key == 'previous_first_open_count':
            result[ 'previous_first_open_count' ] = value['int_value']
        elif key == 'firebase_conversion':
            result[ 'firebase_conversion'       ] = value['int_value']
        elif key == 'system_app':
            result[ 'system_app'                ] = value['int_value']
        elif key == 'selected_option':
            result[ 'selected_option'           ] = value['string_value']
        elif key == 'click_timestamp':
            result[ 'click_timestamp'           ] = value['int_value']
        elif key == 'campaign_info_source':
            result[ 'campaign_info_source'      ] = value['string_value']
        elif key == 'gclid':
            result[ 'gclid'                     ] = value['string_value']
        elif key == 'value':
            result[ 'value'                     ] = value['int_value']
        elif key == 'medium':
            result[ 'medium'                    ] = value['string_value']
        elif key == 'source':
            result[ 'source'                    ] = value['string_value']
        elif key == 'ga_session_number':
            result[ 'ga_session_number'         ] = value['int_value']
        elif key == 'session_engaged':
            result[ 'session_engaged'           ] = value['int_value']
        elif key == 'ga_session_id':
            result[ 'ga_session_id'             ] = value['int_value']
        elif key == 'engaged_session_event':
            result[ 'engaged_session_event'     ] = value['int_value']
        elif key == 'entrances':
            result[ 'entrances'                 ] = value['int_value']
        elif key == 'cushion_used':
            result[ 'cushion_used'              ] = value['string_value']
        elif key == 'recurring':
            result[ 'recurring'                 ] = value['int_value']
        elif key == 'previous_app_version':
            result[ 'previous_app_version'      ] = value['string_value']
        elif key == 'previous_os_version':
            result[ 'previous_os_version'       ] = value['string_value']
        elif key == 'fatal':
            result[ 'fatal'                     ] = value['int_value']
        elif key == 'timestamp':
            result[ 'timestamp'                 ] = value['int_value']
        else:
            print('Unknown event params key (%s)' % key)

    sjson = json.dumps(result)
    sjson = sjson.replace("'", "''")
    return "'%s'" % sjson


for row in cursor:
    id                            = copy( row[ colnames['id'] ] )
    event_date                    = quote( row[ colnames['event_date'] ] )
    event_timestamp               = timestamp2datetime( row[ colnames['event_timestamp'] ] )
    event_name                    = quote( row[ colnames['event_name'] ] )
    event_params                  = parse_event_params( row[ colnames['event_params'] ] )
    event_previous_timestamp      = timestamp2datetime( row[ colnames['event_previous_timestamp'] ] )
    event_value_in_usd            = 'null' # row[ colnames['event_value_in_usd'] ]
    event_bundle_sequence_id      = 'null' # row[ colnames['event_bundle_sequence_id'] ]
    event_server_timestamp_offset = 'null' # row[ colnames['event_server_timestamp_offset'] ]
    user_id                       = 'null' # row[ colnames['user_id'] ]
    user_pseudo_id                = quote( row[ colnames['user_pseudo_id'] ] )
    user_properties               = parse_user_properties( row[ colnames['user_properties'] ] )
    user_first_touch_timestamp    = timestamp2datetime( row[ colnames['user_first_touch_timestamp'] ] )
    user_ltv                      = 'null' # row[ colnames['user_ltv'] ]
    device                        = 'null' # row[ colnames['device'] ]
    geo                           = 'null' # row[ colnames['geo'] ]
    app_info                      = 'null' # row[ colnames['app_info'] ]
    traffic_source                = copy_json( row[ colnames['traffic_source'] ] )
    stream_id                     = 'null' # row[ colnames['stream_id'] ]
    platform                      = 'null' # row[ colnames['platform'] ]
    event_dimensions              = 'null' # row[ colnames['event_dimensions'] ]

    query  = "INSERT INTO events"
    query += "  (events_import_bigquery_fk, event_date, event_timestamp, event_name, event_params, event_previous_timestamp,"
    query += "   event_value_in_usd, event_bundle_sequence_id, event_server_timestamp_offset, user_id, user_pseudo_id,"
    query += "   user_properties, user_first_touch_timestamp, user_ltv, device, geo, app_info, traffic_source,"
    query += "   stream_id, platform, event_dimensions) "
    query += "VALUES ("
    query += "   %s," % id
    query += "   %s," % event_date
    query += "   %s," % event_timestamp
    query += "   %s," % event_name
    query += "   %s," % event_params
    query += "   %s," % event_previous_timestamp
    query += "   %s," % event_value_in_usd
    query += "   %s," % event_bundle_sequence_id
    query += "   %s," % event_server_timestamp_offset
    query += "   %s," % user_id
    query += "   %s," % user_pseudo_id
    query += "   %s," % user_properties
    query += "   %s," % user_first_touch_timestamp
    query += "   %s," % user_ltv
    query += "   %s," % device
    query += "   %s," % geo
    query += "   %s," % app_info
    query += "   %s," % traffic_source
    query += "   %s," % stream_id
    query += "   %s," % platform
    query += "   %s)" % event_dimensions

    insert.execute(query)

    if (id % 1000 == 0):
        print('Processed %s records' % id)

print('Processed %s records' % id)

insert.execute('COMMIT')
insert.close()
cursor.close()
conn.close()
