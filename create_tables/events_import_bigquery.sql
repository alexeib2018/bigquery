CREATE TABLE events_import_bigquery (
	id SERIAL NOT NULL PRIMARY KEY,
	event_date                     VARCHAR (10),
	event_timestamp                BIGINT,
	event_name                     VARCHAR (32),
	event_params                   JSON,
	event_previous_timestamp       BIGINT,
	event_value_in_usd             NUMERIC (5,2),
	event_bundle_sequence_id       BIGINT,
	event_server_timestamp_offset  BIGINT,
	user_id                        VARCHAR (32),
	user_pseudo_id                 VARCHAR (32),
	user_properties                JSON,
	user_first_touch_timestamp     BIGINT,
	user_ltv                       VARCHAR (32),
	device                         JSON,
	geo                            JSON,
	app_info                       JSON,
	traffic_source                 JSON,
	stream_id                      VARCHAR (32),
	platform                       VARCHAR (32),
	event_dimensions               VARCHAR (32)
);

CREATE UNIQUE INDEX events_import_bigquery_unique ON events_import_bigquery (event_name, event_timestamp, user_pseudo_id);