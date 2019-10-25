CREATE TABLE events (
	id                             SERIAL NOT NULL PRIMARY KEY,
	events_import_bigquery_fk	   INT,
	event_date                     DATE,
	event_timestamp                TIMESTAMP,
	event_name                     VARCHAR (32),
	event_params                   JSON,
	event_previous_timestamp       BIGINT,
	event_value_in_usd             NUMERIC (5,2),
	event_bundle_sequence_id       BIGINT,
	event_server_timestamp_offset  BIGINT,
	user_id                        VARCHAR (32),
	user_pseudo_id                 VARCHAR (32),
	user_properties                JSON,
	user_first_touch_timestamp     TIMESTAMP,
	user_ltv                       VARCHAR (32),
	device                         JSON,
	geo                            JSON,
	app_info                       JSON,
	traffic_source                 JSON,
	stream_id                      VARCHAR (32),
	platform                       VARCHAR (32),
	event_dimensions               VARCHAR (32)
);

CREATE UNIQUE INDEX events_unique ON events (event_name, event_timestamp, user_pseudo_id);
