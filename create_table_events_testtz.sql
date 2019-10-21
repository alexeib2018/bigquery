CREATE TABLE events_testtz (
	id SERIAL NOT NULL PRIMARY KEY,
	event_date            VARCHAR (10),
	event_timestamp       BIGINT,
	postgres_timestamp    TIMESTAMP,
	postgres_timestamptz  TIMESTAMPTZ
);
