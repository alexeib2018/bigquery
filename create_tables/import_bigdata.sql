CREATE TABLE import_bigdata (
    id SERIAL NOT NULL PRIMARY KEY,
    created TIMESTAMP DEFAULT NOW(),
    date DATE,
    success INT,
    errors INT,
    output TEXT
);
