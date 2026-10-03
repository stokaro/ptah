-- Create users table (this should succeed)
CREATE TABLE users (
    id Serial NOT NULL,
    email Utf8 NOT NULL,
    name Utf8 NOT NULL,
    created_at Timestamp,
    PRIMARY KEY (id)
);
