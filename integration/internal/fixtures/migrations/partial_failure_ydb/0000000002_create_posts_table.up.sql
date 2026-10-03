-- Create posts table (this should succeed)
CREATE TABLE posts (
    id Serial NOT NULL,
    user_id Int32 NOT NULL,
    title Utf8 NOT NULL,
    content Utf8,
    created_at Timestamp,
    PRIMARY KEY (id)
);
