-- Create posts table. YDB has no foreign keys, and adds a further index with
-- ALTER TABLE, one index per statement.
CREATE TABLE posts (
    id Serial NOT NULL,
    user_id Int32 NOT NULL,
    title Utf8 NOT NULL,
    content Utf8,
    published Bool NOT NULL DEFAULT false,
    created_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (id),
    INDEX idx_posts_user_id GLOBAL SYNC ON (user_id)
);

ALTER TABLE posts ADD INDEX idx_posts_published GLOBAL SYNC ON (published);
