-- Create comments table
CREATE TABLE comments (
    id Serial NOT NULL,
    post_id Int32 NOT NULL,
    user_id Int32 NOT NULL,
    content Utf8 NOT NULL,
    created_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (id),
    INDEX idx_comments_post_id GLOBAL SYNC ON (post_id),
    INDEX idx_comments_user_id GLOBAL SYNC ON (user_id)
);
