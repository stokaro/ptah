-- Create comments table
CREATE TABLE comments (
    id UInt64,
    post_id UInt64,
    user_id UInt64,
    content String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY id;
