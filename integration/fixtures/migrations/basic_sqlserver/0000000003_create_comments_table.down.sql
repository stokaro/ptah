-- Drop comments table
DROP INDEX IF EXISTS [idx_comments_user_id] ON [comments];
DROP INDEX IF EXISTS [idx_comments_post_id] ON [comments];
DROP TABLE IF EXISTS [comments];
