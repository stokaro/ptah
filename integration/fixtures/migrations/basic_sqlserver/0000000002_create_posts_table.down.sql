-- Drop posts table
DROP INDEX IF EXISTS [idx_posts_published] ON [posts];
DROP INDEX IF EXISTS [idx_posts_user_id] ON [posts];
DROP TABLE IF EXISTS [posts];
