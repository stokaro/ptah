-- Create comments table
CREATE TABLE [comments] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [post_id] INT NOT NULL,
    [user_id] INT NOT NULL,
    [content] NVARCHAR(MAX) NOT NULL,
    [created_at] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    [updated_at] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [fk_comments_posts] FOREIGN KEY ([post_id]) REFERENCES [posts]([id]) ON DELETE CASCADE,
    CONSTRAINT [fk_comments_users] FOREIGN KEY ([user_id]) REFERENCES [users]([id]) ON DELETE CASCADE
);

CREATE INDEX [idx_comments_post_id] ON [comments]([post_id]);
CREATE INDEX [idx_comments_user_id] ON [comments]([user_id]);
