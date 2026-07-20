-- Create posts table
CREATE TABLE [posts] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [user_id] INT NOT NULL,
    [title] NVARCHAR(255) NOT NULL,
    [content] NVARCHAR(MAX),
    [published] BIT NOT NULL DEFAULT 0,
    [created_at] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    [updated_at] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [fk_posts_users] FOREIGN KEY ([user_id]) REFERENCES [users]([id]) ON DELETE CASCADE
);

CREATE INDEX [idx_posts_user_id] ON [posts]([user_id]);
CREATE INDEX [idx_posts_published] ON [posts]([published]);
