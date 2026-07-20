-- Create posts table (this should succeed)
CREATE TABLE [posts] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [user_id] INT NOT NULL,
    [title] NVARCHAR(255) NOT NULL,
    [content] NVARCHAR(MAX),
    [created_at] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [fk_posts_users] FOREIGN KEY ([user_id]) REFERENCES [users]([id]) ON DELETE CASCADE
);
