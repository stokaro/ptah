-- Create users table (this should succeed)
CREATE TABLE [users] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [email] NVARCHAR(255) NOT NULL UNIQUE,
    [name] NVARCHAR(255) NOT NULL,
    [created_at] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
