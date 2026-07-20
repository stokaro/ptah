-- This migration should fail due to invalid SQL
CREATE TABLE [invalid_table] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [invalid_column] INVALID_TYPE_THAT_DOES_NOT_EXIST,
    [another_column] NVARCHAR(255)
);
