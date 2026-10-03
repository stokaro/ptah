-- This migration should fail due to a type YQL does not have
CREATE TABLE invalid_table (
    id Serial NOT NULL,
    invalid_column INVALID_TYPE_THAT_DOES_NOT_EXIST,
    another_column Utf8,
    PRIMARY KEY (id)
);
