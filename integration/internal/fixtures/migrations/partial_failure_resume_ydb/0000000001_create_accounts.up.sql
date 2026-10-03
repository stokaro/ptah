-- The third query fails. The two before it commit on their own: the table,
-- and the INSERT, which a resume must not run a second time.
CREATE TABLE accounts (
    id Int64 NOT NULL,
    balance Int64,
    PRIMARY KEY (id)
);
INSERT INTO accounts (id, balance) VALUES (1, 100);
CREATE TABLE ledger (
    id Int64 NOT NULL,
    amount INVALID_TYPE_THAT_DOES_NOT_EXIST,
    PRIMARY KEY (id)
);
