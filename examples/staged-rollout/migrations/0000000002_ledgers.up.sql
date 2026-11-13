CREATE TABLE ledgers (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts (id),
    amount INTEGER NOT NULL
);
