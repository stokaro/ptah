package sharedrowsecurity

// Account carries the row-level security a ClickHouse target plans as a row
// policy. Its scope keeps it out of the PostgreSQL row-security owner and the
// SQL Server security policy owner, so it stays in the shared schema model.
//
//ptah:schema:table name="accounts"
//ptah:schema:rls:enable table="accounts" dialects="clickhouse"
//ptah:schema:rls:policy name="accounts_owner" table="accounts" for="SELECT" using="owner_id = 1" dialects="clickhouse"
type Account struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
	//ptah:schema:field name="owner_id" type="INTEGER"
	OwnerID int64
}
