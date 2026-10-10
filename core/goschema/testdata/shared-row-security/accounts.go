package sharedrowsecurity

// Account carries row-level security scoped to a target no owner holds it for.
// Its scope keeps it out of the PostgreSQL row-security owner, the SQL Server
// security policy owner and the ClickHouse row policy owner, so it stays in
// the shared schema model.
//
//ptah:schema:table name="accounts"
//ptah:schema:rls:enable table="accounts" dialects="mysql"
//ptah:schema:rls:policy name="accounts_owner" table="accounts" for="SELECT" using="owner_id = 1" dialects="mysql"
type Account struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
	//ptah:schema:field name="owner_id" type="INTEGER"
	OwnerID int64
}
