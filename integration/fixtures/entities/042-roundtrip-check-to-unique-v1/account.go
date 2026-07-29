package entities

//ptah:schema:table name="accounts"
type Account struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//ptah:schema:field name="region" type="VARCHAR(64)" not_null="true"
	Region string
}

//ptah:schema:constraint name="accounts_identity_guard" type="CHECK" table="accounts" check="email <> ''"
