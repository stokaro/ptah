package entities

//migrator:schema:table name="accounts"
type Account struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//migrator:schema:field name="region" type="VARCHAR(64)" not_null="true"
	Region string
}

//migrator:schema:constraint name="accounts_identity_unique" type="UNIQUE" table="accounts" columns="email"
