package stubs

//ptah:schema:table name="primary_not_null_test"
type PrimaryNotNullTest struct {
	// Primary key with explicit not_null (should only show PRIMARY KEY)
	//ptah:schema:field name="id" type="SERIAL" primary not_null
	ID int `db:"id"`

	// Primary key with explicit not_null="true" (should only show PRIMARY KEY)
	//ptah:schema:field name="alt_id" type="INTEGER" primary="true" not_null="true"
	AltID int `db:"alt_id"`

	// Non-primary field with not_null (should show NOT NULL)
	//ptah:schema:field name="name" type="VARCHAR(255)" not_null
	Name string `db:"name"`

	// Non-primary field with unique and not_null (should show both)
	//ptah:schema:field name="email" type="VARCHAR(255)" unique not_null
	Email string `db:"email"`

	// Nullable field (nullable is the default when not_null is omitted)
	//ptah:schema:field name="description" type="TEXT"
	Description *string `db:"description"`
}
