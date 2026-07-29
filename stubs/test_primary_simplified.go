package stubs

//migrator:schema:table name="test_primary"
type TestPrimary struct {
	// Test simplified primary syntax
	//migrator:schema:field name="id" type="SERIAL" primary not_null
	ID int `db:"id"`

	// Test simplified unique syntax
	//migrator:schema:field name="email" type="VARCHAR(255)" unique not_null
	Email string `db:"email"`

	// Nullable is the default when not_null is omitted.
	//migrator:schema:field name="description" type="TEXT"
	Description *string `db:"description"`

	// Test a separate index directive on the field.
	//migrator:schema:field name="username" type="VARCHAR(100)" unique not_null
	//migrator:schema:index name="idx_test_primary_username" fields="username"
	Username string `db:"username"`
}
