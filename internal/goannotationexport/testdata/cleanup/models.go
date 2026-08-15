package cleanup

//ptah:schema:schema name="app"
type AppSchema struct{}

type Audit struct {
	//ptah:schema:field name="created_at" type="TIMESTAMPTZ" not_null="true"
	CreatedAt string
}

// User is the application user.
//
//ptah:schema:table name="users" schema="app" primary_key="id"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true"
	Email string

	//ptah:embedded mode="inline" prefix="audit_"
	Audit
}
