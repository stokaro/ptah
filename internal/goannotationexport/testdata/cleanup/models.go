package cleanup

//ptah:schema:schema name="app"
type AppSchema struct{}

type Audit struct {
	//ptah:schema:field name="created_at" type="TIMESTAMPTZ" not_null="true"
	CreatedAt string
}

// User is the application user.
//
//ptah:schema:table name="users" schema="app" primary_key="id" api_name="accounts" openapi_name="account_documents" graphql_name="account_records" proto_name="account_records"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true" api_name="contact" openapi_name="contact-email" graphql_name="contactEmail" proto_name="contact_email" api_type="VARCHAR(320)" api_expose="read"
	Email string

	//ptah:embedded mode="inline" prefix="audit_"
	Audit
}
