package entities

// Define PostgreSQL roles for application security
//
//ptah:schema:role name="app_user" login="true" comment="Application user role for regular users"
//ptah:schema:role name="admin_user" login="true" superuser="true" comment="Administrator role with full privileges"
//ptah:schema:role name="readonly_user" login="true" comment="Read-only user role for reporting"
//ptah:schema:table name="users" comment="User accounts table"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string `json:"email" db:"email"`

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string `json:"name" db:"name"`

	//ptah:schema:field name="created_at" type="TIMESTAMP" default_expr="NOW()"
	CreatedAt string `json:"created_at" db:"created_at"`
}
