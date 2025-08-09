package entities

// Enable RLS for multi-tenant isolation
//
//migrator:schema:rls:enable table="users" comment="Enable RLS for multi-tenant user isolation"
//migrator:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"
//migrator:schema:table name="users" comment="User accounts table"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string `json:"tenant_id" db:"tenant_id"`

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string `json:"email" db:"email"`

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string `json:"name" db:"name"`
}
