package entities

// Define helper functions for tenant context management
// Enable RLS and create policies for users table
//
//ptah:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;"
//ptah:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" volatility="STABLE" body="BEGIN RETURN current_setting('app.current_tenant_id', true); END;" comment="Gets the current tenant ID from session"
//ptah:schema:rls:enable table="users" comment="Enable RLS for multi-tenant isolation"
//ptah:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="PUBLIC" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"
//ptah:schema:table name="users" comment="User accounts table"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//ptah:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string `json:"tenant_id" db:"tenant_id"`

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string `json:"email" db:"email"`

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string `json:"name" db:"name"`

	//ptah:schema:field name="created_at" type="TIMESTAMP" default_expr="NOW()"
	CreatedAt string `json:"created_at" db:"created_at"`
}
