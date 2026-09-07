package entities

// Define helper functions for tenant context management
// Add a new function for user validation
// Enable RLS and create policies for users table with separate policies for different operations
//
//ptah:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;"
//ptah:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" volatility="STABLE" body="BEGIN RETURN current_setting('app.current_tenant_id', true); END;"
//ptah:schema:function name="validate_user_access" params="user_id_param INTEGER, tenant_id_param TEXT" returns="BOOLEAN" language="plpgsql" volatility="STABLE" body="BEGIN RETURN EXISTS(SELECT 1 FROM users WHERE id = user_id_param AND tenant_id = tenant_id_param); END;"
//ptah:schema:rls:enable table="users" comment="Enable RLS for multi-tenant isolation"
//ptah:schema:rls:policy name="user_tenant_select" table="users" for="SELECT" to="PUBLIC" using="tenant_id = get_current_tenant_id()" comment="Allows users to select only their tenant's data"
//ptah:schema:rls:policy name="user_tenant_insert" table="users" for="INSERT" to="PUBLIC" with_check="tenant_id = get_current_tenant_id()" comment="Ensures new users are created in the correct tenant"
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

	//ptah:schema:field name="role" type="VARCHAR(50)" not_null="true" default="user"
	Role string `json:"role" db:"role"`

	//ptah:schema:field name="created_at" type="TIMESTAMP" default_expr="NOW()"
	CreatedAt string `json:"created_at" db:"created_at"`
}
