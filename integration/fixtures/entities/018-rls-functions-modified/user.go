package entities

// Same tables and policies as 014-rls-functions, but with deliberate function
// attribute changes to exercise the issue #89 / PR #129 modify-function path:
//
//   - set_tenant_context: body switches from set_config(..., false) (session-
//     scoped) to set_config(..., true) (transaction-local), and the
//     SECURITY DEFINER qualifier is removed (defaults back to INVOKER).
//   - get_current_tenant_id: volatility tightens from STABLE to IMMUTABLE.
//
// Before PR #129 the diff comparator already detected these changes, but the
// postgres planner had no handler for diff.FunctionsModified — the rewrites
// silently never reached the database. This fixture exists so the natural
// fixture-driven migration suite verifies that the CREATE OR REPLACE actually
// lands and the live pg_proc reflects the new attributes.
//
//migrator:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, true); END;"
//migrator:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" volatility="IMMUTABLE" body="BEGIN RETURN current_setting('app.current_tenant_id', true); END;" comment="Gets the current tenant ID from session"
//migrator:schema:rls:enable table="users" comment="Enable RLS for multi-tenant isolation"
//migrator:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="PUBLIC" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"
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

	//migrator:schema:field name="created_at" type="TIMESTAMP" default_expr="NOW()"
	CreatedAt string `json:"created_at" db:"created_at"`
}
