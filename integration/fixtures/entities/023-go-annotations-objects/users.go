package entities

//migrator:schema:schema name="public" comment="Fixture public schema"
type SchemaMarker struct{}

//migrator:schema:extension name="pg_trgm" if_not_exists="true" comment="Fixture extension"
type ExtensionsMarker struct{}

//migrator:schema:table name="users"
//migrator:schema:constraint name="users_email_check" type="CHECK" check="email <> ''" comment="Email must not be empty"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//migrator:schema:field name="status" type="ENUM" enum="active,disabled" default="active"
	Status string

	//migrator:schema:field name="name" type="VARCHAR(255)"
	Name string

	//migrator:schema:field name="deleted_at" type="TIMESTAMP"
	DeletedAt *string

	//migrator:schema:field name="updated_at" type="TIMESTAMP" default_expr="NOW()"
	UpdatedAt *string

	//migrator:schema:index name="idx_users_email" fields="email"
	_ int

	//migrator:embedded mode="json" name="metadata" type="JSONB"
	Metadata UserMetadata
}

//migrator:schema:role name="fixture_app_user" login="false" inherit="true" comment="App role for grants demo"
type AppRoleMarker struct{}

//migrator:schema:sequence name="fixture_order_seq" as="bigint" start="1000" increment="1" cache="20" comment="Fixture standalone sequence"
type OrderSeqMarker struct{}

//migrator:schema:function name="get_fixture_tenant_id" returns="TEXT" language="sql" body="SELECT current_setting('app.tenant_id', true)" comment="Fixture RLS helper"
//migrator:schema:rls:enable table="users" comment="Enable RLS for fixture users"
//migrator:schema:rls:policy name="users_tenant_policy" table="users" for="SELECT" to="fixture_app_user" using="get_fixture_tenant_id() IS NOT NULL" comment="Fixture RLS policy"
type SecurityMarker struct{}

//migrator:schema:view name="active_users" body="SELECT id, email FROM users WHERE deleted_at IS NULL" with_check="false" comment="Active users view"
type ActiveUsersView struct{}

//migrator:schema:matview name="user_stats" body="SELECT COUNT(*) as cnt FROM users" refresh_strategy="manual" comment="User count matview"
type UserStatsMatView struct{}

//migrator:schema:trigger name="users_set_updated_at" table="users" timing="BEFORE" event="UPDATE" for="ROW" body="NEW.updated_at = NOW(); RETURN NEW;" comment="Auto update"
type UserTrigger struct{}

//migrator:schema:grant role="fixture_app_user" privilege="SELECT,INSERT,UPDATE,DELETE" on_table="users" comment="DML grants to fixture_app_user"
//migrator:schema:grant role="fixture_app_user" privileges="SELECT, INSERT" on_table="users" with_option="true" comment="Grant option fixture"
//migrator:schema:grant role="fixture_app_user" privilege="USAGE" on_schema="public" comment="Schema usage"
//migrator:schema:grant role="fixture_app_user" privilege="USAGE,SELECT" on_sequence="fixture_order_seq" comment="Sequence usage for fixture_app_user"
type AccessControlMarker struct{}

type UserMetadata struct {
	TraceID string
}
