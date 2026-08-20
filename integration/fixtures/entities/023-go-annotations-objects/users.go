package entities

//ptah:schema:schema name="public" comment="Fixture public schema"
type SchemaMarker struct{}

//ptah:schema:extension name="pg_trgm" if_not_exists="true" comment="Fixture extension"
type ExtensionsMarker struct{}

//ptah:schema:table name="users"
//ptah:schema:constraint name="users_email_check" type="CHECK" check="email <> ''" comment="Email must not be empty"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//ptah:schema:field name="status" type="ENUM" enum="active,disabled" default="active"
	Status string

	//ptah:schema:field name="name" type="VARCHAR(255)"
	Name string

	//ptah:schema:field name="deleted_at" type="TIMESTAMP"
	DeletedAt *string

	//ptah:schema:field name="updated_at" type="TIMESTAMP" default_expr="NOW()"
	UpdatedAt *string

	//ptah:schema:index name="idx_users_email" fields="email"
	_ int

	//ptah:embedded mode="json" name="metadata" type="JSONB"
	Metadata UserMetadata
}

//ptah:schema:role name="fixture_app_user" login="false" inherit="true" comment="App role for grants demo"
type AppRoleMarker struct{}

//ptah:schema:sequence name="fixture_order_seq" as="bigint" start="1000" increment="1" cache="20" comment="Fixture standalone sequence"
type OrderSeqMarker struct{}

//ptah:schema:domain name="fixture_email" type="TEXT" not_null="true" check="VALUE ~ '@'" comment="Fixture domain"
type EmailDomainMarker struct{}

//ptah:schema:composite name="fixture_address" fields="street:TEXT,city:TEXT,zip:VARCHAR(10)" comment="Fixture composite type"
type AddressCompositeMarker struct{}

//ptah:schema:range name="fixture_floatrange" subtype="float8" subtype_diff="float8mi" comment="Fixture range type"
type FloatRangeMarker struct{}

//ptah:schema:function name="get_fixture_tenant_id" returns="TEXT" language="sql" body="SELECT current_setting('app.tenant_id', true)" comment="Fixture RLS helper"
//ptah:schema:rls:enable table="users" comment="Enable RLS for fixture users"
//ptah:schema:rls:policy name="users_tenant_policy" table="users" for="SELECT" to="fixture_app_user" using="get_fixture_tenant_id() IS NOT NULL" comment="Fixture RLS policy"
type SecurityMarker struct{}

//ptah:schema:view name="active_users" body="SELECT id, email FROM users WHERE deleted_at IS NULL" with_check="false" comment="Active users view"
type ActiveUsersView struct{}

//ptah:schema:matview name="user_stats" body="SELECT COUNT(*) as cnt FROM users" comment="User count matview"
type UserStatsMatView struct{}

//ptah:schema:synonym name="current_users" schema="app" target="dbo.users" comment="Alias for the users table"
type CurrentUsersSynonym struct{}

//ptah:schema:trigger name="users_set_updated_at" table="users" timing="BEFORE" event="UPDATE" for="ROW" body="NEW.updated_at = NOW(); RETURN NEW;" comment="Auto update"
type UserTrigger struct{}

//ptah:schema:grant role="fixture_app_user" privilege="SELECT,INSERT,UPDATE,DELETE" on_table="users" comment="DML grants to fixture_app_user"
//ptah:schema:grant role="fixture_app_user" privileges="SELECT, INSERT" on_table="users" with_option="true" comment="Grant option fixture"
//ptah:schema:grant role="fixture_app_user" privilege="USAGE" on_schema="public" comment="Schema usage"
//ptah:schema:grant role="fixture_app_user" privilege="USAGE,SELECT" on_sequence="fixture_order_seq" comment="Sequence usage for fixture_app_user"
type AccessControlMarker struct{}

//ptah:schema:data table="users" key="id" file="users.yaml"
type UserSeedDataMarker struct{}

type UserMetadata struct {
	TraceID string
}
