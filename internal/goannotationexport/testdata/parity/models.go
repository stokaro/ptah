package parity

//ptah:schema:schema name="app" comment="Application schema"
type AppSchema struct{}

//ptah:schema:extension name="pg_trgm" if_not_exists="true" version="1.6" comment="Trigram support"
type Extensions struct{}

//ptah:schema:sequence name="order_seq" schema="app" as="bigint" start="1000" increment="5" minvalue="1000" maxvalue="999999" cache="20" cycle="true" owned_by="app.users.id" if_not_exists="true" comment="Order numbers"
type OrderSequence struct{}

//ptah:schema:domain name="email_address" schema="app" type="TEXT" not_null="true" default_expr="current_user" check="VALUE ~ '@'" comment="Validated email"
type EmailAddress struct{}

//ptah:schema:composite name="postal_address" schema="app" fields="street:TEXT,latitude:DOUBLE PRECISION,amount:NUMERIC(10,2)" comment="Postal address"
type PostalAddress struct{}

//ptah:schema:range name="price_range" schema="app" subtype="NUMERIC" subtype_opclass="numeric_ops" collation="C" canonical="canonical_price" subtype_diff="numeric_subdiff" comment="Price interval"
type PriceRange struct{}

type Audit struct {
	//ptah:schema:field name="created_at" type="TIMESTAMPTZ" not_null="true" platform.mysql.type="DATETIME"
	CreatedAt string
}

//ptah:schema:table name="accounts" schema="app" comment="Accounts"
type Account struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}

//ptah:schema:table name="users" schema="app" engine="InnoDB" comment="Users" primary_key="id" checks="id > 0" custom="WITHOUT OIDS" platform.mysql.engine="MyISAM" platform.mysql.comment="MySQL users"
//ptah:schema:constraint name="users_positive_id" type="CHECK" check="id > 0" comment="Positive identifier"
//ptah:schema:constraint name="users_email_key" type="UNIQUE" columns="email" include="score" nulls_distinct="false" comment="Unique email"
//ptah:schema:constraint name="users_account_fk" type="FOREIGN KEY" columns="account_id" foreign_table="app.accounts" foreign_columns="id" on_delete="CASCADE" on_update="RESTRICT" comment="Account owner"
//ptah:schema:constraint name="users_no_overlap" type="EXCLUDE" using="gist" elements="id WITH =" condition="id > 0" comment="No overlap"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true" identity_generation="ALWAYS" identity_start="100" identity_increment="5" identity_options="CACHE 20" platform.mysql.type="BIGINT AUTO_INCREMENT"
	ID int64

	//ptah:schema:field name="account_id" type="BIGINT" foreign="app.accounts(id)" foreign_key_name="users_account_field_fk" on_delete="CASCADE" on_update="RESTRICT"
	AccountID int64

	//ptah:schema:field name="email" type="TEXT" unique="true" unique_expr="lower(email)" default="nobody@example.test" check="email <> ''" check_name="users_email_not_empty" comment="Email address"
	Email string

	//ptah:schema:field name="score" type="DOUBLE PRECISION" default_expr="0.0"
	Score float64

	//ptah:schema:field name="slug" type="TEXT" generated="lower(email)" generated_kind="STORED"
	Slug string

	//ptah:schema:field name="status" type="ENUM" enum="active,disabled" default="active"
	Status string

	//ptah:schema:index name="users_email_search" fields="email" unique="true" comment="Email lookup" type="GIN" condition="email <> ''" ops="gin_trgm_ops" table="app.users" nulls_distinct="false"
	_ int

	//ptah:schema:index name="users_score_bloom" fields="score" type="bloom_filter" table="app.users" granularity="64"
	_ int

	//ptah:embedded mode="inline" prefix="audit_" platform.mysql.type="DATETIME(6)"
	Audit

	//ptah:embedded mode="json" name="metadata" type="JSONB" nullable="true" comment="User metadata" platform.mysql.type="JSON"
	Metadata map[string]any

	//ptah:embedded mode="relation" field="manager_id" ref="app.accounts(id)" nullable="true" on_delete="SET NULL" on_update="CASCADE" comment="Manager" platform.mysql.type="BIGINT UNSIGNED"
	Manager Account
}

//ptah:schema:table name="named_keys" schema="app" comment="Named keys"
//ptah:schema:constraint name="named_keys_pk" type="PRIMARY KEY" columns="id" comment="Named primary key"
type NamedKey struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64
}

//ptah:schema:function name="app.lookup_user" params="IN user_id BIGINT, OUT display_value DOUBLE PRECISION" returns="DOUBLE PRECISION" language="sql" security="DEFINER" volatility="STABLE" body="SELECT user_id::double precision" comment="Lookup user"
type LookupUser struct{}

//ptah:schema:view name="app.active_users" body="SELECT id FROM app.users WHERE status = 'active'" with_check="true" comment="Active users"
type ActiveUsers struct{}

//ptah:schema:matview name="app.user_stats" body="SELECT count(*) FROM app.users" refresh_strategy="concurrently" comment="User statistics"
type UserStats struct{}

//ptah:schema:trigger name="users_touch" table="app.users" timing="BEFORE" event="UPDATE" for="STATEMENT" body="RETURN NEW;" comment="Touch users"
type UsersTouch struct{}

//ptah:schema:role name="app_user" login="true" password="SCRAM-SHA-256$fixture" superuser="false" createdb="true" createrole="true" inherit="false" replication="true" comment="Application role"
type AppRole struct{}

//ptah:schema:rls:enable table="app.users" comment="Enable user isolation"
//ptah:schema:rls:policy name="users_policy" table="app.users" for="SELECT" to="app_user,PUBLIC" using="id > 0" with_check="id > 0" comment="User isolation"
type UserSecurity struct{}

//ptah:schema:grant role="app_user" privilege="SELECT,INSERT" on_table="app.users" with_option="true" comment="Table access"
//ptah:schema:grant role="app_user" privilege="USAGE" on_schema="app" comment="Schema access"
//ptah:schema:grant role="app_user" privilege="USAGE,SELECT" on_sequence="app.order_seq" comment="Sequence access"
type Grants struct{}

//ptah:schema:data table="users" schema="app" key="id,email" file="users.yaml"
type UserData struct{}
