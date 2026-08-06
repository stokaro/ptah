// Package models is the fixture behind
// TestFromDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName.
//
// Every declaration here exists to separate one dialect-name predicate in
// fromschema from the spelling the caller typed. Deleting any of them makes the
// parity test pass for a reason it should not:
//
//   - platform.<name>.* on note pins applyPlatformOverrides' map lookup for all
//     nine engines, with a distinct value per engine. Measured: stripping these
//     overrides and then reverting the lookup to index Overrides by the raw
//     dialect name leaves the parity test green, so they are the only thing
//     holding that comparison. SQLite gets a default rather than only a type
//     because the SQLite renderer maps every string width onto TEXT and would
//     otherwise erase the difference.
//   - enum_status plus ref_status pins handleEnumTypes / applyInlineEnumModel
//     against emitsStandaloneEnumDefinitions: on the four engines that model an
//     enum on the column, a spelling that reaches only one of the two halves
//     drops the enum entirely.
//   - active_users and users_touch pin supportsStandaloneViewsAndTriggers.
//   - email_lower pins defaultGeneratedKind (STORED / VIRTUAL / PERSISTED).
//   - orders.user_id pins isSQLiteTarget (inline vs ALTER TABLE foreign keys)
//     and isMySQLFamilyTarget (index emission order around ADD CONSTRAINT).
//   - the PostgreSQL object block pins isPostgreSQLPlatform.
//
// The app-qualified audit table exercises renderTableName, but it does not pin
// sqlident.Quote's quote style: the dialect renderer re-quotes the table name,
// so reverting Quote to raw-string matching leaves this fixture green. That
// comparison is pinned in internal/sqlident/sqlident_test.go instead.
package models

//ptah:schema:extension name="pgcrypto" if_not_exists="true"
type ExtensionsMarker struct{}

//ptah:schema:enum name="enum_status" values="active,archived"
type StatusEnumMarker struct{}

//ptah:schema:role name="app_user" login="true" inherit="true"
type AppRoleMarker struct{}

//ptah:schema:sequence name="order_number_seq" as="bigint" start="1000" increment="1"
type OrderSeqMarker struct{}

//ptah:schema:domain name="email_domain" type="TEXT" not_null="true" check="VALUE ~ '@'"
type EmailDomainMarker struct{}

//ptah:schema:composite name="addr_type" fields="street:TEXT,city:TEXT"
type AddressCompositeMarker struct{}

//ptah:schema:range name="float_range" subtype="float8"
type FloatRangeMarker struct{}

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true"
	Email string

	//ptah:schema:field name="ref_status" type="enum_status"
	RefStatus string

	//ptah:schema:field name="note" type="VARCHAR(64)" platform.postgres.type="VARCHAR(80)" platform.cockroachdb.type="VARCHAR(81)" platform.yugabytedb.type="VARCHAR(82)" platform.spanner.type="STRING(83)" platform.clickhouse.type="FixedString(9)" platform.sqlite.type="VARCHAR(85)" platform.sqlite.default="sqlite-only" platform.mysql.type="VARCHAR(86)" platform.mariadb.type="VARCHAR(87)" platform.sqlserver.type="NVARCHAR(88)"
	Note string

	//ptah:schema:field name="email_lower" type="TEXT" generated="LOWER(email)"
	EmailLower string

	//ptah:schema:field name="updated_at" type="TIMESTAMP"
	UpdatedAt *string

	//ptah:schema:index name="idx_users_email" fields="email" unique="true"
	_ int
}

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//ptah:schema:field name="user_id" type="BIGINT" not_null="true" foreign="users(id)"
	UserID int64

	//ptah:schema:index name="idx_orders_user" fields="user_id"
	_ int
}

//ptah:schema:table name="audit" schema="app"
type Audit struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}

//ptah:schema:function name="touch_updated" returns="trigger" language="plpgsql" body="BEGIN NEW.updated_at = NOW(); RETURN NEW; END;"
type FunctionMarker struct{}

//ptah:schema:view name="active_users" body="SELECT id, email FROM users"
type ActiveUsersView struct{}

//ptah:schema:matview name="user_counts" body="SELECT COUNT(*) AS cnt FROM users"
type UserCountsMatView struct{}

//ptah:schema:trigger name="users_touch" table="users" timing="BEFORE" event="UPDATE" for="ROW" body="NEW.updated_at = NOW(); RETURN NEW;"
type UsersTouchTrigger struct{}

//ptah:schema:rls:enable table="users"
//ptah:schema:rls:policy name="users_self" table="users" for="SELECT" to="app_user" using="true"
type SecurityMarker struct{}

//ptah:schema:grant role="app_user" privilege="SELECT" on_table="users"
type AccessControlMarker struct{}
