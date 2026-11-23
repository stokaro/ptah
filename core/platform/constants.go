package platform

// The canonical names of the built-in dialects. Every other accepted spelling
// folds onto one of these through NormalizeDialect. The constants are names,
// not the declaration: what each dialect is and which spellings select it is
// stated by its descriptor, which [Dialects] serves. Use the constants rather
// than a string a user typed or a literal of your own: the names are the
// public identifiers, and the values can still change before a stable release.
const (
	Postgres    = "postgres"
	MySQL       = "mysql"
	MariaDB     = "mariadb"
	ClickHouse  = "clickhouse"
	SQLite      = "sqlite"
	SQLServer   = "sqlserver"
	CockroachDB = "cockroachdb"
	YugabyteDB  = "yugabytedb"
	Oracle      = "oracle"
)

// NormalizeDialect folds every spelling of a target onto the one canonical
// name the rest of Ptah compares against, and returns "" for a name it does
// not know. It reads the process registry; see [Dialects].
//
// The empty answer is load-bearing: a caller that treats it as a dialect asks
// every layer below to render for a target nobody implemented. Check it.
//
// A name a descriptor adds becomes valid in every layer that switches on the
// result, including the ones that cannot handle it yet: a renderer, a reader
// and a planner each carry their own list, and none of them is derived from
// the registry yet.
func NormalizeDialect(dialect string) string {
	return Dialects().Normalize(dialect)
}

// DialectSpellings returns every target name and alias accepted by
// NormalizeDialect, each name before its aliases. Each call returns an
// independent slice. Composition uses this declaration so registered aliases
// cannot drift from normalization.
func DialectSpellings() []string {
	return Dialects().Spellings()
}

// IsPostgresFamily reports whether a target speaks the PostgreSQL wire protocol
// and catalog, which is what lets one reader and one renderer serve every
// member. The answer is the descriptor's family; see [Lookup].
//
// It answers a question about the DIALECT, not about a feature: CockroachDB
// and YugabyteDB are in the family and each refuses things PostgreSQL
// accepts. A caller deciding whether a capability exists reads the capability,
// not this.
func IsPostgresFamily(dialect string) bool {
	target, found := described(dialect)
	return found && target.Family == Postgres
}

// GrantOptionIsPerObject reports whether a WITH GRANT OPTION grant on this
// target belongs to the grantee at the whole object, rather than to one
// privilege. The answer is the descriptor's GrantOptionPerObject; see
// [Lookup]. A spelling no descriptor claims answers false.
//
// Measured on MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3: after
// `GRANT SELECT, INSERT ON t TO r WITH GRANT OPTION`, `REVOKE GRANT OPTION ON
// t FROM r` leaves both privileges in place and makes neither grantable, and a
// later `GRANT INSERT ON t TO r` -- without repeating WITH GRANT OPTION --
// makes SELECT grantable again along with it. The option is a property of
// (grantee, object), not of (grantee, object, privilege). Every other built-in
// target keeps one grantable flag per privilege.
func GrantOptionIsPerObject(dialect string) bool {
	target, found := described(dialect)
	return found && target.GrantOptionPerObject
}

// TableRevokeTakesOnlyTableGrants reports whether a REVOKE on a table, on
// this target, takes only a privilege granted on that table and fails where
// the grantee holds the privilege nowhere on the table. The answer is the
// descriptor's TableRevokeTakesOnlyTableGrants; see [Lookup]. A spelling no
// descriptor claims answers false.
//
// Measured on MySQL 8.4.13, MariaDB 10.11.19 and 11.8.9 and Oracle 23.26.3. A
// REVOKE of a privilege the grantee holds nowhere on the table fails, with
// Error 1147 on MySQL and MariaDB and ORA-01927 on Oracle. It fails the same
// way where `GRANT SELECT ON d.*`, `GRANT SELECT ON *.*`, a MariaDB grant to
// PUBLIC or SELECT ANY TABLE gives the grantee the privilege, and a member of
// the grantee keeps reading the table. PostgreSQL answers false: a REVOKE of a
// privilege nobody holds succeeds there.
func TableRevokeTakesOnlyTableGrants(dialect string) bool {
	target, found := described(dialect)
	return found && target.TableRevokeTakesOnlyTableGrants
}

// NamesFoldBySetting reports whether this target compares database and table
// names by a setting each server is started with, rather than by one rule for
// every server. The answer is the descriptor's NamesFoldBySetting; see
// [Lookup]. A spelling no descriptor claims answers false.
//
// MySQL and MariaDB answer true: lower_case_table_names 0 keeps FA and fa
// apart, and 1 and 2 read them as one database. Measured on MySQL 8.4.9 with
// 0, and on MySQL 26.10.0 and MariaDB 12.3.3 with 1.
func NamesFoldBySetting(dialect string) bool {
	target, found := described(dialect)
	return found && target.NamesFoldBySetting
}

// ColumnPrivilegeIsBySpelling reports whether a privilege on a column, held
// through a role, serves only a statement that spells the column the way the
// GRANT did. The answer is the descriptor's ColumnPrivilegeBySpelling; see
// [Lookup]. A spelling no descriptor claims answers false.
//
// Measured on MySQL 8.4 and 26.10: with `GRANT SELECT (Name) ON t TO r`, a
// user holding r runs `SELECT Name FROM t` and gets ERROR 1143 for `SELECT
// name FROM t`, while a user granted SELECT (Name) directly runs both. MariaDB
// 11.8 and 12.3 serve both spellings either way.
func ColumnPrivilegeIsBySpelling(dialect string) bool {
	target, found := described(dialect)
	return found && target.ColumnPrivilegeBySpelling
}
