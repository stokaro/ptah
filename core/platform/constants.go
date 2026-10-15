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
	Spanner     = "spanner"
	Oracle      = "oracle"
	// YDB is the YDB database, read and written in its own query language,
	// YQL, over gRPC. It is not in the PostgreSQL family: YDB serves no
	// PostgreSQL wire protocol or syntax, and in YQL a double-quoted "x" is a
	// string rather than a name.
	YDB = "ydb"
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
// It answers a question about the DIALECT, not about a feature: CockroachDB,
// YugabyteDB and Spanner are in the family and each refuses things PostgreSQL
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
