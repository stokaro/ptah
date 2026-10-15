package schemaselection_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemaselection"
)

// TestDialectDefault_EveryBuiltInDialect pins the default schema of every
// built-in dialect, spelled both canonically and as an alias: "public" on
// PostgreSQL, CockroachDB and YugabyteDB, "main" on SQLite, and nothing on
// the rest. Spanner is in the PostgreSQL family and still answers nothing.
func TestDialectDefault_EveryBuiltInDialect(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: "postgres", want: "public"},
		{dialect: "postgresql", want: "public"},
		{dialect: "cockroachdb", want: "public"},
		{dialect: "crdb", want: "public"},
		{dialect: "yugabytedb", want: "public"},
		{dialect: "spanner", want: ""},
		{dialect: "sqlite", want: "main"},
		{dialect: "libsql", want: "main"},
		{dialect: "mysql", want: ""},
		{dialect: "mariadb", want: ""},
		{dialect: "sqlserver", want: ""},
		{dialect: "clickhouse", want: ""},
		{dialect: "oracle", want: ""},
		{dialect: "ydb", want: ""},
		{dialect: "nosuch", want: ""},
		{dialect: "", want: ""},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaselection.DialectDefault(test.dialect), qt.Equals, test.want)
		})
	}
}

// TestRealm_EveryBuiltInDialect pins which connections describe a whole realm:
// a PostgreSQL-family URL without a search_path, and a MySQL-family connection
// that selected no database. SQLite has no wider scope, and a dialect whose
// boundary nobody measured stays at schema scope.
func TestRealm_EveryBuiltInDialect(t *testing.T) {
	tests := []struct {
		name            string
		dialect         string
		url             string
		connectedSchema string
		want            bool
	}{
		{name: "postgres without search_path", dialect: "postgres", url: "postgres://h/db", connectedSchema: "public", want: true},
		{name: "postgres with search_path", dialect: "postgres", url: "postgres://h/db?search_path=app", connectedSchema: "app", want: false},
		{name: "cockroachdb without search_path", dialect: "cockroachdb", url: "cockroachdb://h/db", connectedSchema: "public", want: true},
		{name: "yugabytedb with search_path", dialect: "yugabytedb", url: "yugabytedb://h/db?search_path=app", connectedSchema: "app", want: false},
		{name: "spanner without search_path", dialect: "spanner", url: "spanner://h/db", connectedSchema: "public", want: true},
		{name: "mysql with no database", dialect: "mysql", url: "mysql://h:3306/", connectedSchema: " ", want: true},
		{name: "mysql with a database", dialect: "mysql", url: "mysql://h:3306/app", connectedSchema: "app", want: false},
		{name: "mariadb with no database", dialect: "mariadb", url: "mariadb://h:3306/", connectedSchema: "", want: true},
		{name: "sqlite", dialect: "sqlite", url: "sqlite://dv?mode=memory", connectedSchema: "", want: false},
		{name: "sqlserver", dialect: "sqlserver", url: "sqlserver://h/db", connectedSchema: "", want: false},
		{name: "clickhouse", dialect: "clickhouse", url: "clickhouse://h/db", connectedSchema: "", want: false},
		{name: "oracle", dialect: "oracle", url: "oracle://h/db", connectedSchema: "", want: false},
		{name: "ydb", dialect: "ydb", url: "ydb://h/local", connectedSchema: "", want: false},
		{name: "unknown dialect", dialect: "nosuch", url: "nosuch://h/db", connectedSchema: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaselection.Realm(test.dialect, test.url, test.connectedSchema), qt.Equals, test.want)
		})
	}
}

// TestFromURL_ReadsSearchPathOnlyWhereItSelects pins that search_path selects a
// schema on every PostgreSQL-family scheme and on no other.
func TestFromURL_ReadsSearchPathOnlyWhereItSelects(t *testing.T) {
	tests := []struct {
		url   string
		raw   string
		scope string
	}{
		{url: "postgres://h/db?search_path=app", raw: "app", scope: "app"},
		{url: "cockroach://h/db?search_path=app", raw: "app", scope: "app"},
		{url: "yugabytedb://h/db?search_path=app", raw: "app", scope: "app"},
		{url: "spanner://h/db?search_path=app", raw: "app", scope: "app"},
		{url: "mysql://h:3306/db?search_path=app", raw: "", scope: ""},
		{url: "sqlserver://h/db?search_path=app", raw: "", scope: ""},
		{url: "sqlite://app.db?search_path=app", raw: "", scope: ""},
		{url: "ydb://h/local?search_path=app", raw: "", scope: ""},
	}
	for _, test := range tests {
		t.Run(test.url, func(t *testing.T) {
			c := qt.New(t)
			got := schemaselection.FromURL(test.url)
			c.Assert(got.Raw, qt.Equals, test.raw)
			c.Assert(got.Scope, qt.Equals, test.scope)
		})
	}
}
