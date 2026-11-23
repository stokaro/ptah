package schemaselection_test

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/schemaselection"
)

// TestFromURLScope pins which dev URLs restrict schema analysis to one schema
// (stokaro/ptah#1074 S1).
//
// The empty results are what keep the boundary honest: a URL that scopes
// nothing leaves every object under review, which is how a dialect or a URL
// form that has not been measured against the pinned community binary v1.3.0
// stays at least as strict as it is today.
func TestFromURLScope(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "empty URL scopes nothing",
			rawURL: "",
			want:   "",
		},
		{
			name:   "postgres without search_path scopes nothing",
			rawURL: "postgres://localhost:5432/dev?sslmode=disable",
			want:   "",
		},
		{
			name:   "postgres search_path names the reviewed schema",
			rawURL: "postgres://localhost:5432/dev?sslmode=disable&search_path=public",
			want:   "public",
		},
		{
			name:   "a non-public search_path is honored as written",
			rawURL: "postgres://localhost:5432/dev?search_path=app",
			want:   "app",
		},
		{
			name:   "the postgresql spelling is the same dialect",
			rawURL: "postgresql://localhost/dev?search_path=reporting",
			want:   "reporting",
		},
		{
			name:   "cockroachdb is in the PostgreSQL family",
			rawURL: "cockroach://localhost:26257/dev?search_path=app",
			want:   "app",
		},
		{
			name:   "yugabytedb is in the PostgreSQL family",
			rawURL: "yugabyte://localhost:5433/dev?search_path=app",
			want:   "app",
		},
		{
			// The pinned binary reads this value as one schema NAME and refuses
			// the run with `schema "public,app" was not found`, so there is no
			// scoping behavior to match and nothing is filtered.
			name:   "a comma-carrying search_path scopes nothing",
			rawURL: "postgres://localhost/dev?search_path=public,app",
			want:   "",
		},
		{
			name:   "an empty search_path value scopes nothing",
			rawURL: "postgres://localhost/dev?search_path=",
			want:   "",
		},
		{
			name:   "a whitespace-only search_path value scopes nothing",
			rawURL: "postgres://localhost/dev?search_path=%20",
			want:   "",
		},
		{
			name:   "surrounding whitespace is trimmed off the schema name",
			rawURL: "postgres://localhost/dev?search_path=%20app%20",
			want:   "app",
		},
		{
			name:   "one quoted identifier is one schema name, a comma included",
			rawURL: "postgres://localhost/dev?search_path=%22a%2Cb%22",
			want:   "a,b",
		},
		{
			name:   "a quoted name keeps its space",
			rawURL: "postgres://localhost/dev?search_path=%22%20a%22",
			want:   " a",
		},
		{
			name:   "a quoted name keeps its case",
			rawURL: "postgres://localhost/dev?search_path=%22A%22",
			want:   "A",
		},
		{
			name:   "a doubled quote inside a quoted name is one quote",
			rawURL: "postgres://localhost/dev?search_path=%22a%22%22b%22",
			want:   `a"b`,
		},
		{
			name:   "two quoted names are a list, which scopes nothing",
			rawURL: "postgres://localhost/dev?search_path=%22a%22,%22b%22",
			want:   "",
		},
		{
			name:   "MySQL scopes nothing even with a search_path parameter",
			rawURL: "mysql://root@localhost:3306/dev?search_path=public",
			want:   "",
		},
		{
			name:   "SQLite scopes nothing",
			rawURL: "sqlite://file?mode=memory&search_path=public",
			want:   "",
		},
		{
			name:   "an unsupported scheme scopes nothing",
			rawURL: "oracle://localhost/dev?search_path=public",
			want:   "",
		},
		{
			name:   "a docker dev URL without search_path scopes nothing",
			rawURL: "docker://postgres/16/dev",
			want:   "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			c.Assert(schemaselection.FromURL(test.rawURL).Scope, qt.Equals, test.want)
		})
	}
}

// TestFromURLRawSurvivesAValueThatScopesNothing is the fact the two-field shape
// exists for. Scope is the analysis boundary and is deliberately empty for a
// value the pinned binary reads as one schema NAME; Raw is what the operator
// wrote, and the refusal in Resolve quotes it. Collapsing the two would report
// `selects schema ""` for a URL that plainly selects something.
func TestFromURLRawSurvivesAValueThatScopesNothing(t *testing.T) {
	tests := []struct {
		name      string
		rawURL    string
		wantRaw   string
		wantScope string
	}{
		{
			name:      "a comma-carrying value scopes nothing and is still quotable",
			rawURL:    "postgres://localhost/dev?search_path=public,app",
			wantRaw:   "public,app",
			wantScope: "",
		},
		{
			name:      "a single schema is both",
			rawURL:    "postgres://localhost/dev?search_path=app",
			wantRaw:   "app",
			wantScope: "app",
		},
		{
			name:      "no search_path carries nothing to quote",
			rawURL:    "postgres://localhost/dev?sslmode=disable",
			wantRaw:   "",
			wantScope: "",
		},
		{
			name:      "MySQL carries nothing even with the parameter",
			rawURL:    "mysql://root@localhost:3306/dev?search_path=app",
			wantRaw:   "",
			wantScope: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			selection := schemaselection.FromURL(test.rawURL)

			c.Assert(selection.Raw, qt.Equals, test.wantRaw)
			c.Assert(selection.Scope, qt.Equals, test.wantScope)
		})
	}
}

// TestResolveAsksTheServer covers the other fact: what the session landed in,
// which the URL cannot answer. The row that matters is the last one — a URL
// naming a schema the database does not have is refused rather than folded back
// to "public", and the refusal quotes the URL's own value.
func TestResolveAsksTheServer(t *testing.T) {
	tests := []struct {
		name       string
		rawURL     string
		current    driver.Value
		wantSchema string
		wantErr    string
	}{
		{
			name:       "the server's answer wins over the URL's spelling",
			rawURL:     "postgres://localhost/dev?search_path=app",
			current:    "app",
			wantSchema: "app",
		},
		{
			name:       "a URL naming no schema still resolves to a schema",
			rawURL:     "postgres://localhost/dev?sslmode=disable",
			current:    "public",
			wantSchema: "public",
		},
		{
			name:    "a search_path that resolves to nothing is refused, naming the URL's value",
			rawURL:  "postgres://localhost/dev?search_path=nosuchschema",
			current: nil,
			wantErr: `database URL selects schema "nosuchschema", which does not exist in this database or holds no USAGE privilege for the connecting role`,
		},
		{
			name:    "a quoted value is refused naming the schema it quotes",
			rawURL:  "postgres://localhost/dev?search_path=%22a%2Cb%22",
			current: nil,
			wantErr: `database URL selects schema "a,b", which does not exist in this database or holds no USAGE privilege for the connecting role`,
		},
		{
			name:    "a URL that names no schema is refused without quoting one",
			rawURL:  "postgres://localhost/dev",
			current: nil,
			wantErr: `the search_path of the connecting role resolves to no schema: each schema on it does not exist in this database or holds no USAGE privilege for the role`,
		},
		{
			name:    "an empty answer is refused the same way",
			rawURL:  "postgres://localhost/dev?search_path=nosuchschema",
			current: "",
			wantErr: `database URL selects schema "nosuchschema", which does not exist in this database or holds no USAGE privilege for the connecting role`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			var asked []string
			db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				asked = append(asked, query)
				return dbtest.QueryResult{
					Columns: []string{"current_schema"},
					Rows:    [][]driver.Value{{test.current}},
				}, nil
			})

			schema, err := schemaselection.FromURL(test.rawURL).Resolve(t.Context(), db.SQL)

			c.Assert(asked, qt.DeepEquals, []string{"SELECT current_schema()"})
			c.Assert(schema, qt.Equals, test.wantSchema)
			c.Assert(errString(err), qt.Equals, test.wantErr)
		})
	}
}

func TestRealmSchemas_PostgresFamilyQueryExcludesSystemSchemas(t *testing.T) {
	c := qt.New(t)
	var queries []string
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		return dbtest.QueryResult{
			Columns: []string{"nspname"},
			Rows: [][]driver.Value{
				{"public"},
				{"extra"},
			},
		}, nil
	})

	got, err := schemaselection.RealmSchemas(
		t.Context(), "cockroachdb", capability.ForDialect("cockroachdb"), postgresConnection(db.SQL, "cockroachdb"))

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"extra", "public"})
	c.Assert(queries, qt.HasLen, 1)
	c.Assert(queries[0], qt.Contains, "n.nspname <> 'information_schema'")
	c.Assert(queries[0], qt.Contains, "n.nspname <> 'crdb_internal'")
	c.Assert(queries[0], qt.Contains, "n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'")
	c.Assert(queries[0], qt.Contains, "d.deptype = 'e'")
}

// TestRealmSchemas_MySQLFamilyListsTheUserDatabases lists a whole server's
// databases in byte order, and asks for the user databases alone: the server's
// own are left out by name, as the pinned community binary leaves them
// out (stokaro/ptah#3789).
func TestRealmSchemas_MySQLFamilyListsTheUserDatabases(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			var queries []string
			db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				queries = append(queries, query)
				return dbtest.QueryResult{
					Columns: []string{"SCHEMA_NAME"},
					Rows:    [][]driver.Value{{"r2"}, {"R1"}, {"r1"}},
				}, nil
			})

			got, err := schemaselection.RealmSchemas(t.Context(), dialect, capability.ForDialect(dialect), mysqlConnection(db.SQL, dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []string{"R1", "r1", "r2"})
			c.Assert(queries, qt.HasLen, 1)
			c.Assert(queries[0], qt.Contains, "FROM information_schema.SCHEMATA")
			c.Assert(queries[0], qt.Contains, "'performance_schema'")
			c.Assert(queries[0], qt.Contains, "'sys'")
		})
	}
}

// TestRealmSchemas_FailurePath has no probe for a dialect whose writer offers
// no catalog to list its realm, and says so rather than answering no schemas.
// The query is never sent.
func TestRealmSchemas_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, nil
	})

	got, err := schemaselection.RealmSchemas(t.Context(), "sqlserver", capability.ForDialect("sqlserver"), writerlessConnection(db.SQL))

	c.Assert(err, qt.ErrorIs, schemaselection.ErrNoRealmProbe)
	c.Assert(err, qt.ErrorMatches, `no realm-scope schema probe for dialect "sqlserver"`)
	c.Assert(got, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 0)
}

// TestRealmSchemas_ReadFailure wraps a catalog read the server refused.
func TestRealmSchemas_ReadFailure(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, errors.New("permission denied for pg_namespace")
	})

	got, err := schemaselection.RealmSchemas(t.Context(), "postgres", capability.ForDialect("postgres"), postgresConnection(db.SQL, "postgres"))

	c.Assert(err, qt.ErrorMatches, `failed to list realm schemas: permission denied for pg_namespace`)
	c.Assert(got, qt.IsNil)
}

// TestRealmSchemas_ReadsTheWritersCatalog lists the realm the scope catalog
// of the connection's writer lists, for an engine Ptah does not ship, in byte
// order. The function names no engine and sends no query of its own.
func TestRealmSchemas_ReadsTheWritersCatalog(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, nil
	})
	conn := realmConnection{DB: db.SQL, writer: acmeWriter{scope: acmeScope{schemas: []string{"b", "Zed", "a"}}}}

	got, err := schemaselection.RealmSchemas(t.Context(), "acme", capability.Capabilities{}, conn)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"Zed", "a", "b"})
	c.Assert(db.QueryCount(), qt.Equals, 0)
}

// errString renders an error for comparison, so the table can carry the wanted
// message as one field instead of a nil check plus a match in every row.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// A value the pinned community binary reads as one schema name, byte for byte,
// and the server's search_path grammar reads as another is reported as the URL
// writes it, so strict mode can refuse it and say which value to change.
func TestLiteralReadingDiffers_ReportsTheValue(t *testing.T) {
	for _, test := range []struct {
		name, rawURL, want string
	}{
		{name: "a leading space", rawURL: "postgres://h/db?search_path=%20a", want: "%20a"},
		{name: "a trailing space", rawURL: "postgres://h/db?search_path=a%20", want: "a%20"},
		{name: "a space inside", rawURL: "postgres://h/db?search_path=a%20b", want: "a%20b"},
		{name: "a tab", rawURL: "postgres://h/db?search_path=%09a", want: "%09a"},
		{name: "a form feed", rawURL: "postgres://h/db?search_path=a%0C", want: "a%0C"},
		{name: "the session user's schema", rawURL: "postgres://h/db?search_path=$user", want: "$user"},
		{name: "the session user's schema, escaped", rawURL: "postgres://h/db?search_path=%24user", want: "%24user"},
		{name: "a plus sign, which the binary reads as a space", rawURL: "postgres://h/db?search_path=a+b", want: "a+b"},
		{name: "a capital letter", rawURL: "postgres://h/db?search_path=A", want: "A"},
		{name: "a capital letter after a lower-case one", rawURL: "postgres://h/db?search_path=aB", want: "aB"},
		{name: "a comma", rawURL: "postgres://h/db?search_path=a,b", want: "a,b"},
		{name: "an escaped comma", rawURL: "postgres://h/db?search_path=a%2Cb", want: "a%2Cb"},
		{name: "a quoted name", rawURL: "postgres://h/db?search_path=%22a%22", want: "%22a%22"},
		{name: "a quoted name with a space", rawURL: "postgres://h/db?search_path=%22%20a%22", want: "%22%20a%22"},
		{name: "a name the server truncates", rawURL: "postgres://h/db?search_path=" + strings.Repeat("a", 64), want: strings.Repeat("a", 64)},
		{name: "the second value", rawURL: "postgres://h/db?search_path=a&search_path=B", want: "B"},
		{name: "beside other parameters", rawURL: "postgres://u@h/db?sslmode=disable&search_path=A&application_name=x", want: "A"},
		{name: "the postgresql scheme", rawURL: "postgresql://h/db?search_path=A", want: "A"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			got, differs := schemaselection.LiteralReadingDiffers(test.rawURL)

			c.Assert(differs, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The controls: a name the two readings agree on, and a URL that selects no
// schema through a search_path, report nothing.
func TestLiteralReadingDiffers_AcceptsAPlainName(t *testing.T) {
	for _, test := range []struct{ name, rawURL string }{
		{name: "a lower-case name", rawURL: "postgres://h/db?search_path=app"},
		{name: "digits, an underscore and a dollar sign", rawURL: "postgres://h/db?search_path=app_2%24"},
		{name: "a name of 63 bytes", rawURL: "postgres://h/db?search_path=" + strings.Repeat("a", 63)},
		{name: "a no-break space, which the server keeps", rawURL: "postgres://h/db?search_path=%C2%A0a"},
		{name: "a name that only begins with $user", rawURL: "postgres://h/db?search_path=$users"},
		{name: "a plus sign escaped, which both read as a plus", rawURL: "postgres://h/db?search_path=a%2Bb"},
		{name: "no query", rawURL: "postgres://u@h/db"},
		{name: "no search_path", rawURL: "postgres://h/db?sslmode=disable"},
		{name: "an empty search_path", rawURL: "postgres://h/db?search_path="},
		{name: "MySQL", rawURL: "mysql://h/db?search_path=A"},
		{name: "a docker URL", rawURL: "docker://postgres/16/dev?search_path=A"},
		{name: "a file URL", rawURL: "file://schema.sql"},
		{name: "not a URL", rawURL: "%zz"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			got, differs := schemaselection.LiteralReadingDiffers(test.rawURL)

			c.Assert(differs, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
