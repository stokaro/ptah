package schemaselection_test

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
	"go.5x5.cz/ptah/internal/schemaselection"
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
			wantErr: `database URL selects schema "nosuchschema", which does not exist in this database`,
		},
		{
			name:    "an empty answer is refused the same way",
			rawURL:  "postgres://localhost/dev?search_path=nosuchschema",
			current: "",
			wantErr: `database URL selects schema "nosuchschema", which does not exist in this database`,
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

	got, err := schemaselection.RealmSchemas(t.Context(), "cockroachdb", db.SQL)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"extra", "public"})
	c.Assert(queries, qt.HasLen, 1)
	c.Assert(queries[0], qt.Contains, "n.nspname <> 'information_schema'")
	c.Assert(queries[0], qt.Contains, "n.nspname <> 'crdb_internal'")
	c.Assert(queries[0], qt.Contains, "n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'")
}

func TestPostgresNonSystemSchemasPredicate_DerivesSystemSchemasFromDialect(t *testing.T) {

	t.Run("postgres", func(t *testing.T) {
		c := qt.New(t)
		predicate := schemaselection.PostgresNonSystemSchemasPredicate("postgres")

		c.Assert(predicate, qt.Contains, "n.nspname <> 'information_schema'")
		c.Assert(predicate, qt.Not(qt.Contains), "crdb_internal")
		c.Assert(predicate, qt.Contains, "n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'")
	})

	t.Run("cockroachdb", func(t *testing.T) {
		c := qt.New(t)
		predicate := schemaselection.PostgresNonSystemSchemasPredicate("cockroachdb")

		c.Assert(predicate, qt.Contains, "n.nspname <> 'information_schema'")
		c.Assert(predicate, qt.Contains, "n.nspname <> 'crdb_internal'")
		c.Assert(predicate, qt.Contains, "n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'")
	})
}

func TestIsPostgresSystemSchemaPreservesQuotedIdentifierIdentity(t *testing.T) {
	c := qt.New(t)

	for _, name := range []string{"pg_catalog", "pg_toast", "information_schema"} {
		c.Assert(schemaselection.IsPostgresSystemSchema(name), qt.IsTrue, qt.Commentf("name: %q", name))
	}
	for _, name := range []string{"public", "extensions", "PG_CATALOG", " pg_catalog ", " "} {
		c.Assert(schemaselection.IsPostgresSystemSchema(name), qt.IsFalse, qt.Commentf("name: %q", name))
	}
}

func TestIsPostgresFamilySystemSchemaAddsOnlyCockroachDBInternal(t *testing.T) {

	tests := []struct {
		name    string
		dialect string
		schema  string
		want    bool
	}{
		{name: "Cockroach internal", dialect: "cockroachdb", schema: "crdb_internal", want: true},
		{name: "Postgres does not own it", dialect: "postgres", schema: "crdb_internal", want: false},
		{name: "quoted lookalike", dialect: "cockroachdb", schema: "CRDB_INTERNAL", want: false},
		{name: "common catalog", dialect: "cockroachdb", schema: "pg_catalog", want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				schemaselection.IsPostgresFamilySystemSchema(test.dialect, test.schema),
				qt.Equals,
				test.want,
			)
		})
	}
}

func TestValidateDeclaredPostgresSystemSchemasRefusesServerNamespaces(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
	}{
		{name: "PostgreSQL catalog", dialect: "postgres", schema: "pg_catalog"},
		{name: "PostgreSQL reserved prefix", dialect: "yugabytedb", schema: "pg_app"},
		{name: "information schema", dialect: "spanner", schema: "information_schema"},
		{name: "CockroachDB internal", dialect: "cockroachdb", schema: "crdb_internal"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := schemaselection.ValidateDeclaredPostgresSystemSchemas(
				test.dialect,
				[]goschema.Schema{{Name: test.schema}},
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches,
				`.*declares server-owned PostgreSQL schema "`+test.schema+`".*`)
		})
	}
}

func TestValidateDeclaredPostgresSystemSchemasKeepsUserNamespaces(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
	}{
		{name: "quoted catalog lookalike", dialect: "postgres", schema: "PG_CATALOG"},
		{name: "CockroachDB lookalike", dialect: "cockroachdb", schema: "CRDB_INTERNAL"},
		{name: "CockroachDB namespace on PostgreSQL", dialect: "postgres", schema: "crdb_internal"},
		{name: "non PostgreSQL target", dialect: "mysql", schema: "pg_catalog"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := schemaselection.ValidateDeclaredPostgresSystemSchemas(
				test.dialect,
				[]goschema.Schema{{Name: test.schema}},
			)
			c.Assert(err, qt.IsNil)
		})
	}
}

// errString renders an error for comparison, so the table can carry the wanted
// message as one field instead of a nil check plus a match in every row.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
