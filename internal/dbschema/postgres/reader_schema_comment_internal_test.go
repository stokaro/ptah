package postgres

// White-box testing required: readSchemaInfo is unexported, and the SQL it
// issues is the whole subject. A catalog that refuses obj_description refuses
// the statement that asks for it, so the comment is not a field that comes back
// empty -- asking for it costs the schema read entirely.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// Measured on the Cloud Spanner emulator through PGAdapter 0.55.2:
//
//	select coalesce(obj_description(2200, 'pg_namespace'),'')
//	ERROR: The Postgres Type is not supported: name
//
// while `select nspname from (…) n` is accepted. The refusal is the catalog
// name argument, not the schema read, which is why the reader can still read
// the schema without it (stokaro/ptah#942).

// schemaQueryRecorder keeps the SQL it was asked. It answers with one row,
// which is what QueryRow needs; emptyQueryRecorder answers with none, for the
// reads that iterate.
type schemaQueryRecorder struct {
	statements []string
}

// emptyQueryRecorder records the SQL and answers no rows, so a read that
// iterates completes without the recorder having to know each query's shape.
type emptyQueryRecorder struct {
	statements []string
}

func (r *emptyQueryRecorder) handle(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	// The pg_relation_size probe is a QueryRow and needs an answer; every other
	// read here iterates and is content with none.
	if strings.Contains(query, "p.proname = 'pg_relation_size'") {
		return dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{false}}}, nil
	}
	r.statements = append(r.statements, query)
	return dbtest.QueryResult{Columns: []string{"unused"}}, nil
}

func (r *schemaQueryRecorder) handle(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	r.statements = append(r.statements, query)
	return dbtest.QueryResult{
		Columns: []string{"nspname", "schema_comment"},
		Rows:    [][]driver.Value{{"public", ""}},
	}, nil
}

func TestReadSchemaInfoAsksForACommentOnlyWhenTheCatalogAnswers(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want bool
	}{
		{
			name: "a catalog that answers obj_description is asked",
			caps: capability.Postgres16(),
			want: true,
		},
		{
			// Spanner's PostgreSQL interface. Asking anyway loses the schema.
			name: "a catalog that refuses it is not",
			caps: capability.SpannerPostgres(),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			recorder := &schemaQueryRecorder{}
			db := dbtest.Open(t, recorder.handle)

			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", tt.caps)
			schema, err := reader.readSchemaInfo("public")

			c.Assert(err, qt.IsNil)
			c.Assert(schema.Name, qt.Equals, "public")
			c.Assert(recorder.statements, qt.HasLen, 1)
			c.Assert(strings.Contains(recorder.statements[0], "obj_description"), qt.Equals, tt.want)
		})
	}
}

func TestTablesQueryDropsWhatTheCatalogCannotAnswer(t *testing.T) {
	tests := []struct {
		name     string
		caps     capability.Capabilities
		fragment string
		want     bool
	}{
		{name: "a comment is asked for where obj_description resolves", caps: capability.Postgres16(),
			fragment: "obj_description", want: true},
		{name: "and not where it does not", caps: capability.SpannerPostgres(),
			fragment: "obj_description", want: false},
		{name: "the statistics view is joined where it exists", caps: capability.Postgres16(),
			fragment: "pg_stat_all_tables", want: true},
		{name: "and not where it does not", caps: capability.SpannerPostgres(),
			fragment: "pg_stat_all_tables", want: false},
		{name: "the statistics column is read where the view is joined", caps: capability.Postgres16(),
			fragment: "st.n_live_tup", want: true},
		{
			// Leaving this behind is a missing-FROM-clause error rather than a
			// null, which is how the join and the projection came apart.
			name: "and not where the join is gone", caps: capability.SpannerPostgres(),
			fragment: "st.n_live_tup", want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			recorder := &emptyQueryRecorder{}
			db := dbtest.Open(t, recorder.handle)

			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", tt.caps)
			_, err := reader.readTables()

			c.Assert(err, qt.IsNil)
			c.Assert(len(recorder.statements) > 0, qt.IsTrue)
			// Across every statement the read issued: "asked for" means some
			// query names it, and "not asked for" means none does.
			c.Assert(strings.Contains(strings.Join(recorder.statements, "\n"), tt.fragment),
				qt.Equals, tt.want)
		})
	}
}

func TestCatalogDependenciesAreNotReadWhereThereAreNone(t *testing.T) {
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantQuery bool
	}{
		{name: "read where the type system exists", caps: capability.Postgres16(), wantQuery: true},
		{name: "skipped where it does not", caps: capability.SpannerPostgres(), wantQuery: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			recorder := &emptyQueryRecorder{}
			db := dbtest.Open(t, recorder.handle)

			// The read joins pg_depend, and a missing relation cannot be stood
			// in for by a constant the way a missing function can, so the whole
			// read is skipped rather than reduced.
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", tt.caps)
			err := reader.readUserTypesInto(&types.DBSchema{})

			c.Assert(err, qt.IsNil)
			c.Assert(len(recorder.statements) > 0, qt.Equals, tt.wantQuery)
		})
	}
}
