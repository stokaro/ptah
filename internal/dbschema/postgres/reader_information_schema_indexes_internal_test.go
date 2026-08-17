package postgres

// White-box testing required: readInformationSchemaIndexes is unexported, and
// the branch that
// reaches it is a capability lookup inside readIndexesForSchema. Both halves can
// be right while the reader still asks pg_catalog, which is the state this
// package was in when a live Spanner endpoint answered
// `failed to read indexes: relation "pg_am" does not exist`
// (stokaro/ptah#942).

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// informationSchemaIndexRows is the catalog a live Spanner endpoint answers
// with, measured through PGAdapter on this exact schema:
//
//	CREATE TABLE s (id bigint PRIMARY KEY, a bigint, b text, extra text);
//	CREATE INDEX s_a ON s (a) INCLUDE (extra);
//	CREATE UNIQUE INDEX s_b ON s (b DESC);
//
// The STORING column is the row worth reading twice: it arrives with a NULL
// ordinal position and a NULL ordering, which is how a payload column is
// distinguishable from a key part at all.
func informationSchemaIndexRows() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{
			"table_name", "index_name", "index_type", "is_unique",
			"column_name", "ordinal_position", "column_ordering",
		},
		Rows: [][]driver.Value{
			{"s", "PRIMARY_KEY", "PRIMARY_KEY", "YES", "id", int64(1), "ASC"},
			{"s", "s_a", "INDEX", "NO", "a", int64(1), "ASC"},
			{"s", "s_a", "INDEX", "NO", "extra", nil, nil},
			{"s", "s_b", "INDEX", "YES", "b", int64(1), "DESC"},
		},
	}
}

func informationSchemaIndexReader(c *qt.C, caps capability.Capabilities) (*Reader, *int) {
	c.Helper()

	asked := 0
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		asked++
		c.Assert(query, qt.Contains, "information_schema.indexes",
			qt.Commentf("the reader asked a different catalog"))
		return informationSchemaIndexRows(), nil
	})
	return NewPostgreSQLReaderWithCapabilities(db.SQL, "public", caps), &asked
}

// The SQL-standard catalog reports an index's parts and never its text, so
// every field the model carries has to be assembled from those parts.
func TestReadInformationSchemaIndexes(t *testing.T) {
	tests := []struct {
		name           string
		index          string
		wantColumns    []string
		wantInclude    []string
		wantUnique     bool
		wantPrimary    bool
		wantDefinition string
		wantDesc       bool
	}{
		{
			name:           "the primary key is reported as one",
			index:          "PRIMARY_KEY",
			wantColumns:    []string{"id"},
			wantUnique:     true,
			wantPrimary:    true,
			wantDefinition: "CREATE UNIQUE INDEX PRIMARY_KEY ON s (id)",
		},
		{
			// The payload column is NOT a key part: counting it as one reports
			// a key the table does not have, and a comparison that trusts it
			// plans a rebuild on every run.
			name:           "a storing column is payload, not a key part",
			index:          "s_a",
			wantColumns:    []string{"a"},
			wantInclude:    []string{"extra"},
			wantDefinition: "CREATE INDEX s_a ON s (a) INCLUDE (extra)",
		},
		{
			name:           "a descending unique key keeps both",
			index:          "s_b",
			wantColumns:    []string{"b"},
			wantUnique:     true,
			wantDesc:       true,
			wantDefinition: "CREATE UNIQUE INDEX s_b ON s (b DESC)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			reader, _ := informationSchemaIndexReader(c, capability.SpannerPostgres())
			indexes, err := reader.readIndexesForSchema("public")
			c.Assert(err, qt.IsNil)

			found := indexNamed(indexes, tt.index)
			c.Assert(found, qt.IsNotNil, qt.Commentf("read %d index(es)", len(indexes)))
			c.Assert(found.Columns, qt.DeepEquals, tt.wantColumns)
			c.Assert(found.IncludeColumns, qt.DeepEquals, tt.wantInclude)
			c.Assert(found.IsUnique, qt.Equals, tt.wantUnique)
			c.Assert(found.IsPrimary, qt.Equals, tt.wantPrimary)
			c.Assert(found.Definition, qt.Equals, tt.wantDefinition)
			c.Assert(found.Parts[0].Desc, qt.Equals, tt.wantDesc)
			// The default schema is the empty string, as the tables spell it.
			// Spelling it "public" keyed every index "public.s" while the
			// tables keyed "s", so enhanceTablesWithIndexes matched nothing.
			c.Assert(found.Schema, qt.Equals, "")
		})
	}
}

// The branch is the point: a preset that has pg_catalog's helpers must keep
// asking pg_catalog, or this read would replace the PostgreSQL one for every
// server and drop everything it carries that the standard catalog does not --
// operator classes, expressions, partial-index predicates.
func TestReadIndexesForSchemaPicksTheCatalogTheServerHas(t *testing.T) {
	tests := []struct {
		name     string
		caps     capability.Capabilities
		standard bool
	}{
		{name: "spanner has no pg_catalog helpers", caps: capability.SpannerPostgres(), standard: true},
		{name: "postgres has them", caps: capability.Postgres17(), standard: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			asked := ""
			db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				asked = query
				return informationSchemaIndexRows(), nil
			})
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", tt.caps)

			_, _ = reader.readIndexesForSchema("public")

			c.Assert(strings.Contains(asked, "information_schema.indexes"), qt.Equals, tt.standard,
				qt.Commentf("query was:\n%s", asked))
			c.Assert(strings.Contains(asked, "pg_get_indexdef"), qt.Equals, !tt.standard)
		})
	}
}

// indexNamed returns the read index with this name, or nil.
func indexNamed(indexes []types.DBIndex, name string) *types.DBIndex {
	for position := range indexes {
		if indexes[position].Name == name {
			return &indexes[position]
		}
	}
	return nil
}
