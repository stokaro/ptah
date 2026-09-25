package postgres

// White-box testing required: the comment of a domain, a composite and a range
// type is a projection of readDomainsForSchema, readCompositesForSchema and
// readRangesForSchema, which are unexported, and the column is read only when
// the target resolves pg_catalog's helpers. The fake below answers the
// projection the query spells rather than a fixed row, so a read that stops
// asking for the comment reads none.

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// typeCommentProjection is the expression that reads a type's own comment,
// spelled here rather than read from the reader so a mutation of it is seen.
const typeCommentProjection = "obj_description(t.oid, 'pg_type')"

// commentedTypeServer answers the three type reads with one type each, whose
// comment is `note` wherever the query projects it.
func commentedTypeServer(tb interface{ Cleanup(func()) }, caps capability.Capabilities) *Reader {
	db := dbtest.Open(tb, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		comment := ""
		if strings.Contains(query, typeCommentProjection) {
			comment = "note"
		}
		switch {
		case strings.Contains(query, "c.conname AS constraint_name"):
			return dbtest.QueryResult{Columns: []string{"domain_name", "constraint_name", "constraint_expr"}}, nil
		case strings.Contains(query, "t.typtype = 'd'"):
			return dbtest.QueryResult{
				Columns: typeColumns("d"),
				Rows:    [][]driver.Value{{"public", "d", "integer", false, "", "", comment}},
			}, nil
		case strings.Contains(query, "t.typtype = 'c'"):
			return dbtest.QueryResult{
				Columns: typeColumns("c"),
				Rows:    [][]driver.Value{{"public", "c", "n", "integer", int64(1), comment}},
			}, nil
		default:
			return dbtest.QueryResult{
				Columns: typeColumns("r"),
				Rows:    [][]driver.Value{{"public", "r", "integer", "int4_ops", "", "", "", comment}},
			}, nil
		}
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", caps)
	reader.SetSchemas([]string{"public"})
	return reader
}

// typeComments reads the comment of each kind the server holds.
func typeComments(ctx context.Context, reader *Reader) ([]string, error) {
	domains, err := reader.readDomainsForSchema(ctx, "public")
	if err != nil {
		return nil, err
	}
	composites, err := reader.readCompositesForSchema(ctx, "public")
	if err != nil {
		return nil, err
	}
	ranges, err := reader.readRangesForSchema(ctx, "public")
	if err != nil {
		return nil, err
	}
	return []string{domains[0].Comment, composites[0].Comment, ranges[0].Comment}, nil
}

// A domain's, a composite's and a range's comment are read where the target
// resolves obj_description, and read as none where it does not
// (stokaro/ptah#3627).
func TestReadTypeComments(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want []string
	}{
		{name: "a target with the catalog helpers", caps: capability.Postgres18(), want: []string{"note", "note", "note"}},
		{
			name: "a target without them",
			caps: capability.Postgres18().With(capability.PostgresCatalogFunctions, false),
			want: []string{"", "", ""},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			comments, err := typeComments(t.Context(), commentedTypeServer(c, test.caps))

			c.Assert(err, qt.IsNil)
			c.Assert(comments, qt.DeepEquals, test.want)
		})
	}
}
