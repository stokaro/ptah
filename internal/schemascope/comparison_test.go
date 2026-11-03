package schemascope_test

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/schemascope"
)

// TestComparisonScope pins which names go to the reader of the live side of a
// comparison. The end-to-end property -- inspecting a two-schema database and
// applying its own description back plans nothing -- is covered live by
// TestPostgreSQLSchemaBoundaryGuardIntegration's `two_schemas_plain_url`
// fixture.
//
// The load-bearing rows are the ones where base and the document disagree. base
// is what schemascope.ReadNames said this URL covers; the document's own
// schemas are added on top rather than replacing it, because a URL pinned by
// search_path covers less than a document may name and a creation planned for
// an object that exists fails the run. A row that returned only the document's
// schemas would make a whole schema silently unmanaged, which is
// stokaro/ptah#1276 with the sign flipped.

// postgresPinned is a connection whose URL pins one schema, so its scope is a
// list of names that a document's schemas extend.
var postgresPinned = catalog.ServerInfo{
	Dialect: "postgres",
	URL:     "postgres://localhost/db?search_path=public",
	Schema:  "public",
}

// postgresRealm is a connection whose URL pins no schema, so its scope is
// every schema of the database, listed by name.
var postgresRealm = catalog.ServerInfo{Dialect: "postgres", URL: "postgres://localhost/db", Schema: "public"}

// ydbRoot is a YDB connection. It names no schema, because the database root
// has no name, and its reader's default is every directory.
var ydbRoot = catalog.ServerInfo{Dialect: "ydb", URL: "ydb://localhost:2136/local"}

func TestComparisonScope(t *testing.T) {
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		requested []string
		base      []string
		desired   *schemamodel.Database
		want      []string
	}{
		{
			name:      "explicit schemas outrank both the URL and the document",
			info:      postgresPinned,
			requested: []string{"only_this"},
			base:      []string{"extra", "public"},
			desired: &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "public"}, {Name: "extra"}},
			},
			want: []string{"only_this"},
		},
		{
			name:      "a comma-separated selection is split like the flag",
			info:      postgresPinned,
			requested: []string{"one,two"},
			base:      []string{"public"},
			desired:   &schemamodel.Database{},
			want:      []string{"one", "two"},
		},
		{
			name: "the URL's realm scope is read whether or not the document names it",
			info: postgresRealm,
			base: []string{"extra", "public"},
			desired: &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "public"}},
				Tables:  []schemamodel.Table{{Name: "a", Schema: "public"}},
			},
			want: []string{"extra", "public"},
		},
		{
			name: "a document qualifying nothing reads exactly the URL's scope",
			info: postgresPinned,
			base: []string{"public"},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "a"}},
			},
			want: []string{"public"},
		},
		{
			name: "a schema block beyond a pinned URL widens the read",
			info: postgresPinned,
			base: []string{"public"},
			desired: &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "extra"}, {Name: "public"}},
				Tables:  []schemamodel.Table{{Name: "a", Schema: "public"}, {Name: "b", Schema: "extra"}},
			},
			want: []string{"extra", "public"},
		},
		{
			name: "a qualified table alone widens the read",
			info: postgresPinned,
			base: []string{"public"},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "b", Schema: "extra"}},
			},
			want: []string{"extra", "public"},
		},
		{
			name: "declarations other than tables name schemas too",
			info: postgresPinned,
			base: []string{"public"},
			desired: &schemamodel.Database{
				Sequences: []schemamodel.Sequence{{Name: "s", Schema: "seqs"}},
				FeatureObjects: must.Must(schemaext.NewObjects(
					schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "doms", "d"), Value: &pgtype.DesiredDomain{BaseType: "text"}},
					schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "comps", "c"),
						Value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "integer"}}}},
					schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "rngs", "r"), Value: &pgtype.DesiredRange{}})),
			},
			want: []string{"comps", "doms", "public", "rngs", "seqs"},
		},
		{
			name: "a default privilege's schema may be its only appearance",
			info: postgresPinned,
			base: []string{"public"},
			desired: &schemamodel.Database{
				FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{
					Ref:   must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader"}, identifier.ForDialect("postgres"))),
					Value: &pgdefaultprivilege.Desired{},
				})),
			},
			want: []string{"app", "public"},
		},
		{
			name: "blank names are not schemas",
			info: postgresPinned,
			base: []string{"public"},
			desired: &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "  "}},
				Tables:  []schemamodel.Table{{Name: "a", Schema: ""}},
			},
			want: []string{"public"},
		},
		{
			name:    "a connection naming no schema and a document naming none reads the reader's default",
			info:    ydbRoot,
			base:    nil,
			desired: nil,
			want:    nil,
		},
		{
			// Reading only shop would miss the root table, and the plan would
			// create a table that is already there.
			name: "a YDB document declaring a directory beside the root still reads every directory",
			info: ydbRoot,
			base: nil,
			desired: &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "shop"}},
				Tables:  []schemamodel.Table{{Name: "kinds"}, {Name: "orders", Schema: "shop"}},
			},
			want: nil,
		},
		{
			name:      "an explicit directory on YDB is the scope",
			info:      ydbRoot,
			requested: []string{"shop"},
			base:      nil,
			desired:   &schemamodel.Database{Tables: []schemamodel.Table{{Name: "orders", Schema: "shop"}}},
			want:      []string{"shop"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			names, err := schemascope.ComparisonScope(tt.info, tt.requested, tt.base, tt.desired)
			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.DeepEquals, tt.want)
		})
	}
}

// reportingTable is a document declaring reporting.b and nothing in public.
var reportingTable = &schemamodel.Database{Tables: []schemamodel.Table{{Name: "b", Schema: "reporting"}}}

// TestComparisonReadNames_HappyPath reads, for a document declaring
// reporting.b, every schema the URL covers and reporting with them. A read of
// the connection's schema alone, public, finds no reporting.b, and the
// comparison plans `CREATE TABLE "reporting"."b"` for a table that exists
// (stokaro/ptah#5030).
func TestComparisonReadNames_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		info       catalog.ServerInfo
		requested  []string
		want       []string
		wantProbes int
	}{
		{name: "a URL pinning no schema reads the realm and the declared schemas", info: postgresRealm,
			want: []string{"extra", "public", "reporting"}, wantProbes: 1},
		{name: "a URL pinning a schema reads it and the declared schemas", info: postgresPinned,
			want: []string{"public", "reporting"}, wantProbes: 0},
		{name: "an explicit selection is the scope and asks the server nothing", info: postgresRealm,
			requested: []string{"public"}, want: []string{"public"}, wantProbes: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, realmProbeRows("public", "extra"))

			got, err := schemascope.ComparisonReadNames(t.Context(), test.info, test.requested, postgresConnection(db.SQL), reportingTable)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(db.QueryCount(), qt.Equals, test.wantProbes)
		})
	}
}

func TestComparisonReadNames_FailurePath(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("realm probe exploded")
	db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, failure
	})

	got, err := schemascope.ComparisonReadNames(t.Context(), postgresRealm, nil, postgresConnection(db.SQL), reportingTable)

	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(got, qt.IsNil)
}
