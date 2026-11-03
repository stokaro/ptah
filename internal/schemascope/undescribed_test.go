package schemascope_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/schemascope"
)

// The desired schema these tests compare with declares public.users, and the
// database also holds crm and audit, which it declares nothing in. A URL that
// pins no search_path reads all three, and the comparison removes the tables
// of crm and audit (stokaro/ptah#5181). The rule names those two schemas, and
// nothing in any of the cases where the operator named the scope or nothing is
// removed.

// publicUsers is a document that qualifies its one table with public.
var publicUsers = &schemamodel.Database{Tables: []schemamodel.Table{{Name: "users", Schema: "public"}}}

// removing is a diff that removes each named table, given as schema.table or
// as a bare name; the schema half is what the read captured for it.
func removing(qualified ...string) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	for _, name := range qualified {
		schema, table, qualifiedName := strings.Cut(name, ".")
		if !qualifiedName {
			schema, table = "", name
		}
		diff.TablesRemoved = append(diff.TablesRemoved, difftypes.TableRemoval{
			Name:    name,
			Current: schemacapture.TableObservation{Table: catalog.Table{Schema: schema, Name: table}},
		})
	}
	return diff
}

// mysqlServer is a MySQL connection whose URL names no database, the realm
// scope of a dialect that selects a database rather than a search_path.
var mysqlServer = catalog.ServerInfo{Dialect: "mysql", URL: "mysql://localhost:3306/"}

func TestUndescribedRemovals_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		requested []string
		desired   *schemamodel.Database
		diff      *difftypes.SchemaDiff
		want      []schemascope.Undescribed
	}{
		{
			name:    "each schema the document names nothing in is counted, in name order",
			info:    postgresRealm,
			desired: publicUsers,
			diff:    removing("crm.orders", "audit.log", "crm.customers"),
			want:    []schemascope.Undescribed{{Schema: "audit", Tables: 1}, {Schema: "crm", Tables: 2}},
		},
		{
			// An unqualified declaration lives in the connection's schema,
			// which every scope reads, so a removal there is not the realm's.
			name:    "the connection's own schema is not reported",
			info:    postgresRealm,
			desired: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "users"}}},
			diff:    removing("public.legacy", "crm.customers"),
			want:    []schemascope.Undescribed{{Schema: "crm", Tables: 1}},
		},
		{
			name:      "an explicit selection is the operator naming the scope",
			info:      postgresRealm,
			requested: []string{"public,crm"},
			desired:   publicUsers,
			diff:      removing("crm.customers"),
			want:      nil,
		},
		{
			name:    "a URL that pins search_path reads no schema nobody named",
			info:    postgresPinned,
			desired: publicUsers,
			diff:    removing("crm.customers"),
			want:    nil,
		},
		{
			name:    "a schema the document declares is not undescribed",
			info:    postgresRealm,
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "crm"}}, Tables: publicUsers.Tables},
			diff:    removing("crm.customers"),
			want:    nil,
		},
		{
			name: "a default privilege names its schema",
			info: postgresRealm,
			desired: &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{
				Ref:   must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "o", Schema: "crm", ObjectType: "TABLES", Grantee: "r"}, identifier.ForDialect("postgres"))),
				Value: &pgdefaultprivilege.Desired{},
			}))},
			diff: removing("crm.customers"),
			want: nil,
		},
		{
			name:    "nothing removed is nothing to warn about",
			info:    postgresRealm,
			desired: publicUsers,
			diff:    &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "crm.customers"}}},
			want:    nil,
		},
		{
			name:    "no diff is nothing to warn about",
			info:    postgresRealm,
			desired: publicUsers,
			diff:    nil,
			want:    nil,
		},
		{
			name:    "a removal whose captured table names no schema is the default's",
			info:    postgresRealm,
			desired: publicUsers,
			diff:    removing("customers"),
			want:    nil,
		},
		{
			name:    "a YDB read names no schema nobody named",
			info:    ydbRoot,
			desired: publicUsers,
			diff:    removing("crm.customers"),
			want:    nil,
		},
		{
			name:    "a server-wide MySQL read selects databases, not a search_path",
			info:    mysqlServer,
			desired: publicUsers,
			diff:    removing("crm.customers"),
			want:    nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemascope.UndescribedRemovals(test.info, test.requested, test.desired, test.diff)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestUndescribedWarning(t *testing.T) {
	tests := []struct {
		name   string
		schema schemascope.Undescribed
		want   string
	}{
		{
			name:   "one table",
			schema: schemascope.Undescribed{Schema: "crm", Tables: 1},
			want: `the desired schema declares nothing in schema "crm", so its 1 table is planned for removal; ` +
				`to keep it, pin search_path in the URL or pass --schemas`,
		},
		{
			name:   "several tables",
			schema: schemascope.Undescribed{Schema: "crm", Tables: 2},
			want: `the desired schema declares nothing in schema "crm", so its 2 tables are planned for removal; ` +
				`to keep them, pin search_path in the URL or pass --schemas`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.schema.Warning(), qt.Equals, test.want)
		})
	}
}

// TestWarnUndescribed writes one line per schema, in the order given, and
// writes nothing for nothing.
func TestWarnUndescribed(t *testing.T) {
	tests := []struct {
		name  string
		found []schemascope.Undescribed
		want  string
	}{
		{
			name:  "one line per schema",
			found: []schemascope.Undescribed{{Schema: "audit", Tables: 1}, {Schema: "crm", Tables: 2}},
			want: "warning: " + schemascope.Undescribed{Schema: "audit", Tables: 1}.Warning() + "\n" +
				"warning: " + schemascope.Undescribed{Schema: "crm", Tables: 2}.Warning() + "\n",
		},
		{name: "nothing found", found: nil, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out strings.Builder

			schemascope.WarnUndescribed(&out, test.found)

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestWarnUndescribedToNoStream is a caller without a diagnostics stream.
func TestWarnUndescribedToNoStream(t *testing.T) {
	c := qt.New(t)
	c.Assert(func() { schemascope.WarnUndescribed(nil, []schemascope.Undescribed{{Schema: "crm", Tables: 1}}) }, qt.Not(qt.PanicMatches), ".*")
}
