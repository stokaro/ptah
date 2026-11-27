package schemascope_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/schemascope"
)

// A selection meets a name as it is written, so a comparison pairs the names
// of both sides before it selects: a --schemas name, and the schemas and
// tables an --include or --exclude selector matches, are spelled as the
// target holds them, where the target reads the two spellings as one name
// (stokaro/ptah#6785).

// collation answers how a SQL Server database compares names the way one
// under a case-insensitive or a case-sensitive collation does.
type collation struct {
	insensitive bool
	err         error
	// unread answers a catalog-resolved collation that resolved no name,
	// which is not a reading of the collation.
	unread bool
}

func (c collation) ResolveIdentifierSemantics(_ context.Context, names []string) (identifier.Semantics, error) {
	if c.err != nil {
		return identifier.Semantics{}, c.err
	}
	if c.unread {
		return identifier.ForSQLServerCatalog("Latin1_General_100_CI_AS"), nil
	}
	names = slices.DeleteFunc(slices.Clone(names), func(name string) bool { return name == "" })
	slices.Sort(names)
	names = slices.Compact(names)
	class := func(name string) string {
		if c.insensitive {
			return strings.ToLower(name)
		}
		return name
	}
	keys := make(map[string]string, len(names))
	for _, name := range names {
		if _, seen := keys[class(name)]; !seen {
			keys[class(name)] = name
		}
	}
	resolved := make([]identifier.ResolvedName, 0, len(names))
	for _, name := range names {
		resolved = append(resolved, identifier.ResolvedName{Name: name, Key: keys[class(name)]})
	}
	return identifier.ForSQLServerCatalog("Latin1_General_100_CI_AS").WithResolvedNames(resolved), nil
}

var (
	mysqlFolds   = catalog.ServerInfo{Dialect: platform.MySQL, NameFolding: catalog.NameFoldingFolds}
	mysqlExact   = catalog.ServerInfo{Dialect: platform.MySQL, NameFolding: catalog.NameFoldingExact}
	mysqlUnknown = catalog.ServerInfo{Dialect: platform.MySQL}
	sqlServer    = catalog.ServerInfo{Dialect: platform.SQLServer, Schema: "dbo"}
)

// A --schemas name takes the spelling the target holds the schema in where the
// target reads the two as one, and keeps its own otherwise. The order and the
// repeats of the request are kept.
func TestTarget_SpellAsHeld_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		target    schemascope.Target
		requested []string
		held      []string
		want      []string
	}{
		{
			name: "a MySQL server that folds names", target: schemascope.Target{Info: mysqlFolds},
			requested: []string{"Sales", "archive", "SALES"}, held: []string{"sales", "archive"},
			want: []string{"sales", "archive", "sales"},
		},
		{
			name: "a MySQL server that keeps names apart", target: schemascope.Target{Info: mysqlExact},
			requested: []string{"Sales"}, held: []string{"sales"}, want: []string{"Sales"},
		},
		{
			name: "a MariaDB server whose folding no connection read, spelled as held", target: schemascope.Target{Info: catalog.ServerInfo{Dialect: platform.MariaDB}},
			requested: []string{"sales"}, held: []string{"sales", "Sales"}, want: []string{"sales"},
		},
		{
			name:      "a SQL Server database under a case-insensitive collation",
			target:    schemascope.Target{Info: sqlServer, Names: collation{insensitive: true}},
			requested: []string{"sales"}, held: []string{"dbo", "Sales"}, want: []string{"Sales"},
		},
		{
			name:      "a SQL Server database under a case-sensitive collation",
			target:    schemascope.Target{Info: sqlServer, Names: collation{}},
			requested: []string{"sales"}, held: []string{"dbo", "Sales"}, want: []string{"sales"},
		},
		{
			name:      "a PostgreSQL database, which compares names as bytes",
			target:    schemascope.Target{Info: catalog.ServerInfo{Dialect: platform.Postgres}, Names: collation{err: errors.New("asked")}},
			requested: []string{"sales"}, held: []string{"Sales"}, want: []string{"sales"},
		},
		{
			name: "a name the target does not hold", target: schemascope.Target{Info: sqlServer},
			requested: []string{"archive"}, held: []string{"Sales"}, want: []string{"archive"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := test.target.SpellAsHeld(t.Context(), test.requested, test.held)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A name that matches a held one only without regard to case is refused where
// no connection said how the target compares names, rather than selected
// apart from it: whether the two are one name decides what the plan drops.
func TestTarget_SpellAsHeld_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		target    schemascope.Target
		requested []string
		held      []string
		want      string
	}{
		{
			name: "a MySQL server whose folding no connection read", target: schemascope.Target{Info: mysqlUnknown},
			requested: []string{"Sales"}, held: []string{"sales"},
			want: `database "Sales" and "sales" differ only in case, and whether they name one database depends on ` +
				`the server's lower_case_table_names, .*`,
		},
		{
			name: "a SQL Server target no connection answers for", target: schemascope.Target{Info: sqlServer},
			requested: []string{"sales"}, held: []string{"Sales"},
			want: `schema "sales" and "Sales" differ only in case, and whether they name one schema depends on ` +
				`the database collation, .*`,
		},
		{
			// An answer that is not a complete reading counts as no answer.
			name: "a connection whose answer resolved no name", target: schemascope.Target{Info: sqlServer, Names: collation{unread: true}},
			requested: []string{"sales"}, held: []string{"Sales"},
			want: `schema "sales" and "Sales" differ only in case, and whether they name one schema depends on ` +
				`the database collation, .*`,
		},
		{
			name:      "two held spellings of one name",
			target:    schemascope.Target{Info: sqlServer, Names: collation{insensitive: true}},
			requested: []string{"sales"}, held: []string{"Sales", "SALES"},
			want: `schema "sales" matches "Sales" and "SALES", names the server reads as one under the database collation; no spelling is chosen`,
		},
		{
			name:      "a connection that does not answer",
			target:    schemascope.Target{Info: sqlServer, Names: collation{err: errors.New("login failed")}},
			requested: []string{"sales"}, held: []string{"Sales"},
			want: `compare schema names under the database collation: login failed`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := test.target.SpellAsHeld(t.Context(), test.requested, test.held)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

// salesDocument declares the table t in the schema schema, and salesRead holds
// the tables t and u in the schema schema.
func salesDocument(schema string) *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: schema}},
		Tables:  []schemamodel.Table{{Name: "t", Schema: schema}},
	}
}

func salesRead(schema string) *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: schema}},
		Tables:  []catalog.Table{{Name: "t", Schema: schema}, {Name: "u", Schema: schema}},
	}
}

// A selection between a document and a read spells the scope and the document
// as the target holds them, so a --schemas name keeps both sides of a schema.
// A target that keeps the names apart selects them apart.
func TestPairing_Selection_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		requested []string
		declared  string
		wantScope []string
		wantDB    string
	}{
		{
			name: "a MySQL server that folds names, scoped as the document spells it", info: mysqlFolds,
			requested: []string{"Sales"}, declared: "Sales", wantScope: []string{"sales"}, wantDB: "sales",
		},
		{
			name: "a MySQL server that folds names, scoped as the server spells it", info: mysqlFolds,
			requested: []string{"sales"}, declared: "Sales", wantScope: []string{"sales"}, wantDB: "sales",
		},
		{
			name: "a MySQL server that keeps names apart", info: mysqlExact,
			requested: []string{"sales"}, declared: "Sales", wantScope: []string{"sales"}, wantDB: "Sales",
		},
		{
			name: "no scope", info: mysqlFolds,
			declared: "Sales", wantDB: "sales",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := salesDocument(test.declared)

			scope, got, err := schemascope.PairingOf(test.info, nil).Selection(test.requested, desired, salesRead("sales"))

			c.Assert(err, qt.IsNil)
			c.Assert(scope, qt.DeepEquals, test.wantScope)
			c.Assert(got, qt.DeepEquals, salesDocument(test.wantDB))
			c.Assert(desired, qt.DeepEquals, salesDocument(test.declared))
		})
	}
}

// Under a resolved collation the SQL Server document is spelled as the
// database holds its schema, and under a case-sensitive one it is not.
func TestPairing_Selection_SQLServer(t *testing.T) {
	tests := []struct {
		name      string
		names     collation
		wantScope []string
		wantDB    string
	}{
		{name: "a case-insensitive collation", names: collation{insensitive: true}, wantScope: []string{"Sales"}, wantDB: "Sales"},
		{name: "a case-sensitive collation", names: collation{}, wantScope: []string{"sales"}, wantDB: "sales"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := schemascope.Target{Info: sqlServer, Names: test.names}
			desired, current := salesDocument("sales"), salesRead("Sales")

			pairing, err := target.Pairing(t.Context(), []string{"sales"}, desired, current)
			c.Assert(err, qt.IsNil)
			scope, got, err := pairing.Selection([]string{"sales"}, desired, current)

			c.Assert(err, qt.IsNil)
			c.Assert(scope, qt.DeepEquals, test.wantScope)
			c.Assert(got, qt.DeepEquals, salesDocument(test.wantDB))
		})
	}
}

// Where no connection said how the target compares names, a selection whose
// document spells a held schema in another case is refused, whatever the
// scope, rather than selecting either spelling alone.
func TestPairing_Selection_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		requested []string
		want      string
	}{
		{
			name: "a MySQL scope", info: mysqlUnknown, requested: []string{"Sales"},
			want: `database "Sales" and "sales" differ only in case, .*lower_case_table_names.*`,
		},
		{
			name: "a MySQL selector with no scope", info: mysqlUnknown,
			want: `database "Sales" and "sales" differ only in case, .*lower_case_table_names.*`,
		},
		{
			name: "a SQL Server scope", info: sqlServer, requested: []string{"sales"},
			want: `schema "Sales" and "sales" differ only in case, .*the database collation.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			scope, got, err := schemascope.PairingOf(test.info, nil).Selection(test.requested, salesDocument("Sales"), salesRead("sales"))

			var unknown *schemaprep.UnknownFoldingError
			c.Assert(err, qt.ErrorAs, &unknown)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(scope, qt.IsNil)
			c.Assert(got, qt.IsNil)
		})
	}
}

// A diff of two documents pairs the --to document against the --from one as it
// would against a read: with no connection to say how the target compares
// names, a schema the two spell in two cases is refused.
func TestPairing_DocumentSelection(t *testing.T) {
	t.Run("a MySQL server that folds names", func(t *testing.T) {
		c := qt.New(t)

		scope, got, err := schemascope.PairingOf(mysqlFolds, nil).DocumentSelection([]string{"Sales"}, salesDocument("Sales"), salesDocument("sales"))

		c.Assert(err, qt.IsNil)
		c.Assert(scope, qt.DeepEquals, []string{"sales"})
		c.Assert(got, qt.DeepEquals, salesDocument("sales"))
	})
	t.Run("a server no connection described", func(t *testing.T) {
		c := qt.New(t)

		scope, got, err := schemascope.PairingOf(mysqlUnknown, nil).DocumentSelection([]string{"sales"}, salesDocument("Sales"), salesDocument("sales"))

		c.Assert(err, qt.ErrorMatches, `database "Sales" and "sales" differ only in case, .*`)
		c.Assert(scope, qt.IsNil)
		c.Assert(got, qt.IsNil)
	})
}

// A pairing changes nothing where the target compares names as bytes.
func TestPairingOf_Exact(t *testing.T) {
	tests := []struct {
		name string
		info catalog.ServerInfo
		want bool
	}{
		{name: "PostgreSQL", info: catalog.ServerInfo{Dialect: platform.Postgres}, want: true},
		{name: "a MySQL server that keeps names apart", info: mysqlExact, want: true},
		{name: "a MySQL server that folds names", info: mysqlFolds, want: false},
		{name: "a MySQL server whose folding no connection read", info: mysqlUnknown, want: false},
		{name: "SQL Server", info: sqlServer, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemascope.PairingOf(test.info, nil).Exact(), qt.Equals, test.want)
		})
	}
}

// Respell spells the document as the target holds the names, and returns a
// document a target that compares names as bytes reads as it came.
func TestTarget_Respell(t *testing.T) {
	t.Run("a SQL Server database under a case-insensitive collation", func(t *testing.T) {
		c := qt.New(t)
		target := schemascope.Target{Info: sqlServer, Names: collation{insensitive: true}}

		got, err := target.Respell(t.Context(), salesDocument("sales"), salesRead("Sales"))

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, salesDocument("Sales"))
	})
	t.Run("PostgreSQL", func(t *testing.T) {
		c := qt.New(t)
		desired := salesDocument("sales")
		target := schemascope.Target{Info: catalog.ServerInfo{Dialect: platform.Postgres}, Names: collation{err: errors.New("asked")}}

		got, err := target.Respell(t.Context(), desired, salesRead("Sales"))

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, desired)
	})
}

// A --schemas request is read as the schemas the target holds under its rule:
// the realm is listed once, and the request is spelled as the list spells it.
func TestReadNames_SpellsARequestAsHeld(t *testing.T) {
	t.Run("a MySQL server that folds names", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(t, realmProbeRows("archive", "sales"))
		info := catalog.ServerInfo{Dialect: platform.MySQL, URL: "mysql://root@tcp(127.0.0.1:3306)/", NameFolding: catalog.NameFoldingFolds}

		got, err := schemascope.ReadNames(t.Context(), info, []string{"Sales", "sales"}, postgresConnection(db.SQL))

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"sales"})
		c.Assert(db.QueryCount(), qt.Equals, 1)
	})
	t.Run("a MySQL server whose folding no connection read", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(t, realmProbeRows("sales"))
		info := catalog.ServerInfo{Dialect: platform.MySQL, URL: "mysql://root@tcp(127.0.0.1:3306)/"}

		got, err := schemascope.ReadNames(t.Context(), info, []string{"Sales"}, postgresConnection(db.SQL))

		c.Assert(err, qt.ErrorMatches, `database "Sales" and "sales" differ only in case, .*`)
		c.Assert(got, qt.IsNil)
	})
}
