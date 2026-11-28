package schemaremoval_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/schemaremoval"
)

// protectedSchema is a read whose database name holds an excluded table.
func protectedSchema(name string) *catalog.Database {
	return &catalog.Database{SchemaRemovalBlockers: map[string]catalog.SelectionBlocker{
		name: {Object: "table protected.t", Cause: catalog.SelectionExcluded},
	}}
}

// protectedTable is a read whose table app.u holds an excluded column.
func protectedTable() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Schema: "app", Name: "u"}, {Schema: "app", Name: "v"}},
		TableRemovalBlockers: map[string]catalog.SelectionBlocker{
			"app.u": {Object: "column app.u.c", Cause: catalog.SelectionExcluded},
		},
	}
}

func TestRefuseStatements_ProtectsExcludedObjectsFromSavedSchemaDrops(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql, schema string
		comparison                 identifier.Comparison
	}{
		{name: "database", dialect: "mysql", sql: "DROP DATABASE app", schema: "app"},
		{name: "schema synonym", dialect: "mariadb", sql: "DROP SCHEMA IF EXISTS `app`", schema: "app"},
		{name: "comments", dialect: "mysql", sql: "/* review */ DROP/* explanation */DATABASE app;", schema: "app"},
		{name: "executable comment", dialect: "mysql", sql: "/*!80000 DROP DATABASE app */", schema: "app"},
		{name: "MariaDB executable comment", dialect: "mariadb", sql: "/*M!100100 DROP DATABASE app */", schema: "app"},
		{name: "escaped backtick", dialect: "mysql", sql: "DROP DATABASE `odd``db`", schema: "odd`db"},
		{name: "literal dot", dialect: "mysql", sql: "DROP DATABASE `app.dot`", schema: "app.dot"},
		{name: "later statement", dialect: "mysql", sql: "SELECT 1; DROP DATABASE app", schema: "app"},
		{name: "folded database", dialect: "mysql", sql: "DROP DATABASE APP", schema: "app", comparison: identifier.ComparisonASCIIInsensitive},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			info := catalog.ServerInfo{Dialect: test.dialect, IdentifierSemantics: identifier.Semantics{TableNames: test.comparison}}

			err := schemaremoval.RefuseStatements([]string{test.sql}, protectedSchema(test.schema), info)

			c.Assert(err, qt.ErrorIs, schemaremoval.ErrRemovalBlocked)
			c.Assert(err, qt.ErrorMatches, `cannot drop database ".*": it would remove table protected.t, which the selection excludes; .*`)
		})
	}
}

// A saved plan's DROP TABLE of a table that now holds an excluded child is
// refused, whichever way the statement spells the table.
func TestRefuseStatements_ProtectsExcludedChildrenFromSavedTableDrops(t *testing.T) {
	for _, test := range []struct {
		name, dialect, connected, sql string
		comparison                    identifier.Comparison
	}{
		{name: "qualified", dialect: "mysql", sql: "DROP TABLE `app`.`u`"},
		{name: "in the connection's schema", dialect: "mysql", connected: "app", sql: "DROP TABLE `u`"},
		{name: "if exists", dialect: "postgres", sql: `DROP TABLE IF EXISTS "app"."u"`},
		{name: "second of a list", dialect: "postgres", sql: `DROP TABLE app.v, app.u CASCADE`},
		{name: "bracketed", dialect: "sqlserver", sql: "DROP TABLE [app].[u]"},
		{name: "folded", dialect: "mysql", sql: "DROP TABLE APP.U", comparison: identifier.ComparisonASCIIInsensitive},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			info := catalog.ServerInfo{
				Dialect: test.dialect, Schema: test.connected, IdentifierSemantics: identifier.Semantics{TableNames: test.comparison},
			}

			err := schemaremoval.RefuseStatements([]string{test.sql}, protectedTable(), info)

			c.Assert(err, qt.ErrorIs, schemaremoval.ErrRemovalBlocked)
			c.Assert(err, qt.ErrorMatches, `cannot drop table "app.u": it would remove column app.u.c, which the selection excludes; .*`)
		})
	}
}

func TestRefuseStatements_AllowsChangesOutsideProtectedSchemas(t *testing.T) {
	for _, statement := range []string{
		"DROP DATABASE other",
		"DROP DATABASE APP",
		"DROP TABLE app.u",
		"ALTER TABLE app.u ADD COLUMN note int",
		"SELECT 'DROP DATABASE app'",
		"/* DROP DATABASE app */ SELECT 1",
		"CREATE PROCEDURE p() BEGIN SELECT 'DROP DATABASE app'; END",
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			info := catalog.ServerInfo{Dialect: "mysql", IdentifierSemantics: identifier.Semantics{TableNames: identifier.ComparisonExact}}

			c.Assert(schemaremoval.RefuseStatements([]string{statement}, protectedSchema("app"), info), qt.IsNil)
		})
	}
}

func TestRefuseStatements_AllowsChangesOutsideProtectedTables(t *testing.T) {
	for _, test := range []struct {
		connected, sql string
	}{
		{sql: "DROP TABLE app.v"},
		{sql: "DROP TABLE other.u"},
		{connected: "other", sql: "DROP TABLE u"},
		{sql: "DROP TABLE APP.U"},
		{sql: "ALTER TABLE app.u DROP COLUMN note"},
		{sql: "SELECT 'DROP TABLE app.u'"},
		{sql: "DROP VIEW app.u"},
	} {
		t.Run(test.sql, func(t *testing.T) {
			c := qt.New(t)
			info := catalog.ServerInfo{
				Dialect: "mysql", Schema: test.connected, IdentifierSemantics: identifier.Semantics{TableNames: identifier.ComparisonExact},
			}

			c.Assert(schemaremoval.RefuseStatements([]string{test.sql}, protectedTable(), info), qt.IsNil)
		})
	}
}

func TestRefuseStatements_AllowsDropsWithoutExcludedObjects(t *testing.T) {
	for _, current := range []*catalog.Database{nil, {}} {
		c := qt.New(t)
		c.Assert(schemaremoval.RefuseStatements([]string{"DROP DATABASE app", "DROP TABLE app.u"}, current, catalog.ServerInfo{Dialect: "mysql"}), qt.IsNil)
	}
}

// Each cause names the ways out that work for it; a remedy that does not
// work for the cause, such as excluding the whole database when --include
// left the object out, is not offered.
func TestRefusalError_NamesTheRemediesOfItsCause(t *testing.T) {
	for _, test := range []struct {
		name    string
		removal difftypes.BlockedRemoval
		want    string
	}{
		{
			name: "excluded object in a database",
			removal: difftypes.BlockedRemoval{Kind: difftypes.BlockedDatabase, Name: "db",
				BlockedBy: catalog.SelectionBlocker{Object: "table db.t", Cause: catalog.SelectionExcluded}},
			want: `cannot drop database "db": it would remove table db.t, which the selection excludes; ` +
				`declare the database in the desired schema to keep it, or exclude the whole database (--exclude db) to leave it alone`,
		},
		{
			name: "excluded child of a table",
			removal: difftypes.BlockedRemoval{Kind: difftypes.BlockedTable, Name: "db.u",
				BlockedBy: catalog.SelectionBlocker{Object: "column db.u.c", Cause: catalog.SelectionExcluded}},
			want: `cannot drop table "db.u": it would remove column db.u.c, which the selection excludes; ` +
				`declare the table in the desired schema to keep it, or exclude the whole table (--exclude db.u) to leave it alone`,
		},
		{
			// On a run whose patterns count from the connection's schema, a
			// table in another schema is reached by its bare name.
			name: "excluded child of a table another schema holds",
			removal: difftypes.BlockedRemoval{Kind: difftypes.BlockedTable, Name: "s1.u",
				BlockedBy: catalog.SelectionBlocker{Object: "index s1.u.ix", Cause: catalog.SelectionExcluded, Pattern: "u"}},
			want: `cannot drop table "s1.u": it would remove index s1.u.ix, which the selection excludes; ` +
				`declare the table in the desired schema to keep it, or exclude the whole table (--exclude u) to leave it alone`,
		},
		{
			name: "child the include leaves out of a table",
			removal: difftypes.BlockedRemoval{Kind: difftypes.BlockedTable, Name: "db.u",
				BlockedBy: catalog.SelectionBlocker{Object: "trigger db.u.trg", Cause: catalog.SelectionNotIncluded, Pattern: "db.u"}},
			want: `cannot drop table "db.u": it would remove trigger db.u.trg, which --include does not select; ` +
				`declare the table in the desired schema to keep it, or select the whole table (--include db.u) to drop it`,
		},
		{
			name: "object the include leaves out",
			removal: difftypes.BlockedRemoval{Kind: difftypes.BlockedDatabase, Name: "db",
				BlockedBy: catalog.SelectionBlocker{Object: "table db.t", Cause: catalog.SelectionNotIncluded}},
			want: `cannot drop database "db": it would remove table db.t, which --include does not select; ` +
				`declare the database in the desired schema to keep it, or select everything in it (--include 'db.*') to drop it`,
		},
		{
			name: "kind an exclusion leaves unread",
			removal: difftypes.BlockedRemoval{Kind: difftypes.BlockedDatabase, Name: "old",
				BlockedBy: catalog.SelectionBlocker{Object: "routines", Cause: catalog.SelectionKindExcluded, Selector: "*[type=function]"}},
			want: `cannot drop database "old": --exclude "*[type=function]" keeps routines out of the read, so it is not known ` +
				`whether the database holds any; declare the database in the desired schema to keep it, exclude the database ` +
				`(--exclude old) to leave it alone, or run without that selector to drop it`,
		},
		{
			name: "kind an include leaves unread",
			removal: difftypes.BlockedRemoval{Kind: difftypes.BlockedDatabase, Name: "db",
				BlockedBy: catalog.SelectionBlocker{Object: "views", Cause: catalog.SelectionKindNotIncluded}},
			want: `cannot drop database "db": --include selects no views, so the read leaves them out and it is not known ` +
				`whether the database holds any; declare the database in the desired schema to keep it, or select everything ` +
				`in it (--include 'db.*') to drop it`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaremoval.RefuseBlocked(&difftypes.SchemaDiff{RemovalsBlocked: []difftypes.BlockedRemoval{test.removal}})

			c.Assert(err, qt.ErrorIs, schemaremoval.ErrRemovalBlocked)
			c.Assert(err.Error(), qt.Equals, test.want)
		})
	}
}

func TestRefuseBlocked_HappyPath(t *testing.T) {
	for _, diff := range []*difftypes.SchemaDiff{nil, {}, {SchemasRemoved: []string{"old"}}} {
		c := qt.New(t)
		c.Assert(schemaremoval.RefuseBlocked(diff), qt.IsNil)
	}
}

// The refusal names the first blocked drop, so a plan that blocks several
// names the same one on every run.
func TestRefuseBlocked_FailurePath(t *testing.T) {
	c := qt.New(t)
	blocker := catalog.SelectionBlocker{Object: "table db.t", Cause: catalog.SelectionExcluded}
	diff := &difftypes.SchemaDiff{RemovalsBlocked: []difftypes.BlockedRemoval{
		{Kind: difftypes.BlockedDatabase, Name: "a", BlockedBy: blocker},
		{Kind: difftypes.BlockedDatabase, Name: "b", BlockedBy: blocker},
	}}

	err := schemaremoval.RefuseBlocked(diff)

	var refusal *schemaremoval.RefusalError
	c.Assert(err, qt.ErrorAs, &refusal)
	c.Assert(refusal.Removal.Name, qt.Equals, "a")
}

func TestReport_WritesAWarningForEachBlockedDrop(t *testing.T) {
	c := qt.New(t)
	blocker := catalog.SelectionBlocker{Object: "column db.u.c", Cause: catalog.SelectionExcluded}
	var out strings.Builder

	schemaremoval.Report(&out, &difftypes.SchemaDiff{RemovalsBlocked: []difftypes.BlockedRemoval{
		{Kind: difftypes.BlockedTable, Name: "db.u", BlockedBy: blocker},
		{Kind: difftypes.BlockedTable, Name: "db.w", BlockedBy: blocker},
	}})

	c.Assert(out.String(), qt.Matches, `warning: cannot drop table "db.u": [^\n]*\nwarning: cannot drop table "db.w": [^\n]*\n`)
}

func TestReport_WritesNothingWithoutABlockedDrop(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder

	schemaremoval.Report(&out, &difftypes.SchemaDiff{SchemasRemoved: []string{"old"}})
	schemaremoval.Report(nil, &difftypes.SchemaDiff{RemovalsBlocked: []difftypes.BlockedRemoval{{Kind: difftypes.BlockedTable, Name: "db.u"}}})

	c.Assert(out.String(), qt.Equals, "")
}
