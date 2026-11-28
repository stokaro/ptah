package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/migration/schemadiff"
)

// An excluded object must survive a plan that removes the database containing
// it. Filtering its table alone cannot prevent DROP DATABASE from removing it,
// so the comparison does not plan the drop and reports it as blocked.
func TestCompareWithDatabaseInfo_BlocksTheDropOfADatabaseWithExcludedObjects(t *testing.T) {
	database := func(blocker catalog.SelectionBlocker) difftypes.BlockedRemoval {
		blocker.Pattern = "app"
		return difftypes.BlockedRemoval{Kind: difftypes.BlockedDatabase, Name: "app", BlockedBy: blocker}
	}
	excluded := func(object string) catalog.SelectionBlocker {
		return catalog.SelectionBlocker{Object: object, Cause: catalog.SelectionExcluded}
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range []struct {
			name    string
			current catalog.Database
			exclude string
			want    []difftypes.BlockedRemoval
		}{
			{
				name: "table", current: catalog.Database{Tables: []catalog.Table{{Schema: "app", Name: "t"}}}, exclude: "app.t",
				want: []difftypes.BlockedRemoval{database(excluded("table app.t"))},
			},
			{
				// The table is dropped on its own once the database is not, and
				// that drop would take the column as well.
				name:    "column",
				current: catalog.Database{Tables: []catalog.Table{{Schema: "app", Name: "t", Columns: []catalog.Column{{Name: "id", DataType: "int"}}}}},
				exclude: "app.t.id",
				want: []difftypes.BlockedRemoval{
					database(excluded("column app.t.id")),
					{Kind: difftypes.BlockedTable, Name: "app.t", BlockedBy: catalog.SelectionBlocker{
						Object: "column app.t.id", Cause: catalog.SelectionExcluded, Pattern: "app.t",
					}},
				},
			},
			{
				name: "view", current: catalog.Database{Views: []catalog.View{{Schema: "app", Name: "v"}}}, exclude: "app.v",
				want: []difftypes.BlockedRemoval{database(excluded("view app.v"))},
			},
			{
				name: "routine", current: catalog.Database{Functions: []catalog.Function{{Schema: "app", Name: "f"}}}, exclude: "app.f",
				want: []difftypes.BlockedRemoval{database(excluded("function app.f"))},
			},
			{
				name: "unread kind", exclude: "*[type=function]",
				want: []difftypes.BlockedRemoval{database(catalog.SelectionBlocker{
					Object: "routines", Cause: catalog.SelectionKindExcluded, Selector: "*[type=function]",
				})},
			},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				runtime := must.Must(builtin.New())
				current := test.current
				current.Schemas = []catalog.Schema{{Name: "app"}}
				filtered, err := atlasfilter.ScopeDatabase(&current, atlasfilter.Scope{
					Selections: runtime.Selections(), Dialect: dialect, RealmRelativePatterns: true, Exclude: []string{test.exclude},
				})
				c.Assert(err, qt.IsNil)

				diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &schemamodel.Database{}, filtered,
					catalog.ServerInfo{Dialect: dialect, WholeServer: true}, nil, runtime)

				c.Assert(err, qt.IsNil)
				c.Assert(diff.SchemasRemoved, qt.HasLen, 0)
				c.Assert(diff.RemovalsBlocked, qt.DeepEquals, test.want)
				c.Assert(diff.HasChanges(), qt.IsTrue)
			})
		}
	}
}

func TestCompareWithDatabaseInfo_KeepsExcludedDatabaseProtectionAcrossFilters(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}},
		Tables:  []catalog.Table{{Schema: "app", Name: "t"}, {Schema: "app", Name: "u"}},
	}
	scope := atlasfilter.Scope{Selections: runtime.Selections(), Dialect: platform.MySQL, RealmRelativePatterns: true, Exclude: []string{"app.t"}}
	filtered, err := atlasfilter.ScopeDatabase(current, scope)
	c.Assert(err, qt.IsNil)
	scope.Exclude = []string{"app.u"}
	filtered, err = atlasfilter.ScopeDatabase(filtered, scope)
	c.Assert(err, qt.IsNil)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &schemamodel.Database{}, filtered,
		catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}, nil, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.SchemasRemoved, qt.HasLen, 0)
	c.Assert(diff.RemovalsBlocked, qt.DeepEquals, []difftypes.BlockedRemoval{{
		Kind: difftypes.BlockedDatabase, Name: "app",
		BlockedBy: catalog.SelectionBlocker{Object: "table app.t", Cause: catalog.SelectionExcluded, Pattern: "app"},
	}})
	c.Assert(current.Tables, qt.HasLen, 2)
}

func TestCompareWithDatabaseInfo_ExcludedTableAllowsOtherTableRemoval(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{Schemas: []catalog.Schema{{Name: "app"}}, Tables: []catalog.Table{
		{Schema: "app", Name: "t"}, {Schema: "app", Name: "u"},
	}}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: platform.MySQL, RealmRelativePatterns: true, Exclude: []string{"app.t"},
	})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}}, filtered,
		catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.SchemasRemoved, qt.HasLen, 0)
	c.Assert(diff.RemovalsBlocked, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 1)
	c.Assert(diff.TablesRemoved[0].Name, qt.Equals, "app.u")
}

// A table the desired schema does not declare is still dropped with what it
// holds, unless the selection excluded part of it: dropping u would then
// remove the excluded child too, on every engine.
func TestCompareWithDatabaseInfo_BlocksTheDropOfATableWithAnExcludedChild(t *testing.T) {
	for _, test := range []struct {
		dialect, schema, declaredIn, exclude, table, object, pattern string
		databases                                                    []string
		realm, wholeServer                                           bool
	}{
		{
			dialect: platform.MySQL, schema: "app", declaredIn: "app", databases: []string{"app"}, realm: true, wholeServer: true,
			exclude: "app.u.c", table: "app.u", object: "column app.u.c", pattern: "app.u",
		},
		{
			dialect: platform.MariaDB, schema: "app", declaredIn: "app", databases: []string{"app"}, realm: true, wholeServer: true,
			exclude: "app.u.ix", table: "app.u", object: "index app.u.ix", pattern: "app.u",
		},
		{dialect: platform.MySQL, exclude: "u.c", table: "u", object: "column u.c", pattern: "u"},
		{dialect: platform.Postgres, schema: "public", exclude: "u.c", table: "public.u", object: "column public.u.c", pattern: "u"},
		{dialect: platform.SQLServer, schema: "dbo", exclude: "u.ix", table: "dbo.u", object: "index dbo.u.ix", pattern: "u"},
	} {
		t.Run(test.dialect+"/"+test.exclude, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			current := &catalog.Database{
				Tables: []catalog.Table{
					{Schema: test.schema, Name: "t", Columns: []catalog.Column{{Name: "id", DataType: "int"}}},
					{Schema: test.schema, Name: "u", Columns: []catalog.Column{{Name: "id", DataType: "int"}, {Name: "c", DataType: "int"}}},
				},
				Indexes: []catalog.Index{{Schema: test.schema, TableName: "u", Name: "ix", Columns: []string{"c"}}},
			}
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Schema: test.declaredIn, Name: "t"}},
				Fields: []schemamodel.Field{{StructName: "t", Name: "id", Type: "int"}},
			}
			for _, name := range test.databases {
				current.Schemas = append(current.Schemas, catalog.Schema{Name: name})
				desired.Schemas = append(desired.Schemas, schemamodel.Schema{Name: name})
			}
			filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
				Selections: runtime.Selections(), Dialect: test.dialect, RealmRelativePatterns: test.realm, Exclude: []string{test.exclude},
				DefaultSchema: test.schema,
			})
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, filtered,
				catalog.ServerInfo{Dialect: test.dialect, Schema: test.schema, WholeServer: test.wholeServer}, nil, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesRemoved, qt.HasLen, 0)
			c.Assert(diff.RemovalsBlocked, qt.DeepEquals, []difftypes.BlockedRemoval{{
				Kind: difftypes.BlockedTable, Name: test.table,
				BlockedBy: catalog.SelectionBlocker{Object: test.object, Cause: catalog.SelectionExcluded, Pattern: test.pattern},
			}})
		})
	}
}

// A table whose drop is blocked stays as the database holds it. Each part of
// it leaves the comparison with it: a key, a foreign key, a check, an index, a
// trigger, a policy, a grant and an owned sequence would each reach the diff as
// a removal of its own and strip the table the drop leaves in place.
func TestCompareWithDatabaseInfo_LeavesEveryPartOfABlockedTable(t *testing.T) {
	for _, test := range []struct {
		dialect, schema, exclude, table, pattern string
		realm, wholeServer                       bool
	}{
		{dialect: platform.MySQL, schema: "app", exclude: "app.u.c", table: "app.u", pattern: "app.u", realm: true, wholeServer: true},
		{dialect: platform.MariaDB, schema: "app", exclude: "app.u.c", table: "app.u", pattern: "app.u", realm: true, wholeServer: true},
		{dialect: platform.MySQL, exclude: "u.c", table: "u", pattern: "u"},
		{dialect: platform.Postgres, schema: "public", exclude: "u.c", table: "public.u", pattern: "u"},
		{dialect: platform.SQLServer, schema: "dbo", exclude: "u.c", table: "dbo.u", pattern: "u"},
	} {
		t.Run(test.dialect+"/"+test.exclude, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			check := "d > 0"
			current := &catalog.Database{
				Tables: []catalog.Table{
					{Schema: test.schema, Name: "t", Columns: []catalog.Column{{Name: "id", DataType: "int"}}},
					{Schema: test.schema, Name: "u", Columns: []catalog.Column{
						{Name: "id", DataType: "int"}, {Name: "c", DataType: "int"}, {Name: "d", DataType: "int"},
					}},
				},
				Constraints: []catalog.Constraint{
					{Schema: test.schema, TableName: "u", Name: "u_pk", Type: "PRIMARY KEY", ColumnNames: []string{"id"}},
					{
						Schema: test.schema, TableName: "u", Name: "u_d_fk", Type: "FOREIGN KEY", ColumnNames: []string{"d"},
						ForeignSchema: test.schema, ForeignTable: new("t"), ForeignColumns: []string{"id"},
					},
					{Schema: test.schema, TableName: "u", Name: "u_d_ck", Type: "CHECK", CheckClause: &check},
					{Schema: test.schema, TableName: "u", Name: "u_d_uq", Type: "UNIQUE", ColumnNames: []string{"d"}},
				},
				Indexes:     []catalog.Index{{Schema: test.schema, TableName: "u", Name: "ixd", Columns: []string{"d"}}},
				Triggers:    []catalog.Trigger{{Schema: test.schema, Table: "u", Name: "trg", Timing: "BEFORE", Event: "INSERT"}},
				RLSPolicies: []catalog.RLSPolicy{{Table: catalog.QualifyTableName(test.schema, "u"), Name: "p", PolicyFor: "SELECT"}},
				Roles:       []catalog.Role{{Name: "reader", Inherit: true}},
				Grants: []catalog.Grant{{
					Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: test.schema, ObjectName: "u",
				}},
				Sequences: []catalog.Sequence{{Schema: test.schema, Name: "u_id_seq", OwnedBy: "u.id"}},
			}
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Schema: test.schema, Name: "t", StructName: "t"}},
				Fields: []schemamodel.Field{{StructName: "t", Name: "id", Type: "int"}},
				// A role the desired schema manages has its grants compared, so
				// the grant on u would be revoked.
				Roles: []schemamodel.Role{{Name: "reader", Inherit: true}},
			}
			if test.wholeServer {
				current.Schemas = []catalog.Schema{{Name: test.schema}}
				desired.Schemas = []schemamodel.Schema{{Name: test.schema}}
			}
			filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
				Selections: runtime.Selections(), Dialect: test.dialect, RealmRelativePatterns: test.realm, Exclude: []string{test.exclude},
				DefaultSchema: test.schema,
			})
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, filtered,
				catalog.ServerInfo{Dialect: test.dialect, Schema: test.schema, WholeServer: test.wholeServer}, nil, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.RemovalsBlocked, qt.DeepEquals, []difftypes.BlockedRemoval{{
				Kind: difftypes.BlockedTable, Name: test.table,
				BlockedBy: catalog.SelectionBlocker{Object: "column " + test.table + ".c", Cause: catalog.SelectionExcluded, Pattern: test.pattern},
			}})
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
			c.Assert(diff.IndexesRemoved, qt.HasLen, 0)
			c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
			diff.RemovalsBlocked = nil
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
		})
	}
}

// A table the desired schema declares is compared as usual, whatever the
// selection leaves out of it: the blocker on it guards a drop that is not
// planned.
func TestCompareWithDatabaseInfo_ComparesADeclaredTableThatHoldsAnExcludedChild(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{Tables: []catalog.Table{
		{Name: "u", Columns: []catalog.Column{{Name: "id", DataType: "int"}, {Name: "c", DataType: "int"}}},
	}}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: platform.MySQL, Exclude: []string{"u.c"},
	})
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "u", StructName: "u"}},
		Fields: []schemamodel.Field{{StructName: "u", Name: "id", Type: "int"}, {StructName: "u", Name: "e", Type: "int"}},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, filtered, catalog.ServerInfo{Dialect: platform.MySQL}, nil, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(filtered.TableRemovalBlockers, qt.HasLen, 1)
	c.Assert(diff.RemovalsBlocked, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].TableName, qt.Equals, "u")
}

// A live table a facet defines, such as a SQLite virtual table, is not dropped
// for its absence alone, so a child the selection excludes blocks no drop and
// nothing is reported.
func TestCompareWithDatabaseInfo_ReportsNoBlockedDropForATableAFacetDefines(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{Tables: []catalog.Table{{
		Name: "docs", Type: "TABLE", Facets: observedVirtualTable("fts5", "title, body"),
		Columns: []catalog.Column{{Name: "title", DataType: "TEXT"}, {Name: "body", DataType: "TEXT"}},
	}}}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: platform.SQLite, Exclude: []string{"docs.title"},
	})
	c.Assert(err, qt.IsNil)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &schemamodel.Database{}, filtered,
		catalog.ServerInfo{Dialect: platform.SQLite}, nil, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(filtered.TableRemovalBlockers, qt.HasLen, 1)
	c.Assert(diff.RemovalsBlocked, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
}

// A feature an owner keeps past its table leaves the comparison with the
// blocked table too: a ClickHouse row policy is dropped on its own when its
// table is gone, so a policy on the kept table would otherwise be planned as
// DROP ROW POLICY.
func TestCompareWithDatabaseInfo_LeavesTheRowPolicyOfABlockedTable(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	policy := must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("", "u", "p"), chschema.ObservedRowPolicy{
		Filter: new("1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true, Names: make([]string, 0), Except: make([]string, 0)},
	}))
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "t", Columns: []catalog.Column{{Name: "id", DataType: "UInt64"}}},
			{Name: "u", Columns: []catalog.Column{{Name: "id", DataType: "UInt64"}, {Name: "c", DataType: "UInt64"}}},
		},
		FeatureObjects:  must.Must(schemaext.NewObjects(policy)),
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: platform.ClickHouse, Exclude: []string{"u.c"},
	})
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{
		Tables:          []schemamodel.Table{{Name: "t", StructName: "t"}},
		Fields:          []schemamodel.Field{{StructName: "t", Name: "id", Type: "UInt64"}},
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, filtered, catalog.ServerInfo{Dialect: platform.ClickHouse}, nil, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.RemovalsBlocked, qt.HasLen, 1)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
}

// Blocked drops are listed by name, whatever order the read listed the tables
// in.
func TestCompareWithDatabaseInfo_ListsBlockedTablesByName(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{Tables: []catalog.Table{
		{Schema: "public", Name: "z", Columns: []catalog.Column{{Name: "id", DataType: "int"}, {Name: "c", DataType: "int"}}},
		{Schema: "public", Name: "u", Columns: []catalog.Column{{Name: "id", DataType: "int"}, {Name: "c", DataType: "int"}}},
	}}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: platform.Postgres, Exclude: []string{"*.c"}, DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &schemamodel.Database{}, filtered,
		catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"}, nil, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.RemovalsBlocked, qt.HasLen, 2)
	c.Assert([]string{diff.RemovalsBlocked[0].Name, diff.RemovalsBlocked[1].Name}, qt.DeepEquals, []string{"public.u", "public.z"})
}

// A table in a database the desired schema records as not described is not a
// drop the comparison plans, so a child the selection excludes blocks
// nothing there and nothing is reported.
func TestCompareWithDatabaseInfo_ReportsNoBlockedDropWhereCoveragePlansNone(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}, {Name: "db"}},
		Tables: []catalog.Table{{Schema: "db", Name: "u", Columns: []catalog.Column{
			{Name: "id", DataType: "int"}, {Name: "c", DataType: "int"},
		}}},
	}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: platform.MySQL, RealmRelativePatterns: true, Exclude: []string{"db.u.c"},
	})
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{
		Schemas:      []schemamodel.Schema{{Name: "app"}},
		NotDescribed: coverage.Set{}.WithObject(coverage.Schema, "db"),
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, filtered,
		catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}, nil, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(filtered.TableRemovalBlockers, qt.HasLen, 1)
	c.Assert(diff.RemovalsBlocked, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
}

// A database the desired schema records as not described is left alone,
// whatever it holds: the record says the desired schema does not know what
// is there, so its absence asks for no drop.
func TestCompareWithDatabaseInfo_LeavesANotDescribedDatabaseAlone(t *testing.T) {
	for _, test := range []struct {
		name    string
		limited coverage.Set
		removed []string
	}{
		{name: "every undeclared database", limited: coverage.Set{}.WithKind(coverage.Schema)},
		{name: "the database by name", limited: coverage.Set{}.WithObject(coverage.Schema, "db"), removed: []string{"old"}},
	} {
		for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				runtime := must.Must(builtin.New())
				current := &catalog.Database{
					Schemas: []catalog.Schema{{Name: "app"}, {Name: "db"}, {Name: "old"}},
					Tables:  []catalog.Table{{Schema: "db", Name: "t", Columns: []catalog.Column{{Name: "id", DataType: "int"}}}},
				}
				desired := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}, NotDescribed: test.limited}

				diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current,
					catalog.ServerInfo{Dialect: dialect, WholeServer: true}, nil, runtime)

				c.Assert(err, qt.IsNil)
				c.Assert(diff.SchemasRemoved, qt.DeepEquals, test.removed)
				c.Assert(diff.TablesRemoved, qt.HasLen, 0)
				c.Assert(diff.RemovalsBlocked, qt.HasLen, 0)
			})
		}
	}
}
