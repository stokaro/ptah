package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/migration/schemadiff"
)

// A table whose drop the selection blocks stays as the database holds it, so
// the drops of what it needs would fail: MySQL refuses `DROP TABLE t` with
// Error 3730 while the kept table's key references t, and PostgreSQL refuses
// `DROP TABLE t` and the `DROP FUNCTION` of its trigger function
// (stokaro/ptah#6847). The comparison keeps them too, and lists each one with
// the table that needs it.

// compareBlockingU compares desired with current after `--exclude u.c`, on a
// connection to schema of dialect.
func compareBlockingU(c *qt.C, dialect, schema string, desired *schemamodel.Database, current *catalog.Database) *difftypes.SchemaDiff {
	c.Helper()
	return compareExcludingIn(c, dialect, schema, "u.c", desired, current)
}

// compareExcludingIn compares desired with current after `--exclude exclude`,
// on a connection to schema of dialect.
func compareExcludingIn(c *qt.C, dialect, schema, exclude string, desired *schemamodel.Database, current *catalog.Database) *difftypes.SchemaDiff {
	c.Helper()
	runtime := must.Must(builtin.New())
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: dialect, Exclude: []string{exclude}, DefaultSchema: schema,
	})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, filtered, catalog.ServerInfo{Dialect: dialect, Schema: schema}, nil, runtime)
	c.Assert(err, qt.IsNil)
	return diff
}

// onlyV declares the table v alone.
func onlyV(schema string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Schema: schema, Name: "v", StructName: "v"}},
		Fields: []schemamodel.Field{{StructName: "v", Name: "id", Type: "int"}},
	}
}

// intColumns are columns of type int.
func intColumns(names ...string) []catalog.Column {
	columns := make([]catalog.Column, 0, len(names))
	for _, name := range names {
		columns = append(columns, catalog.Column{Name: name, DataType: "int"})
	}
	return columns
}

// foreignKey is the key name on table, over column, into referenced.
func foreignKey(schema, table, name, column, referenced string) catalog.Constraint {
	return catalog.Constraint{
		Schema: schema, TableName: table, Name: name, Type: "FOREIGN KEY", ColumnNames: []string{column},
		ForeignSchema: schema, ForeignTable: new(referenced), ForeignColumns: []string{"id"},
	}
}

// TestCompareWithDatabaseInfo_KeepsTheTablesABlockedTableReferences keeps t,
// which the blocked u references, and s, which t references in turn, and
// still drops w, which nothing kept references.
func TestCompareWithDatabaseInfo_KeepsTheTablesABlockedTableReferences(t *testing.T) {
	tests := []struct {
		dialect, schema string
		u, t, s, w      string
	}{
		{dialect: platform.MySQL, u: "u", t: "t", s: "s", w: "w"},
		{dialect: platform.MariaDB, u: "u", t: "t", s: "s", w: "w"},
		{dialect: platform.Postgres, schema: "public", u: "public.u", t: "public.t", s: "public.s", w: "public.w"},
		{dialect: platform.SQLServer, schema: "dbo", u: "dbo.u", t: "dbo.t", s: "dbo.s", w: "dbo.w"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{
				Tables: []catalog.Table{
					{Schema: test.schema, Name: "s", Columns: intColumns("id")},
					{Schema: test.schema, Name: "t", Columns: intColumns("id", "s_id")},
					{Schema: test.schema, Name: "u", Columns: intColumns("id", "c", "t_id")},
					{Schema: test.schema, Name: "v", Columns: intColumns("id")},
					{Schema: test.schema, Name: "w", Columns: intColumns("id")},
				},
				Constraints: []catalog.Constraint{
					foreignKey(test.schema, "t", "fk_ts", "s_id", "s"),
					foreignKey(test.schema, "u", "fk_ut", "t_id", "t"),
				},
			}

			diff := compareBlockingU(c, test.dialect, test.schema, onlyV(test.schema), current)

			c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, []string{test.w})
			blocker := catalog.SelectionBlocker{Object: "column " + test.u + ".c", Cause: catalog.SelectionExcluded, Pattern: "u"}
			c.Assert(diff.RemovalsBlocked, qt.DeepEquals, []difftypes.BlockedRemoval{
				{Kind: difftypes.BlockedTable, Name: test.u, BlockedBy: blocker},
				{Kind: difftypes.BlockedTable, Name: test.s, BlockedBy: blocker, NeededBy: test.t},
				{Kind: difftypes.BlockedTable, Name: test.t, BlockedBy: blocker, NeededBy: test.u},
			})
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfo_ListsATableTheSelectionBlocksOnce lists z once,
// as a drop the selection blocks, though the blocked u references it, and
// lists u once, though u references itself.
func TestCompareWithDatabaseInfo_ListsATableTheSelectionBlocksOnce(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "u", Columns: intColumns("id", "c", "z_id", "parent_id")},
			{Name: "v", Columns: intColumns("id")},
			{Name: "z", Columns: intColumns("id", "c")},
		},
		Constraints: []catalog.Constraint{
			foreignKey("", "u", "fk_uz", "z_id", "z"),
			foreignKey("", "u", "fk_uu", "parent_id", "u"),
		},
	}

	diff := compareExcludingIn(c, platform.MySQL, "", "*.c", onlyV(""), current)

	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.RemovalsBlocked, qt.DeepEquals, []difftypes.BlockedRemoval{
		{
			Kind: difftypes.BlockedTable, Name: "u",
			BlockedBy: catalog.SelectionBlocker{Object: "column u.c", Cause: catalog.SelectionExcluded, Pattern: "u"},
		},
		{
			Kind: difftypes.BlockedTable, Name: "z",
			BlockedBy: catalog.SelectionBlocker{Object: "column z.c", Cause: catalog.SelectionExcluded, Pattern: "z"},
		},
	})
}

// TestCompareWithDatabaseInfo_KeepsTheTableAnExcludedKeyReferences keeps t
// when the selection excludes the key into it: the key is out of the
// comparison's scope and still on the table that stays.
func TestCompareWithDatabaseInfo_KeepsTheTableAnExcludedKeyReferences(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "t", Columns: intColumns("id")},
			{Name: "u", Columns: intColumns("id", "t_id")},
			{Name: "v", Columns: intColumns("id")},
		},
		Constraints: []catalog.Constraint{foreignKey("", "u", "fk_ut", "t_id", "t")},
	}

	diff := compareExcludingIn(c, platform.MySQL, "", "u.fk_ut", onlyV(""), current)

	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.RemovalsBlocked, qt.DeepEquals, []difftypes.BlockedRemoval{
		{
			Kind: difftypes.BlockedTable, Name: "u",
			BlockedBy: catalog.SelectionBlocker{Object: "constraint u.fk_ut", Cause: catalog.SelectionExcluded, Pattern: "u"},
		},
		{
			Kind: difftypes.BlockedTable, Name: "t", NeededBy: "u",
			BlockedBy: catalog.SelectionBlocker{Object: "constraint u.fk_ut", Cause: catalog.SelectionExcluded, Pattern: "u"},
		},
	})
}

// TestCompareWithDatabaseInfo_KeepsTheDatabaseOfATableABlockedTableReferences
// keeps the database xo on a whole server: dropping it drops xo.t, which the
// blocked xa.u references, and MySQL refuses that with Error 3730.
func TestCompareWithDatabaseInfo_KeepsTheDatabaseOfATableABlockedTableReferences(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			current := &catalog.Database{
				Schemas: []catalog.Schema{{Name: "xa"}, {Name: "xo"}, {Name: "xz"}},
				Tables: []catalog.Table{
					{Schema: "xa", Name: "u", Columns: intColumns("id", "c", "t_id")},
					{Schema: "xa", Name: "v", Columns: intColumns("id")},
					{Schema: "xo", Name: "t", Columns: intColumns("id")},
					{Schema: "xz", Name: "z", Columns: intColumns("id")},
				},
				Constraints: []catalog.Constraint{foreignKey("xo", "u", "fk_ut", "t_id", "t")},
			}
			current.Constraints[0].Schema = "xa"
			desired := onlyV("xa")
			desired.Schemas = []schemamodel.Schema{{Name: "xa"}}
			filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
				Selections: runtime.Selections(), Dialect: dialect, RealmRelativePatterns: true, Exclude: []string{"xa.u.c"},
			})
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, filtered,
				catalog.ServerInfo{Dialect: dialect, WholeServer: true}, nil, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.SchemasRemoved, qt.DeepEquals, []string{"xz"})
			blocker := catalog.SelectionBlocker{Object: "column xa.u.c", Cause: catalog.SelectionExcluded, Pattern: "xa.u"}
			c.Assert(diff.RemovalsBlocked, qt.DeepEquals, []difftypes.BlockedRemoval{
				{Kind: difftypes.BlockedTable, Name: "xa.u", BlockedBy: blocker},
				{Kind: difftypes.BlockedDatabase, Name: "xo", BlockedBy: blocker, NeededBy: "xa.u"},
				{Kind: difftypes.BlockedTable, Name: "xo.t", BlockedBy: blocker, NeededBy: "xa.u"},
			})
		})
	}
}

// TestCompareWithDatabaseInfo_LeavesANotDescribedDatabaseABlockedTableReferences
// reports nothing about xo.t when the desired schema records xo as not
// described: the comparison plans no drop there to withhold.
func TestCompareWithDatabaseInfo_LeavesANotDescribedDatabaseABlockedTableReferences(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "xa"}, {Name: "xo"}},
		Tables: []catalog.Table{
			{Schema: "xa", Name: "u", Columns: intColumns("id", "c", "t_id")},
			{Schema: "xa", Name: "v", Columns: intColumns("id")},
			{Schema: "xo", Name: "t", Columns: intColumns("id")},
		},
		Constraints: []catalog.Constraint{foreignKey("xo", "u", "fk_ut", "t_id", "t")},
	}
	current.Constraints[0].Schema = "xa"
	desired := onlyV("xa")
	desired.Schemas = []schemamodel.Schema{{Name: "xa"}}
	desired.NotDescribed = coverage.Set{}.WithObject(coverage.Schema, "xo")
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: platform.MySQL, RealmRelativePatterns: true, Exclude: []string{"xa.u.c"},
	})
	c.Assert(err, qt.IsNil)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, filtered,
		catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}, nil, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.RemovalsBlocked, qt.DeepEquals, []difftypes.BlockedRemoval{{
		Kind: difftypes.BlockedTable, Name: "xa.u",
		BlockedBy: catalog.SelectionBlocker{Object: "column xa.u.c", Cause: catalog.SelectionExcluded, Pattern: "xa.u"},
	}})
	c.Assert(diff.SchemasRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
}

// TestCompareWithDatabaseInfo_LeavesADeclaredTableABlockedTableReferences
// compares t as declared: the desired schema keeps it, so it is no drop the
// selection withholds.
func TestCompareWithDatabaseInfo_LeavesADeclaredTableABlockedTableReferences(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "t", Columns: intColumns("id")},
			{Name: "u", Columns: intColumns("id", "c", "t_id")},
			{Name: "v", Columns: intColumns("id")},
		},
		Constraints: []catalog.Constraint{foreignKey("", "u", "fk_ut", "t_id", "t")},
	}
	desired := onlyV("")
	desired.Tables = append(desired.Tables, schemamodel.Table{Name: "t", StructName: "t"})
	desired.Fields = append(desired.Fields, schemamodel.Field{StructName: "t", Name: "id", Type: "int"}, schemamodel.Field{StructName: "t", Name: "e", Type: "int"})

	diff := compareBlockingU(c, platform.MySQL, "", desired, current)

	c.Assert(diff.RemovalsBlocked, qt.HasLen, 1)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].TableName, qt.Equals, "t")
}

// heldTriggerFunctionAndType is a PostgreSQL read of public where two triggers
// of u run the trigger function trgfn, two columns of u have the enum type
// mood, and a column of u is an array of the enum type tag. Beside them are an overload of trgfn that
// takes an argument, the function other_fn, which a trigger of the declared
// table v runs, and the type other_mood: no table kept needs any of them.
func heldTriggerFunctionAndType() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Schema: "public", Name: "u", Columns: []catalog.Column{
				{Name: "id", DataType: "int"}, {Name: "c", DataType: "int"},
				{Name: "m", DataType: "USER-DEFINED", UDTName: "mood", UDTSchema: "public"},
				{Name: "m2", DataType: "USER-DEFINED", UDTName: "mood", UDTSchema: "public"},
				{Name: "tags", DataType: "ARRAY", UDTName: "_tag", UDTSchema: "public"},
			}},
			{Schema: "public", Name: "v", Columns: intColumns("id")},
		},
		Triggers: []catalog.Trigger{
			{Schema: "public", Table: "u", Name: "trg", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW", ExecuteFunction: "trgfn"},
			{Schema: "public", Table: "u", Name: "trg2", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW", ExecuteFunction: "trgfn"},
			{Schema: "public", Table: "v", Name: "trv", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW", ExecuteFunction: "other_fn"},
		},
		Functions: []catalog.Function{
			{Name: "trgfn", Returns: "trigger", Language: "plpgsql", Body: "BEGIN RETURN NEW; END", IdentityArguments: new("")},
			{Name: "trgfn", Parameters: "n integer", Returns: "integer", Language: "sql", Body: "SELECT n", IdentityArguments: new("integer")},
			{Name: "other_fn", Returns: "trigger", Language: "plpgsql", Body: "BEGIN RETURN NEW; END", IdentityArguments: new("")},
		},
		Enums: []catalog.Enum{
			{Name: "mood", Values: []string{"ok"}}, {Name: "tag", Values: []string{"ok"}}, {Name: "other_mood", Values: []string{"ok"}},
		},
	}
}

// TestCompareWithDatabaseInfo_KeepsTheFunctionAndTypeABlockedTableNeeds keeps
// the function u's trigger executes and the enum types its columns have, and
// still drops the functions and the type nothing kept needs. The trigger is
// kept when the selection excludes it too.
func TestCompareWithDatabaseInfo_KeepsTheFunctionAndTypeABlockedTableNeeds(t *testing.T) {
	tests := []struct{ exclude, object string }{
		{exclude: "u.c", object: "column public.u.c"},
		{exclude: "u.trg", object: "trigger public.u.trg"},
	}
	for _, test := range tests {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)

			diff := compareExcludingIn(c, platform.Postgres, "public", test.exclude, onlyV("public"), heldTriggerFunctionAndType())

			blocker := catalog.SelectionBlocker{Object: test.object, Cause: catalog.SelectionExcluded, Pattern: "u"}
			c.Assert(diff.RemovalsBlocked, qt.DeepEquals, []difftypes.BlockedRemoval{
				{Kind: difftypes.BlockedTable, Name: "public.u", BlockedBy: blocker},
				{Kind: difftypes.BlockedFunction, Name: "trgfn", BlockedBy: blocker, NeededBy: "public.u"},
				{Kind: difftypes.BlockedType, Name: "mood", BlockedBy: blocker, NeededBy: "public.u"},
				{Kind: difftypes.BlockedType, Name: "tag", BlockedBy: blocker, NeededBy: "public.u"},
			})
			c.Assert(diff.FunctionsRemoved.Names(), qt.DeepEquals, []string{"other_fn", "trgfn"})
			c.Assert(diff.EnumsRemoved.Names(), qt.DeepEquals, []string{"other_mood"})
		})
	}
}

// TestCompareWithDatabaseInfo_ComparesAFunctionTheDesiredSchemaNames leaves a
// function the desired schema declares under the trigger function's name in
// the comparison, in another schema too: taken out, it would read as missing
// and the plan would create it again.
func TestCompareWithDatabaseInfo_ComparesAFunctionTheDesiredSchemaNames(t *testing.T) {
	tests := []struct{ name, declared string }{
		{name: "same schema", declared: "trgfn"},
		{name: "another schema", declared: "other.trgfn"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := onlyV("public")
			desired.Functions = []schemamodel.Function{{
				Name: test.declared, Returns: "trigger", Language: "plpgsql", Body: "BEGIN RETURN NEW; END",
			}}

			diff := compareBlockingU(c, platform.Postgres, "public", desired, heldTriggerFunctionAndType())

			kinds := make([]string, 0, len(diff.RemovalsBlocked))
			for _, removal := range diff.RemovalsBlocked {
				kinds = append(kinds, removal.Kind)
			}
			c.Assert(kinds, qt.DeepEquals, []string{difftypes.BlockedTable, difftypes.BlockedType, difftypes.BlockedType})
		})
	}
}

// TestCompareWithDatabaseInfo_ComparesATypeTheDesiredSchemaNames leaves an
// enum type the desired schema declares in the comparison: taken out, it
// would read as missing and the plan would create it again.
func TestCompareWithDatabaseInfo_ComparesATypeTheDesiredSchemaNames(t *testing.T) {
	c := qt.New(t)
	desired := onlyV("public")
	desired.Enums = []schemamodel.Enum{{Name: "mood", Values: []string{"ok"}}}

	diff := compareBlockingU(c, platform.Postgres, "public", desired, heldTriggerFunctionAndType())

	names := make([]string, 0, len(diff.RemovalsBlocked))
	for _, removal := range diff.RemovalsBlocked {
		names = append(names, removal.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"public.u", "trgfn", "tag"})
	c.Assert(diff.EnumsAdded, qt.HasLen, 0)
}
