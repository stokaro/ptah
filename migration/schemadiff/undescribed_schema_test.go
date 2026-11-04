package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A desired schema that records `ptah:not-described schema "crm"` says nothing
// about what crm holds, so nothing in crm is read as gone. The table removal
// honored the record, and every other kind ignored it: against a crm table with
// a primary key, an index and a trigger, an enum and a function, the plan kept
// the table and still dropped its key, its index, its trigger, the enum (with
// CASCADE, taking the column's type) and the function, measured on PostgreSQL
// 18. Each row below is one removal list, asked of the same database twice:
// with the record, where it must be empty, and without it, where it must not
// be, so a row cannot pass by planning nothing at all.

// undescribedSchemaRow is one removal list a comparison fills.
type undescribedSchemaRow struct {
	name string
	// read pulls the list out of the diff, one string per removal.
	read func(*difftypes.SchemaDiff) []string
	// wantWithout is the list without the record. It is never empty.
	wantWithout []string
}

func undescribedSchemaRows() []undescribedSchemaRow {
	return []undescribedSchemaRow{
		{name: "tables", read: func(d *difftypes.SchemaDiff) []string { return d.TablesRemoved.Names() },
			wantWithout: []string{"crm.customers"}},
		{name: "enums", read: func(d *difftypes.SchemaDiff) []string { return undescribedEnumNames(d.EnumsRemoved) },
			wantWithout: []string{"crm.status"}},
		{name: "indexes", read: func(d *difftypes.SchemaDiff) []string { return undescribedIndexNames(d.IndexRemovals()) },
			wantWithout: []string{"crm.customers.customers_email_idx"}},
		{name: "constraints", read: func(d *difftypes.SchemaDiff) []string { return undescribedConstraintNames(d.ConstraintsRemoved) },
			wantWithout: []string{"crm.customers.customers_pkey"}},
		{name: "functions", read: func(d *difftypes.SchemaDiff) []string { return undescribedRoutineNames(d.FunctionsRemoved) },
			wantWithout: []string{"crm.add"}},
		{name: "procedures", read: func(d *difftypes.SchemaDiff) []string { return undescribedRoutineNames(d.ProceduresRemoved) },
			wantWithout: []string{"crm.noop"}},
		{name: "triggers", read: func(d *difftypes.SchemaDiff) []string { return undescribedTriggerNames(d.TriggersRemoved) },
			wantWithout: []string{"crm.customers.customers_touch"}},
		{name: "grants", read: func(d *difftypes.SchemaDiff) []string { return undescribedGrantNames(d.GrantsRemoved) },
			wantWithout: []string{"SCHEMA crm USAGE", "TABLE crm.customers SELECT"}},
		{name: "sequences", read: func(d *difftypes.SchemaDiff) []string { return d.SequencesRemoved.Names() },
			wantWithout: []string{"crm.ticket_seq"}},
		{name: "row-level security", read: func(d *difftypes.SchemaDiff) []string { return undescribedRLSTables(d.RLSEnabledTablesRemoved) },
			wantWithout: []string{"crm.customers"}},
	}
}

// undescribedSchemaDesired declares public.users and the role that holds
// grants in crm, so the grants are the schema's to manage.
func undescribedSchemaDesired(notDescribed coverage.Set) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", Schema: "public", StructName: "User"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "integer", Primary: true}},
		Roles:  []schemamodel.Role{{Name: "crm_reader"}},
		// The record the tests turn on and off.
		NotDescribed: notDescribed,
	}
}

// undescribedSchemaDatabase holds public.users and, in crm, one object of every
// kind a comparison removes.
func undescribedSchemaDatabase() *catalog.Database {
	id := []catalog.Column{{Name: "id", DataType: "integer", IsPrimaryKey: true}}
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "public"}, {Name: "crm"}},
		Tables: []catalog.Table{
			{Name: "users", Schema: "public", Type: "BASE TABLE", Columns: id},
			{Name: "customers", Schema: "crm", Type: "BASE TABLE", RLSEnabled: true, Columns: append(id,
				catalog.Column{Name: "email", DataType: "text"})},
		},
		Enums: []catalog.Enum{{Name: "status", Schema: "crm", Values: []string{"lead", "customer"}}},
		Indexes: []catalog.Index{
			{Name: "customers_email_idx", Schema: "crm", TableName: "customers", Columns: []string{"email"}},
		},
		Constraints: []catalog.Constraint{
			{Name: "customers_pkey", Schema: "crm", TableName: "customers", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
		},
		Functions: []catalog.Function{
			{Name: "add", Schema: "crm", Kind: "function", Parameters: "a integer, b integer", Returns: "integer", Language: "sql", Body: "SELECT a + b"},
			{Name: "noop", Schema: "crm", Kind: "procedure", Language: "sql", Body: "SELECT 1"},
		},
		Triggers: []catalog.Trigger{
			{Name: "customers_touch", Schema: "crm", Table: "customers", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW", ExecuteFunction: "crm.touch"},
		},
		Sequences: []catalog.Sequence{{Name: "ticket_seq", Schema: "crm", DataType: "bigint"}},
		Roles:     []catalog.Role{{Name: "crm_reader"}},
		Grants: []catalog.Grant{
			{Role: "crm_reader", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "crm"},
			{Role: "crm_reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "crm", ObjectName: "customers"},
		},
	}
}

func compareUndescribedSchema(c *qt.C, notDescribed coverage.Set) *difftypes.SchemaDiff {
	c.Helper()
	return compareUndescribedSchemaAs(c, "postgres", notDescribed)
}

// compareUndescribedSchemaAs compares under dialect. An empty dialect has no
// default schema, which takes the routine comparison down its other path.
func compareUndescribedSchemaAs(c *qt.C, dialect string, notDescribed coverage.Set) *difftypes.SchemaDiff {
	c.Helper()
	diff, _, err := schemadiff.CompareReportingUndecidedAdditions(c.Context(),
		undescribedSchemaDesired(notDescribed), undescribedSchemaDatabase(),
		&config.CompareOptions{Dialect: dialect}, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return diff
}

// TestUndescribedSchemaIsLeftWhole_HappyPath plans no removal of anything in
// crm when the desired schema records crm as not described.
func TestUndescribedSchemaIsLeftWhole_HappyPath(t *testing.T) {
	for _, row := range undescribedSchemaRows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareUndescribedSchema(c, coverage.Set{}.WithObject(coverage.Schema, "crm"))

			c.Assert(row.read(diff), qt.HasLen, 0)
		})
	}
}

// TestDescribedSchemaPlansEveryRemoval_HappyPath is the control: the same
// database without the record plans every removal the row reads.
func TestDescribedSchemaPlansEveryRemoval_HappyPath(t *testing.T) {
	for _, row := range undescribedSchemaRows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareUndescribedSchema(c, coverage.Set{})

			c.Assert(row.read(diff), qt.DeepEquals, row.wantWithout)
		})
	}
}

// TestRecordNamingAnotherSchemaKeepsTheRemovals_HappyPath records a schema the
// database does not hold, so every removal in crm is still planned: the record
// covers the schema it names and nothing beside it.
func TestRecordNamingAnotherSchemaKeepsTheRemovals_HappyPath(t *testing.T) {
	for _, row := range undescribedSchemaRows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareUndescribedSchema(c, coverage.Set{}.WithObject(coverage.Schema, "billing"))

			c.Assert(row.read(diff), qt.DeepEquals, row.wantWithout)
		})
	}
}

// TestUndescribedSchemaKeepsRoutinesWithoutADialect_HappyPath compares with no
// dialect, so no default schema, which pairs routines by name on a path of its
// own. The record keeps them there too.
func TestUndescribedSchemaKeepsRoutinesWithoutADialect_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		notDescribed   coverage.Set
		wantFunctions  []string
		wantProcedures []string
	}{
		{name: "recorded", notDescribed: coverage.Set{}.WithObject(coverage.Schema, "crm")},
		{name: "control", notDescribed: coverage.Set{}, wantFunctions: []string{"crm.add"}, wantProcedures: []string{"crm.noop"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareUndescribedSchemaAs(c, "", test.notDescribed)

			c.Assert(undescribedRoutineNames(diff.FunctionsRemoved), qt.DeepEquals, test.wantFunctions)
			c.Assert(undescribedRoutineNames(diff.ProceduresRemoved), qt.DeepEquals, test.wantProcedures)
		})
	}
}

// TestUndescribedConnectionSchemaIsLeftWhole_HappyPath records the
// connection's own schema. A reader leaves that schema off what it reports, so
// the record has to reach an object with no schema through the connection's
// default: public here.
func TestUndescribedConnectionSchemaIsLeftWhole_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		notDescribed coverage.Set
		want         []string
	}{
		{name: "recorded", notDescribed: coverage.Set{}.WithObject(coverage.Schema, "public"), want: nil},
		{name: "control", notDescribed: coverage.Set{}, want: []string{"legacy_idx"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables:       []schemamodel.Table{{Name: "users", StructName: "User"}},
				Fields:       []schemamodel.Field{{StructName: "User", Name: "id", Type: "integer", Primary: true}},
				NotDescribed: test.notDescribed,
			}
			database := &catalog.Database{
				Tables:  []catalog.Table{{Name: "users", Type: "BASE TABLE", Columns: []catalog.Column{{Name: "id", DataType: "integer", IsPrimaryKey: true}}}},
				Indexes: []catalog.Index{{Name: "legacy_idx", TableName: "users", Columns: []string{"id"}}},
			}

			diff, _, err := schemadiff.CompareReportingUndecidedAdditions(c.Context(), desired, database,
				&config.CompareOptions{Dialect: "postgres"}, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(undescribedIndexOnlyNames(diff.IndexRemovals()), qt.DeepEquals, test.want)
		})
	}
}

func undescribedEnumNames(enums difftypes.EnumChanges) []string {
	var names []string
	for _, enum := range enums {
		names = append(names, enum.QualifiedName())
	}
	return names
}

func undescribedIndexNames(refs []difftypes.IndexRef) []string {
	var names []string
	for _, ref := range refs {
		names = append(names, ref.TableName+"."+ref.Name)
	}
	return names
}

func undescribedIndexOnlyNames(refs []difftypes.IndexRef) []string {
	var names []string
	for _, ref := range refs {
		names = append(names, ref.Name)
	}
	return names
}

func undescribedConstraintNames(removals difftypes.ConstraintRemovals) []string {
	var names []string
	for _, removal := range removals {
		names = append(names, removal.TableName+"."+removal.Name)
	}
	return names
}

func undescribedRoutineNames(routines difftypes.FunctionChanges) []string {
	var names []string
	for _, routine := range routines {
		names = append(names, routine.Name)
	}
	return names
}

func undescribedTriggerNames(refs []difftypes.TriggerRef) []string {
	var names []string
	for _, ref := range refs {
		names = append(names, ref.TableName+"."+ref.TriggerName)
	}
	return names
}

func undescribedGrantNames(refs []difftypes.GrantRef) []string {
	var names []string
	for _, ref := range refs {
		names = append(names, ref.ObjectType+" "+ref.ObjectName+" "+ref.Privilege)
	}
	return names
}

func undescribedRLSTables(tables difftypes.RLSEnabledTableChanges) []string {
	var names []string
	for _, table := range tables {
		names = append(names, table.Table)
	}
	return names
}
