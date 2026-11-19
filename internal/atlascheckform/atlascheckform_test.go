package atlascheckform_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlascheckform"
)

// declaredChecks declares a CHECK on a column of t, two table-level CHECKs on
// t of which one repeats the column's expression, a UNIQUE key on t, and a
// CHECK on app.u written with space around it.
func declaredChecks() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "T", Name: "t"},
			{StructName: "U", Name: "u", Schema: "app"},
		},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "qty", Type: "int", Check: "qty > 0"},
			{StructName: "T", Name: "lo", Type: "int"},
			{StructName: "U", Name: "c", Type: "varchar(10)"},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "T", Name: "lo_ck", Type: "CHECK", CheckExpression: "lo < 10"},
			{StructName: "T", Name: "qty_ck", Type: "CHECK", CheckExpression: "qty > 0"},
			{StructName: "T", Name: "lo_uq", Type: "UNIQUE", Columns: []string{"lo"}},
			{StructName: "U", Name: "c_ck", Type: "check", CheckExpression: " c <> 'x' "},
		},
	}
}

// declaredProbes are the probes [declaredChecks] asks for on an engine whose
// server rewrites a CHECK.
var declaredProbes = []atlascheckform.Probe{
	{Table: "t", Expression: "qty > 0", Name: "ptah_check_probe_1"},
	{Table: "t", Expression: "lo < 10", Name: "ptah_check_probe_2"},
	{Schema: "app", Table: "u", Expression: "c <> 'x'", Name: "ptah_check_probe_3"},
}

func TestPlan(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []atlascheckform.Probe
	}{
		{name: "MySQL prints a CHECK as the server does", dialect: platform.MySQL, want: declaredProbes},
		{name: "MariaDB prints a CHECK as the server does", dialect: platform.MariaDB, want: declaredProbes},
		{name: "PostgreSQL prints a CHECK as declared", dialect: platform.Postgres},
		{name: "an unknown dialect prints a CHECK as declared", dialect: "nosuch"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlascheckform.Plan(test.dialect, declaredChecks(), addedChecks())

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A CHECK the diff does not write is left out, and the probes are numbered
// among themselves.
func TestPlan_OnlyWhatTheDiffWrites(t *testing.T) {
	c := qt.New(t)

	got := atlascheckform.Plan(platform.MySQL, declaredChecks(), &difftypes.SchemaDiff{
		ConstraintsAdded: difftypes.ConstraintAdditions{
			{Name: "c_ck", TableName: "app.u", Type: "CHECK", CheckExpression: "c <> 'x'"},
		},
	})

	c.Assert(got, qt.DeepEquals, []atlascheckform.Probe{
		{Schema: "app", Table: "u", Expression: "c <> 'x'", Name: "ptah_check_probe_1"},
	})
}

func TestPlan_NothingToAsk(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		diff    *difftypes.SchemaDiff
	}{
		{name: "no declaration", diff: addedChecks()},
		{name: "a declaration without a CHECK", desired: &schemamodel.Database{
			Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
			Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "int"}},
		}, diff: addedChecks()},
		{name: "no diff", desired: declaredChecks()},
		{name: "a diff that writes nothing", desired: declaredChecks(), diff: &difftypes.SchemaDiff{}},
		{name: "a diff that writes only an undeclared CHECK", desired: declaredChecks(), diff: &difftypes.SchemaDiff{
			ConstraintsAdded: difftypes.ConstraintAdditions{
				{Name: "other_ck", TableName: "app.u", Type: "CHECK", CheckExpression: "c <> 'y'"},
			},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlascheckform.Plan(platform.MySQL, test.desired, test.diff), qt.IsNil)
		})
	}
}

func TestTables(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "app"}, {Name: "other"}},
		Tables: []schemamodel.Table{
			{StructName: "T", Name: "t", Schema: "app"},
			{StructName: "V", Name: "v", Schema: "other"},
			{StructName: "W", Name: "t", Schema: "other"},
		},
		Fields: []schemamodel.Field{
			{StructName: "V", Name: "id", Type: "int", Primary: true},
			{StructName: "W", Name: "id", Type: "int", Check: "id > 0"},
			{StructName: "T", Name: "id", Type: "int", Primary: true, AutoInc: true},
			{
				StructName: "T", Name: "qty", Type: "int", Nullable: true, Collate: "utf8mb4_bin",
				Check: "qty > 0", CheckName: "qty_ck", Default: "1", DefaultSet: true,
				Foreign: "p(id)", ForeignKeyName: "qty_fk", OnDelete: "CASCADE",
			},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "T", Name: "pk", Type: "PRIMARY KEY", Columns: []string{"id"}},
			{StructName: "T", Name: "uq", Type: "unique", Columns: []string{"qty"}},
			{StructName: "T", Name: "ck", Type: "CHECK", CheckExpression: "qty < 9"},
			{StructName: "T", Name: "fk", Type: "FOREIGN KEY", Columns: []string{"qty"}, ForeignTable: "p"},
			{StructName: "V", Name: "v_pk", Type: "PRIMARY KEY", Columns: []string{"id"}},
		},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "ix", Fields: []string{"qty"}}},
		Views:   []schemamodel.View{{Name: "vw", Body: "SELECT 1"}},
		Roles:   []schemamodel.Role{{Name: "reader"}},
	}

	got := atlascheckform.Tables(desired, []atlascheckform.Probe{
		{Schema: "app", Table: "t", Expression: "qty > 0", Name: "ptah_check_probe_1"},
	})

	c.Assert(got, qt.DeepEquals, &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "app"}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: "t", Schema: "app"}},
		Enums:   make([]schemamodel.Enum, 0),
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "qty", Type: "int", Nullable: true, Collate: "utf8mb4_bin"},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "T", Name: "pk", Type: "PRIMARY KEY", Columns: []string{"id"}},
			{StructName: "T", Name: "uq", Type: "unique", Columns: []string{"qty"}},
		},
	})
	c.Assert(desired.Fields[3].Check, qt.Equals, "qty > 0")
	c.Assert(desired.Fields[3].Foreign, qt.Equals, "p(id)")
	c.Assert(desired.Fields[2].AutoInc, qt.IsTrue)
}

func TestTables_NothingToCreate(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlascheckform.Tables(nil, declaredProbes), qt.IsNil)
	c.Assert(atlascheckform.Tables(declaredChecks(), nil), qt.IsNil)
}

// devTables is a dev database holding t in the dev database and a table u in
// each of two others.
var devTables = &catalog.Database{Tables: []catalog.Table{
	{Schema: "dev", Name: "t"},
	{Schema: "other", Name: "u"},
	{Schema: "app", Name: "u"},
}}

func TestStatements_HappyPath(t *testing.T) {
	c := qt.New(t)

	got, err := atlascheckform.Statements(platform.MySQL, declaredProbes, devTables)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE `dev`.`t` ADD CONSTRAINT `ptah_check_probe_1` CHECK (qty > 0)",
		"ALTER TABLE `dev`.`t` ADD CONSTRAINT `ptah_check_probe_2` CHECK (lo < 10)",
		"ALTER TABLE `app`.`u` ADD CONSTRAINT `ptah_check_probe_3` CHECK (c <> 'x')",
	})
}

func TestStatements_UnqualifiedTable(t *testing.T) {
	c := qt.New(t)

	got, err := atlascheckform.Statements(platform.MariaDB, declaredProbes[:1],
		&catalog.Database{Tables: []catalog.Table{{Name: "t"}}})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"ALTER TABLE `t` ADD CONSTRAINT `ptah_check_probe_1` CHECK (qty > 0)"})
}

func TestStatements_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		probe   atlascheckform.Probe
		read    *catalog.Database
		wantErr string
	}{
		{
			name:    "the dev database does not hold the table",
			probe:   atlascheckform.Probe{Table: "w", Expression: "a > 0", Name: "ptah_check_probe_1"},
			read:    devTables,
			wantErr: `check probe ptah_check_probe_1: the dev database holds 0 tables named "w"`,
		},
		{
			name:    "two databases hold a table of that name and neither is the declared one",
			probe:   atlascheckform.Probe{Schema: "elsewhere", Table: "u", Expression: "a > 0", Name: "ptah_check_probe_1"},
			read:    devTables,
			wantErr: `check probe ptah_check_probe_1: the dev database holds 2 tables named "u"`,
		},
		{
			name:    "no read",
			probe:   atlascheckform.Probe{Table: "t", Expression: "a > 0", Name: "ptah_check_probe_1"},
			wantErr: `check probe ptah_check_probe_1: no dev database read`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlascheckform.Statements(platform.MySQL, []atlascheckform.Probe{test.probe}, test.read)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// probedClauses is a dev database read in which each probe of
// [declaredProbes] holds clause, beside a CHECK of the declaration's own.
func probedClauses(qty, lo, c string) *catalog.Database {
	return &catalog.Database{Constraints: []catalog.Constraint{
		{Name: "qty_ck", Type: "CHECK", CheckClause: new("(`qty` > 0)")},
		{Name: "PTAH_CHECK_PROBE_1", Type: "CHECK", CheckClause: new(qty)},
		{Name: "ptah_check_probe_2", Type: "CHECK", CheckClause: new(lo)},
		{Name: "ptah_check_probe_3", Type: "CHECK", CheckClause: new(c)},
	}}
}

// addedChecks is a diff that adds each CHECK of [declaredChecks] in one way
// a plan writes one, and one CHECK no probe asked about.
func addedChecks() *difftypes.SchemaDiff {
	desired := declaredChecks()
	return &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, identifier.ForDialect(platform.MySQL), "t"),
		TablesModified: []difftypes.TableDiff{{
			TableName:    "app.u",
			ColumnsAdded: difftypes.ColumnChanges{{StructName: "U", Name: "c", Type: "varchar(10)", Check: "c <> 'x'"}},
			Desired: schemacapture.TableDeclaration{
				Table:       schemamodel.Table{StructName: "U", Name: "u", Schema: "app"},
				Fields:      []schemamodel.Field{{StructName: "U", Name: "c", Type: "varchar(10)", Check: "c <> 'x'"}},
				Constraints: []schemamodel.Constraint{{StructName: "U", Name: "c_ck", Type: "CHECK", CheckExpression: " c <> 'x' "}},
			},
		}},
		ConstraintsAdded: difftypes.ConstraintAdditions{
			{Name: "c_ck", TableName: "app.u", Type: "CHECK", CheckExpression: " c <> 'x' "},
			{Name: "other_ck", TableName: "app.u", Type: "CHECK", CheckExpression: "c <> 'y'"},
		},
	}
}

func TestSpellingsRespell(t *testing.T) {
	c := qt.New(t)
	spellings, err := atlascheckform.Read(platform.MySQL, declaredProbes,
		probedClauses("(`qty` > 0)", "(`lo` < 10)", "(`c` <> _utf8mb4'x')"))
	c.Assert(err, qt.IsNil)
	declared := addedChecks()
	respelled := *declared

	spellings.Respell(&respelled)

	c.Assert(respelled.TablesAdded[0].Fields[1].Check, qt.Equals, "`qty` > 0")
	c.Assert(respelled.TablesAdded[0].Constraints[0].CheckExpression, qt.Equals, "`lo` < 10")
	c.Assert(respelled.TablesAdded[0].Constraints[1].CheckExpression, qt.Equals, "`qty` > 0")
	c.Assert(respelled.TablesAdded[0].Constraints[2].CheckExpression, qt.Equals, "")
	c.Assert(respelled.TablesModified[0].ColumnsAdded[0].Check, qt.Equals, "`c` <> _utf8mb4'x'")
	c.Assert(respelled.TablesModified[0].Desired.Fields[0].Check, qt.Equals, "`c` <> _utf8mb4'x'")
	c.Assert(respelled.TablesModified[0].Desired.Constraints[0].CheckExpression, qt.Equals, "`c` <> _utf8mb4'x'")
	c.Assert(respelled.ConstraintsAdded[0].CheckExpression, qt.Equals, "`c` <> _utf8mb4'x'")
	c.Assert(respelled.ConstraintsAdded[1].CheckExpression, qt.Equals, "c <> 'y'")
	c.Assert(declared.TablesAdded[0].Fields[1].Check, qt.Equals, "qty > 0")
	c.Assert(declared.TablesAdded[0].Constraints[0].CheckExpression, qt.Equals, "lo < 10")
	c.Assert(declared.TablesModified[0].ColumnsAdded[0].Check, qt.Equals, "c <> 'x'")
	c.Assert(declared.TablesModified[0].Desired.Constraints[0].CheckExpression, qt.Equals, " c <> 'x' ")
	c.Assert(declared.ConstraintsAdded[0].CheckExpression, qt.Equals, " c <> 'x' ")
}

func TestSpellingsRespell_ZeroValue(t *testing.T) {
	c := qt.New(t)
	respelled := addedChecks()

	atlascheckform.Spellings{}.Respell(respelled)
	atlascheckform.Spellings{}.Respell(nil)

	c.Assert(respelled, qt.DeepEquals, addedChecks())
}

func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		clause  string
		want    string
	}{
		{
			name:    "MySQL loses the parentheses around the whole clause",
			dialect: platform.MySQL,
			clause:  "(`qty` > 0)",
			want:    "`qty` > 0",
		},
		{
			name:    "MySQL keeps the parentheses around each operand",
			dialect: platform.MySQL,
			clause:  "((`id` between 1 and 10) and (`c` is not null))",
			want:    "(`id` between 1 and 10) and (`c` is not null)",
		},
		{
			name:    "MySQL keeps a first parenthesis that closes before the end",
			dialect: platform.MySQL,
			clause:  "(`a` > 0) and (`b` > 0)",
			want:    "(`a` > 0) and (`b` > 0)",
		},
		{
			name:    "MySQL reads a parenthesis inside a string as text",
			dialect: platform.MySQL,
			clause:  "(`c` <> _utf8mb4')(')",
			want:    "`c` <> _utf8mb4')('",
		},
		{
			name:    "MySQL reads an escaped quote inside a string as text",
			dialect: platform.MySQL,
			clause:  "(`c` <> _utf8mb4'it\\')s')",
			want:    "`c` <> _utf8mb4'it\\')s'",
		},
		{
			name:    "MySQL reads a parenthesis inside a quoted name as text",
			dialect: platform.MySQL,
			clause:  "(`a)` > 0)",
			want:    "`a)` > 0",
		},
		{
			name:    "MySQL keeps a clause whose parentheses do not balance",
			dialect: platform.MySQL,
			clause:  "((`a` > 0)",
			want:    "((`a` > 0)",
		},
		{
			name:    "MariaDB keeps the clause as reported",
			dialect: platform.MariaDB,
			clause:  "(`qty` > 0)",
			want:    "(`qty` > 0)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spellings, err := atlascheckform.Read(test.dialect, declaredProbes, probedClauses(test.clause, "x", "y"))
			c.Assert(err, qt.IsNil)
			respelled := addedChecks()

			spellings.Respell(respelled)

			c.Assert(respelled.TablesAdded[0].Fields[1].Check, qt.Equals, test.want)
		})
	}
}

func TestRead_NoProbe(t *testing.T) {
	c := qt.New(t)

	spellings, err := atlascheckform.Read(platform.MySQL, nil, nil)

	c.Assert(err, qt.IsNil)
	respelled := addedChecks()
	spellings.Respell(respelled)
	c.Assert(respelled, qt.DeepEquals, addedChecks())
}

func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		read *catalog.Database
	}{
		{name: "the probe's constraint is missing", read: &catalog.Database{Constraints: []catalog.Constraint{
			{Name: "ptah_check_probe_1", Type: "CHECK", CheckClause: new("(`qty` > 0)")},
		}}},
		{name: "no read", read: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlascheckform.Read(platform.MySQL, declaredProbes[1:2], test.read)

			c.Assert(err, qt.ErrorMatches, `check probe ptah_check_probe_2: the dev database reports no CHECK of that name`)
			respelled := addedChecks()
			got.Respell(respelled)
			c.Assert(respelled, qt.DeepEquals, addedChecks())
		})
	}
}
