package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// validatedCheckTable declares `t (id, n)` with the CHECK t_n_positive, which
// the table may hold NOT VALID or not.
func validatedCheckTable(notValid bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "n", Type: "int", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "T", Table: "t", Name: "t_n_positive", Type: "CHECK", CheckExpression: "n > 0",
			NotValid: notValid,
		}},
	}
}

// liveTableWithoutCheck is the table as a catalog reports it before the CHECK
// is added.
func liveTableWithoutCheck() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{
			{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "n", DataType: "int", IsNullable: "YES"},
		}}},
	}
}

// liveValidatedCheckTable is the same table with the CHECK, validated or not.
func liveValidatedCheckTable(notValid bool) *catalog.Database {
	clause := "n > 0"
	database := liveTableWithoutCheck()
	database.Constraints = []catalog.Constraint{{
		Name: "t_n_positive", TableName: "t", Type: "CHECK", CheckClause: &clause, NotValid: notValid,
	}}
	return database
}

// columnForeignKeyTable declares `p (id)` and `c (id, p_id)` with the key
// c_p_fkey written on the column, which has no room for NOT VALID.
func columnForeignKeyTable() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "P", Name: "p", PrimaryKey: []string{"id"}},
			{StructName: "C", Name: "c", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "p_id", Type: "int", Nullable: true, Foreign: "p(id)", ForeignKeyName: "c_p_fkey"},
		},
	}
}

// liveUnvalidatedForeignKey is the same pair as a catalog reports it, with the
// key NOT VALID.
func liveUnvalidatedForeignKey() *catalog.Database {
	parent, column, action := "p", "id", "NO ACTION"
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "p", Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true}}},
			{Name: "c", Columns: []catalog.Column{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "p_id", DataType: "int", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{{
			Name: "c_p_fkey", TableName: "c", Type: "FOREIGN KEY", ColumnName: "p_id", ColumnNames: []string{"p_id"},
			ForeignTable: &parent, ForeignColumn: &column, ForeignColumns: []string{"id"},
			DeleteRule: &action, UpdateRule: &action, NotValid: true,
		}},
	}
}

// TestCompare_NotValid_NoChange pairs a declaration with a constraint whose
// validation it accepts (stokaro/ptah#3853). A declaration that allows NOT
// VALID is satisfied by a validated constraint too: the rows it lets the
// server skip have passed the check already.
func TestCompare_NotValid_NoChange(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		live    *catalog.Database
	}{
		{name: "both NOT VALID", desired: validatedCheckTable(true), live: liveValidatedCheckTable(true)},
		{name: "NOT VALID allowed, validated held", desired: validatedCheckTable(true), live: liveValidatedCheckTable(false)},
		{name: "both validated", desired: validatedCheckTable(false), live: liveValidatedCheckTable(false)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.Postgres, test.desired, test.live)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
		})
	}
}

// TestCompare_NotValid_PlansAValidation plans VALIDATE CONSTRAINT for a
// constraint the database holds NOT VALID and the declaration holds validated,
// and nothing else: the constraint is neither dropped nor added.
func TestCompare_NotValid_PlansAValidation(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		live    *catalog.Database
		want    difftypes.ConstraintValidation
	}{
		{
			name: "a CHECK", desired: validatedCheckTable(false), live: liveValidatedCheckTable(true),
			want: difftypes.ConstraintValidation{TableName: "t", Name: "t_n_positive"},
		},
		{
			name: "a foreign key written on its column", desired: columnForeignKeyTable(), live: liveUnvalidatedForeignKey(),
			want: difftypes.ConstraintValidation{TableName: "c", Name: "c_p_fkey"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.Postgres, test.desired, test.live)

			c.Assert(diff.ConstraintsValidated, qt.DeepEquals, []difftypes.ConstraintValidation{test.want})
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompare_NotValid_AddsAMissingConstraintNotValid adds a constraint the
// declaration allows NOT VALID, and the addition carries the clause.
func TestCompare_NotValid_AddsAMissingConstraintNotValid(t *testing.T) {
	c := qt.New(t)

	diff := compareForDialect(platform.Postgres, validatedCheckTable(true), liveTableWithoutCheck())

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded[0].Name, qt.Equals, "t_n_positive")
	c.Assert(diff.ConstraintsAdded[0].NotValid, qt.IsTrue)
	c.Assert(diff.ConstraintsValidated, qt.HasLen, 0)
}

// TestCompare_NotValid_ARecreatedConstraintIsNotValidated recreates a NOT VALID
// CHECK whose condition changed. The drop and the add replace it, so no
// validation is planned beside them.
func TestCompare_NotValid_ARecreatedConstraintIsNotValidated(t *testing.T) {
	c := qt.New(t)
	desired := validatedCheckTable(false)
	desired.Constraints[0].CheckExpression = "n > 1"

	diff := compareForDialect(platform.Postgres, desired, liveValidatedCheckTable(true))

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
	c.Assert(diff.ConstraintsValidated, qt.HasLen, 0)
}

// TestCompareSchemas_ValidatesWhatTheOtherDeclarationLeavesNotValid compares
// two declarations, the comparison `schema diff` runs between two schema
// files: a CHECK the other side adds NOT VALID and this side declares
// validated is validated.
func TestCompareSchemas_ValidatesWhatTheOtherDeclarationLeavesNotValid(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareSchemas(t.Context(), validatedCheckTable(false), validatedCheckTable(true), platform.Postgres, must.Must(builtin.New())))

	c.Assert(diff.ConstraintsValidated, qt.DeepEquals,
		[]difftypes.ConstraintValidation{{TableName: "t", Name: "t_n_positive"}})
}
