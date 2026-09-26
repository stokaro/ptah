package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// commentedConstraints declares the table app.orders with a named CHECK
// constraint carrying comment, and a column-level CHECK, which has no place
// for a comment.
func commentedConstraints(check, comment string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders", Schema: "app"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "total", Type: "INTEGER", Check: "total < 100", CheckName: "orders_total_cap"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Order", Name: "orders_total_positive", Type: "CHECK", Table: "app.orders",
			CheckExpression: check, Comment: comment,
		}},
	}
}

// reportedConstraints is the database side of commentedConstraints as the
// PostgreSQL reader reports it, with comment on the named CHECK and on the
// column-level one and the primary key.
func reportedConstraints(comment string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "orders", Schema: "app", Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "total", DataType: "integer", IsNullable: "YES", OrdinalPosition: 2},
			},
		}},
		Constraints: []catalog.Constraint{
			{
				Name: "orders_pkey", TableName: "orders", Schema: "app", Type: "PRIMARY KEY",
				ColumnNames: []string{"id"}, ColumnName: "id", Comment: comment,
			},
			{
				Name: "orders_total_positive", TableName: "orders", Schema: "app", Type: "CHECK",
				CheckClause: new("total > 0"), Comment: comment,
			},
			{
				Name: "orders_total_cap", TableName: "orders", Schema: "app", Type: "CHECK",
				ColumnNames: []string{"total"}, ColumnName: "total", CheckClause: new("total < 100"), Comment: comment,
			},
		},
	}
}

// constraintCommentChange is the transition of the named CHECK from current to
// desired.
func constraintCommentChange(current, desired string) []difftypes.ConstraintCommentChange {
	return []difftypes.ConstraintCommentChange{{
		TableName: "app.orders", Name: "orders_total_positive", Current: current, Desired: desired,
	}}
}

// A declared constraint whose comment differs from the database's is a
// transition of its own, addressed by the name and table the database holds,
// and the constraint itself is not recreated (stokaro/ptah#3678). The primary
// key and the column-level CHECK come from forms that cannot declare a
// comment, so the comment the database holds on them is left alone.
func TestCompareWithDialect_ConstraintCommentDifferenceIsAChange(t *testing.T) {
	tests := []struct {
		name       string
		declared   string
		inDatabase string
		want       []difftypes.ConstraintCommentChange
	}{
		{name: "a comment rewritten", declared: "new", inDatabase: "old", want: constraintCommentChange("old", "new")},
		{name: "a comment added", declared: "new", inDatabase: "", want: constraintCommentChange("", "new")},
		{name: "a comment removed from the declaration", declared: "", inDatabase: "old", want: constraintCommentChange("old", "")},
		{name: "the same comment on both sides", declared: "same", inDatabase: "same", want: nil},
		{name: "a difference in surrounding space only", declared: "same", inDatabase: " same\n", want: nil},
		{name: "surrounding space on the declared side", declared: " same\n", inDatabase: "same", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				commentedConstraints("total > 0", test.declared), reportedConstraints(test.inDatabase), platform.Postgres,
			)

			c.Assert(diff.ConstraintCommentsChanged, qt.DeepEquals, test.want)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// A constraint whose definition changed is dropped and added again, and the
// statement that adds it writes the declared comment, so no comment
// transition is reported beside it.
func TestCompareWithDialect_RecreatedConstraintCarriesItsCommentInTheAddition(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(
		commentedConstraints("total > 1", "new"), reportedConstraints("old"), platform.Postgres,
	)

	c.Assert(diff.ConstraintCommentsChanged, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded[0].Comment, qt.Equals, "new")
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
}

// A constraint's comment is compared only where the target stores it and reads
// it back. The Spanner interface refuses COMMENT ON CONSTRAINT, and MySQL has
// no statement for it, so a declared comment there would be a difference no
// plan can close.
func TestCompareWithDatabaseInfo_ConstraintCommentsFollowTheTargetsCapabilities(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		want    []difftypes.ConstraintCommentChange
	}{
		{name: "PostgreSQL 18", dialect: platform.Postgres, caps: capability.Postgres18(), want: constraintCommentChange("old", "new")},
		{name: "CockroachDB 25.4", dialect: platform.CockroachDB, caps: capability.CockroachDB25(), want: constraintCommentChange("old", "new")},
		{name: "YugabyteDB 2024.2", dialect: platform.YugabyteDB, caps: capability.YugabyteDB24(), want: constraintCommentChange("old", "new")},
		{name: "the Spanner interface", dialect: platform.Spanner, caps: capability.SpannerPostgres(), want: nil},
		{name: "a PostgreSQL target without the key", dialect: platform.Postgres, caps: capability.Postgres18().With(capability.ConstraintComments, false), want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				commentedConstraints("total > 0", "new"),
				reportedConstraints("old"),
				catalog.ServerInfo{Dialect: test.dialect, Capabilities: test.caps},
				nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintCommentsChanged, qt.DeepEquals, test.want)
		})
	}
}

// Two declarations compared with each other go through the conversion that
// describes the current one as a database, and the comment has to survive it:
// a changed comment is reported, and an unchanged one is not reported as
// removed.
func TestCompareSchemas_ConstraintCommentsSurviveTheConversion(t *testing.T) {
	tests := []struct {
		name     string
		previous string
		want     []difftypes.ConstraintCommentChange
	}{
		{name: "a changed comment", previous: "old", want: constraintCommentChange("old", "new")},
		{name: "the same comment", previous: "new", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareSchemas(
				commentedConstraints("total > 0", "new"), commentedConstraints("total > 0", test.previous), platform.Postgres,
			)

			c.Assert(diff.ConstraintCommentsChanged, qt.DeepEquals, test.want)
		})
	}
}
