package catalog

import (
	"slices"

	"ptah.run/core/ast"
)

// Clone returns a table with independent columns and mutable settings.
// Immutable facets retain their value ownership in both copies.
func (t Table) Clone() Table {
	t.Columns = slices.Clone(t.Columns)
	for i := range t.Columns {
		t.Columns[i] = t.Columns[i].Clone()
	}
	t.RowDeletionPolicy = t.RowDeletionPolicy.Clone()
	t.YDBColumnFamilies = ast.CloneYDBColumnFamilies(t.YDBColumnFamilies)
	t.YDBPartitioning = t.YDBPartitioning.Clone()
	t.YDBColumnTable = t.YDBColumnTable.Clone()
	return t
}

// Clone returns a column with independent optional metadata.
func (c Column) Clone() Column {
	c.ColumnDefault = cloneScalar(c.ColumnDefault)
	c.CharacterMaxLength = cloneScalar(c.CharacterMaxLength)
	c.NumericPrecision = cloneScalar(c.NumericPrecision)
	c.NumericScale = cloneScalar(c.NumericScale)
	c.DatetimePrecision = cloneScalar(c.DatetimePrecision)
	c.GeneratedExpression = cloneScalar(c.GeneratedExpression)
	return c
}

// Clone returns a constraint with independent column lists and optional values.
func (c Constraint) Clone() Constraint {
	c.ColumnNames = slices.Clone(c.ColumnNames)
	c.ForeignColumns = slices.Clone(c.ForeignColumns)
	c.OnDeleteColumns = slices.Clone(c.OnDeleteColumns)
	c.IncludeColumns = slices.Clone(c.IncludeColumns)
	c.RequiresExtensions = slices.Clone(c.RequiresExtensions)
	c.ForeignTable = cloneScalar(c.ForeignTable)
	c.ForeignColumn = cloneScalar(c.ForeignColumn)
	c.DeleteRule = cloneScalar(c.DeleteRule)
	c.UpdateRule = cloneScalar(c.UpdateRule)
	c.CheckClause = cloneScalar(c.CheckClause)
	c.NullsDistinct = cloneScalar(c.NullsDistinct)
	c.UsingMethod = cloneScalar(c.UsingMethod)
	c.ExcludeElements = cloneScalar(c.ExcludeElements)
	c.WhereCondition = cloneScalar(c.WhereCondition)
	return c
}

func cloneScalar[T ~string | ~int | ~bool](value *T) *T {
	if value == nil {
		return nil
	}
	return new(*value)
}
