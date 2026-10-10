package schemamodel

import (
	"maps"
	"slices"

	"ptah.run/core/ast"
)

// Clone returns a table with independent key, partition, and option definitions.
// Immutable facets retain their value ownership in both copies.
func (t Table) Clone() Table {
	t.PrimaryKey = slices.Clone(t.PrimaryKey)
	t.PrimaryKeyParts = slices.Clone(t.PrimaryKeyParts)
	t.PrimaryKeyInclude = slices.Clone(t.PrimaryKeyInclude)
	t.Checks = slices.Clone(t.Checks)
	t.DependsOn = slices.Clone(t.DependsOn)
	t.Overrides = cloneOverrides(t.Overrides)
	if t.Partition != nil {
		t.Partition = &PartitionSpec{Type: t.Partition.Type, Parts: slices.Clone(t.Partition.Parts)}
	}
	t.YDBColumnFamilies = ast.CloneYDBColumnFamilies(t.YDBColumnFamilies)
	t.YDBPartitioning = t.YDBPartitioning.Clone()
	t.YDBColumnTable = t.YDBColumnTable.Clone()
	return t
}

// Clone returns a field with independent enum values and target overrides.
func (f Field) Clone() Field {
	f.Enum = slices.Clone(f.Enum)
	f.Overrides = cloneOverrides(f.Overrides)
	return f
}

// Clone returns a constraint with independent column lists and optional values.
func (c Constraint) Clone() Constraint {
	c.Columns = slices.Clone(c.Columns)
	c.IncludeColumns = slices.Clone(c.IncludeColumns)
	c.ForeignColumns = slices.Clone(c.ForeignColumns)
	c.OnDeleteColumns = slices.Clone(c.OnDeleteColumns)
	c.RequiresExtensions = slices.Clone(c.RequiresExtensions)
	if c.NullsDistinct != nil {
		c.NullsDistinct = new(*c.NullsDistinct)
	}
	return c
}

// Clone returns an enum with an independent value list.
func (e Enum) Clone() Enum {
	e.Values = slices.Clone(e.Values)
	return e
}

// Clone returns a trigger with an independent target list.
func (t Trigger) Clone() Trigger {
	t.Dialects = slices.Clone(t.Dialects)
	return t
}

func cloneOverrides(overrides map[string]map[string]string) map[string]map[string]string {
	result := maps.Clone(overrides)
	for target, properties := range result {
		result[target] = maps.Clone(properties)
	}
	return result
}
