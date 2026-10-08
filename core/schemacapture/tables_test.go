package schemacapture_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
)

func TestTableDeclarationCloneOwnsNestedChildren(t *testing.T) {
	c := qt.New(t)
	source := declaredTable()
	cloned := source.Clone()
	source.Table.PrimaryKey[0] = "changed"
	source.Fields[0].Overrides["postgres"]["type"] = "changed"
	source.Enums[0].Values[0] = "changed"
	source.Constraints[0].Columns[0] = "changed"
	source.Indexes[0].Fields[0] = "changed"
	source.Triggers[0].Dialects[0] = "changed"
	c.Assert(cloned, qt.DeepEquals, declaredTable())
	c.Assert(source, qt.Not(qt.DeepEquals), declaredTable())
	c.Assert(cloned.HasTable(), qt.IsTrue)
}

func declaredTable() schemacapture.TableDeclaration {
	return schemacapture.TableDeclaration{
		Table: schemamodel.Table{Name: "orders", PrimaryKey: []string{"id"}},
		Fields: []schemamodel.Field{{Name: "id", Overrides: map[string]map[string]string{
			"postgres": {"type": "bigint"},
		}}},
		Enums:       []schemamodel.Enum{{Name: "status", Values: []string{"pending"}}},
		Constraints: []schemamodel.Constraint{{Name: "unique_id", Columns: []string{"id"}}},
		Indexes:     []schemamodel.Index{{Name: "by_id", Fields: []string{"id"}}},
		Triggers:    []schemamodel.Trigger{{Name: "notify", Dialects: []string{"postgres"}}},
	}
}

func TestTableObservationCloneOwnsNestedChildren(t *testing.T) {
	c := qt.New(t)
	source := observedTable()
	cloned := source.Clone()
	*source.Table.Columns[0].ColumnDefault = "changed"
	source.Indexes[0].Columns[0] = "changed"
	source.Indexes[0].StorageParams["fillfactor"] = "20"
	*source.Constraints[0].CheckClause = "changed"
	source.Triggers[0].Name = "changed"
	c.Assert(cloned, qt.DeepEquals, observedTable())
	c.Assert(source, qt.Not(qt.DeepEquals), observedTable())
	c.Assert(cloned.HasTable(), qt.IsTrue)
}

func observedTable() schemacapture.TableObservation {
	return schemacapture.TableObservation{
		Table: catalog.Table{Name: "orders", Columns: []catalog.Column{
			{Name: "id", ColumnDefault: new("42")},
		}},
		Indexes: []catalog.Index{{Name: "by_id", Columns: []string{"id"},
			StorageParams: map[string]string{"fillfactor": "70"},
		}},
		Constraints: []catalog.Constraint{{Name: "valid_id", CheckClause: new("id > 0")}},
		Triggers:    []catalog.Trigger{{Name: "notify"}},
	}
}

func TestEmptyCaptureDoesNotClaimPresence(t *testing.T) {
	c := qt.New(t)
	declared, observed := schemacapture.TableDeclaration{}, schemacapture.TableObservation{}
	c.Assert(declared.HasTable(), qt.IsFalse)
	c.Assert(observed.HasTable(), qt.IsFalse)
	c.Assert(declared.Clone(), qt.DeepEquals, declared)
	c.Assert(observed.Clone(), qt.DeepEquals, observed)
}
