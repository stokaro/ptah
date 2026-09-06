package generator

// White-box testing required: the subject is priorTableCreation as it is
// rendered by the unexported generateDownMigrationSQL. The exported
// migration-file API reaches the same code only through a filesystem and a
// live connection, which would put the failure somewhere other than the rule
// under test.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/migration/schemadiff"
)

// TestGenerateDownMigration_RecreatesDroppedTablesInDependencyOrder is
// stokaro/ptah#2541.
//
// A rollback that puts two tables back has to create the referenced one first,
// and the only thing it can order by is the DependsOn each creation carries.
// priorTableCreation left that at its zero value, so
// TablesAdded.InDependencyOrder() had nothing to sort and the rollback emitted
// the tables in whatever order TablesRemoved held.
//
// The fixture is built so that order is the WRONG one: the catalog lists
// `aaa_orders` before `zzz_customers`, and `aaa_orders` references `zzz_customers`. A creation
// that carries no edge therefore puts the child first and the rollback fails
// on the foreign key, which is the state this test would have caught.
func TestGenerateDownMigration_RecreatesDroppedTablesInDependencyOrder(t *testing.T) {
	c := qt.New(t)
	target, prior := dependencyOrderedDropFixtures()

	upDiff := schemadiff.CompareWithDialect(target, prior, "postgres")
	c.Assert(upDiff.TablesRemoved, qt.Contains, "aaa_orders")
	c.Assert(upDiff.TablesRemoved, qt.Contains, "zzz_customers")

	down, err := generateDownMigrationSQL(upDiff, target, prior, "postgres")
	c.Assert(err, qt.IsNil)
	rendered := legacyRenderedSQL(down)

	parent := strings.Index(rendered, "CREATE TABLE zzz_customers")
	child := strings.Index(rendered, "CREATE TABLE aaa_orders")
	c.Assert(parent >= 0, qt.IsTrue, qt.Commentf("no CREATE for zzz_customers in:\n%s", rendered))
	c.Assert(child >= 0, qt.IsTrue, qt.Commentf("no CREATE for aaa_orders in:\n%s", rendered))
	c.Assert(parent < child, qt.IsTrue, qt.Commentf(
		"zzz_customers is referenced by aaa_orders and must be created first, got zzz_customers at %d and aaa_orders at %d:\n%s",
		parent, child, rendered))
}

// TestPriorTableCreation_CarriesTheEdgeAndNotTheSelfReference pins both halves
// of the decision in stokaro/ptah#2541 at the point they are made.
//
// The second assertion is not a wish: filling SelfReferencingForeignKeys would
// add a third copy of a key the forward path already emits twice
// (stokaro/ptah#2583), so the empty list is the current answer and changing it
// is a decision rather than a tidy-up.
func TestPriorTableCreation_CarriesTheEdgeAndNotTheSelfReference(t *testing.T) {
	_, catalogPrior := dependencyOrderedDropFixtures()
	prior := dbschematogo.ConvertDBSchemaToGoSchema(catalogPrior, "")

	t.Run("the edge to the referenced table is carried", func(t *testing.T) {
		c := qt.New(t)
		creation := priorTableCreation(prior, "aaa_orders")
		c.Assert(creation.DependsOn, qt.DeepEquals, []string{"zzz_customers"})
	})

	t.Run("a table referencing nothing carries no edge", func(t *testing.T) {
		c := qt.New(t)
		creation := priorTableCreation(prior, "zzz_customers")
		c.Assert(creation.DependsOn, qt.HasLen, 0)
	})

	t.Run("the self reference is left to the forward path", func(t *testing.T) {
		c := qt.New(t)
		creation := priorTableCreation(prior, "aaa_orders")
		c.Assert(creation.SelfReferencingForeignKeys, qt.HasLen, 0)
	})
}

// dependencyOrderedDropFixtures drops two tables, one referencing the other,
// named so that alphabetical order is the wrong answer.
func dependencyOrderedDropFixtures() (*schemamodel.Database, *catalog.Database) {
	target := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "widgets", StructName: "Widget"}},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "integer", StructName: "Widget", Primary: true},
		},
	}
	prior := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "widgets", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "aaa_orders", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "customer_id", DataType: "integer", IsNullable: "YES"},
			}},
			{Name: "zzz_customers", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
		},
		Constraints: []catalog.Constraint{
			{Name: "orders_pk", TableName: "aaa_orders", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
			{Name: "customers_pk", TableName: "zzz_customers", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
			{
				Name: "orders_customer_fk", TableName: "aaa_orders", Type: "FOREIGN KEY",
				ColumnName: "customer_id", ColumnNames: []string{"customer_id"},
				ForeignTable: new("zzz_customers"), ForeignColumn: new("id"), ForeignColumns: []string{"id"},
				DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION"),
			},
		},
	}
	return target, prior
}
