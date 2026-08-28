package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestCompare_AColumnModificationCarriesTheDeclaredColumn is the property the
// column-modification carry rests on.
//
// `Changes` says WHAT moved, in a form a person reads: `"INTEGER -> BIGINT"`.
// Rendering `ALTER COLUMN` needs the column itself, and reading `" -> "` back
// out of a display string recovers a fraction of it. So the operand travels
// beside the display, and this asserts it is the column the declaration wrote
// rather than anything reconstructed (stokaro/ptah#2315).
func TestCompare_AColumnModificationCarriesTheDeclaredColumn(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "wf2315_users"}},
		Fields: []schemamodel.Field{{
			StructName: "User",
			Name:       "score",
			Type:       "BIGINT",
			Default:    "7",
		}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "wf2315_users",
			Type: "TABLE",
			Columns: []catalog.Column{{
				Name:       "score",
				DataType:   "INTEGER",
				ColumnType: "INTEGER",
				IsNullable: "NO",
			}},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	modified := diff.TablesModified[0].ColumnsModified[0]
	c.Assert(modified.ColumnName, qt.Equals, "score")
	c.Assert(modified.Desired, qt.DeepEquals, desired.Fields[0])
}

// TestCompare_AnEmbeddedColumnsModificationCarriesTheFoldedColumn is the half a
// declaration-shaped fixture cannot reach by accident.
//
// An embedded struct's column belongs to the host table but is declared under
// the embedded struct's name, so a carry taken from `Database.Fields` without
// folding would attach the modification to a column no table has. The planner
// used to do the folding itself and reported `ERROR: Could not find field
// definition` when it could not; the fold lives in the comparison now, and this
// is where it is asserted.
func TestCompare_AnEmbeddedColumnsModificationCarriesTheFoldedColumn(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "wf2315_users"}},
		Fields: []schemamodel.Field{{
			StructName: "ComputedFields",
			Name:       "score",
			Type:       "BIGINT",
		}},
		EmbeddedFields: []schemamodel.EmbeddedField{{
			StructName:       "User",
			Mode:             "inline",
			EmbeddedTypeName: "ComputedFields",
		}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "wf2315_users",
			Type: "TABLE",
			Columns: []catalog.Column{{
				Name:       "score",
				DataType:   "INTEGER",
				ColumnType: "INTEGER",
				IsNullable: "NO",
			}},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	modified := diff.TablesModified[0].ColumnsModified[0]
	c.Assert(modified.ColumnName, qt.Equals, "score")
	c.Assert(modified.Desired.Name, qt.Equals, "score")
	c.Assert(modified.Desired.Type, qt.Equals, "BIGINT")
}

// TestCompare_ACollidingTableNameCarriesTheStructurallyIdentifiedColumn is the
// resolution that used to happen in the planner, asserted where it happens now.
//
// Two declared tables answer to the string `tenant.data`: one literally named
// that, and one named `data` in schema `tenant`. They are different tables with
// different columns, and a modification naming `tenant.data` belongs to exactly
// one of them. The planner used to decide by re-resolving the diff's table name
// against the declaration; the comparison decides now, by producing the
// modification from the table it matched.
func TestCompare_ACollidingTableNameCarriesTheStructurallyIdentifiedColumn(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Literal", Name: "payload", Type: "TEXT"},
			{StructName: "Qualified", Name: "payload", Type: "BIGINT"},
		},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name:   "data",
			Schema: "tenant",
			Type:   "TABLE",
			Columns: []catalog.Column{{
				Name:       "payload",
				DataType:   "TEXT",
				ColumnType: "TEXT",
				IsNullable: "NO",
			}},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	modified := columnModificationsOf(c, diff.TablesModified, "tenant.data")
	c.Assert(modified.Desired.StructName, qt.Equals, "Qualified")
	c.Assert(modified.Desired.Type, qt.Equals, "BIGINT")
}

// columnModificationsOf returns the single column modification of the named
// table, failing rather than answering when the shape is not what the caller
// assumed.
func columnModificationsOf(
	c *qt.C,
	tables []difftypes.TableDiff,
	tableName string,
) difftypes.ColumnDiff {
	c.Helper()
	for _, table := range tables {
		if table.TableName != tableName {
			continue
		}
		c.Assert(table.ColumnsModified, qt.HasLen, 1)
		return table.ColumnsModified[0]
	}
	c.Fatalf("no modification for table %q in %v", tableName, tables)
	return difftypes.ColumnDiff{}
}
