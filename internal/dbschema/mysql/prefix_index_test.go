package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// TestPrefixIndexReachesTheDocument pins the path a prefix length takes from the
// catalog to the document.
//
// MySQL requires a length for an index on a BLOB or TEXT column, so a key that
// loses it produces a description the server refuses outright:
//
//	Error 1170 (42000): BLOB/TEXT column 'notes' used in key specification
//	without a key length
//
// Measured on MySQL 26.7 by applying Ptah's own description into a fresh
// database (stokaro/ptah#2112).
func TestPrefixIndexReachesTheDocument(t *testing.T) {
	c := qt.New(t)
	database := dbschematogo.ConvertDBSchemaToGoSchema(&types.DBSchema{
		Tables: []types.DBTable{{
			Name: "orders",
			Type: "BASE TABLE",
			Columns: []types.DBColumn{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "notes", DataType: "text", IsNullable: "YES"},
			},
		}},
		Indexes: []types.DBIndex{{
			Name:      "orders_notes_prefix_idx",
			TableName: "orders",
			Columns:   []string{"notes"},
			Parts:     []types.DBIndexPart{{Name: "notes", Prefix: "20"}},
		}},
	})

	rendered, err := atlashclrender.Render(database)

	c.Assert(err, qt.IsNil)
	document := string(rendered.Data)
	c.Assert(document, qt.Contains, "prefix = 20")
	c.Assert(document, qt.Not(qt.Contains), `prefix = "20"`)
	c.Assert(document, qt.Not(qt.Contains), "columns = [column.notes]")
}

// TestWholeColumnIndexKeepsTheCompactSpelling is the control: a key with no
// prefix says nothing an `on` block would carry, so it stays the compact form
// an author writes.
func TestWholeColumnIndexKeepsTheCompactSpelling(t *testing.T) {
	c := qt.New(t)
	database := dbschematogo.ConvertDBSchemaToGoSchema(&types.DBSchema{
		Tables: []types.DBTable{{
			Name: "orders",
			Type: "BASE TABLE",
			Columns: []types.DBColumn{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "customer_id", DataType: "bigint", IsNullable: "NO"},
			},
		}},
		Indexes: []types.DBIndex{{
			Name:      "orders_customer_idx",
			TableName: "orders",
			Columns:   []string{"customer_id"},
		}},
	})

	rendered, err := atlashclrender.Render(database)

	c.Assert(err, qt.IsNil)
	document := string(rendered.Data)
	c.Assert(document, qt.Contains, "columns = [column.customer_id]")
	c.Assert(document, qt.Not(qt.Contains), "prefix")
}
