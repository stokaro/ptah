package dataorder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/dataorder"
)

// hierarchy is one table whose rows reference rows of the same table. `child`
// sorts before `parent`, so a caller that walks rows by key writes the child
// first and the constraint refuses it.
func hierarchy() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Node", Name: "nodes"}},
		Fields: []schemamodel.Field{
			{StructName: "Node", Name: "code", Primary: true},
			{StructName: "Node", Name: "parent_code", Foreign: "nodes(code)"},
			{StructName: "Node", Name: "owner_code", Foreign: "people(code)"},
		},
	}
}

func TestSelfReferences_HappyPath(t *testing.T) {
	t.Run("a column referencing its own table is named", func(t *testing.T) {
		c := qt.New(t)
		db := hierarchy()
		c.Assert(dataorder.SelfReferences(db, db.Tables[0]), qt.DeepEquals, []string{"parent_code"})
	})

	t.Run("the shorthand spelling resolves too", func(t *testing.T) {
		c := qt.New(t)
		db := hierarchy()
		db.Fields[1].Foreign = "nodes"
		c.Assert(dataorder.SelfReferences(db, db.Tables[0]), qt.DeepEquals, []string{"parent_code"})
	})
}

func TestSelfReferences_FailurePath(t *testing.T) {
	t.Run("a column referencing another table is not a self-reference", func(t *testing.T) {
		// owner_code names people(code). Ordering rows by it would pair a row
		// with a row that is not in this set at all.
		c := qt.New(t)
		db := hierarchy()
		db.Fields = db.Fields[2:]
		c.Assert(dataorder.SelfReferences(db, db.Tables[0]), qt.IsNil)
	})

	t.Run("a nil schema names nothing", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(dataorder.SelfReferences(nil, schemamodel.Table{Name: "nodes"}), qt.IsNil)
	})
}

func codes(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row["code"].(string))
	}
	return out
}

func TestRows_HappyPath(t *testing.T) {
	t.Run("a referencing row arrives after the row it references", func(t *testing.T) {
		c := qt.New(t)
		rows := []map[string]any{
			{"code": "child", "parent_code": "parent"},
			{"code": "parent"},
		}
		c.Assert(codes(dataorder.Rows(rows, []string{"code"}, []string{"parent_code"})), qt.DeepEquals,
			[]string{"parent", "child"})
	})

	t.Run("a chain is ordered root first", func(t *testing.T) {
		c := qt.New(t)
		rows := []map[string]any{
			{"code": "c", "parent_code": "b"},
			{"code": "b", "parent_code": "a"},
			{"code": "a"},
		}
		c.Assert(codes(dataorder.Rows(rows, []string{"code"}, []string{"parent_code"})), qt.DeepEquals,
			[]string{"a", "b", "c"})
	})

	t.Run("a value read back as bytes pairs with the declared text", func(t *testing.T) {
		// A driver hands back []byte for a text column on the MySQL family, and
		// the delete direction orders rows that came from the database.
		c := qt.New(t)
		rows := []map[string]any{
			{"code": "child", "parent_code": []byte("parent")},
			{"code": "parent"},
		}
		c.Assert(codes(dataorder.Rows(rows, []string{"code"}, []string{"parent_code"})), qt.DeepEquals,
			[]string{"parent", "child"})
	})

	t.Run("rows referencing nothing keep the order they arrived in", func(t *testing.T) {
		c := qt.New(t)
		rows := []map[string]any{{"code": "b"}, {"code": "a"}}
		c.Assert(codes(dataorder.Rows(rows, []string{"code"}, []string{"parent_code"})), qt.DeepEquals,
			[]string{"b", "a"})
	})
}

func TestRows_FailurePath(t *testing.T) {
	t.Run("two rows referencing each other keep their order", func(t *testing.T) {
		// No arrangement satisfies a cycle, and a foreign key that is not
		// deferrable refuses both rows whichever goes first. Ordering them at
		// all would be inventing an answer.
		c := qt.New(t)
		rows := []map[string]any{
			{"code": "a", "parent_code": "b"},
			{"code": "b", "parent_code": "a"},
		}
		c.Assert(codes(dataorder.Rows(rows, []string{"code"}, []string{"parent_code"})), qt.DeepEquals,
			[]string{"a", "b"})
	})

	t.Run("a composite key is left alone", func(t *testing.T) {
		// Nothing in the declaration says which component of the key the
		// reference names, so pairing would be a guess.
		c := qt.New(t)
		rows := []map[string]any{
			{"code": "child", "parent_code": "parent"},
			{"code": "parent"},
		}
		c.Assert(codes(dataorder.Rows(rows, []string{"code", "tenant"}, []string{"parent_code"})), qt.DeepEquals,
			[]string{"child", "parent"})
	})

	t.Run("a table with no self-reference is left alone", func(t *testing.T) {
		c := qt.New(t)
		rows := []map[string]any{{"code": "b"}, {"code": "a"}}
		c.Assert(codes(dataorder.Rows(rows, []string{"code"}, nil)), qt.DeepEquals, []string{"b", "a"})
	})
}
