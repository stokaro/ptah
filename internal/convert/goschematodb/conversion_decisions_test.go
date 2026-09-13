package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
)

// Two conversions are decisions rather than field carries, and a self-compare
// sweep cannot tell either of them from the wrong answer: both would leave the
// comparison clean. They are pinned here, where the decision is made.

// TestToDBSchema_CarriesAnIndexLevelOperatorOntoPlainColumns is the first.
//
// The DB shape carries an operator class per key and nothing above them, so an
// index that names its keys as plain columns has nowhere to put an index-level
// class. The columns become parts where there is a class to put on them, which
// is what a reader reports for the same index.
func TestToDBSchema_CarriesAnIndexLevelOperatorOntoPlainColumns(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "payload", Type: "TEXT"}},
		Indexes: []schemamodel.Index{{
			StructName: "T", Name: "idx_t_payload", TableName: "t",
			Fields: []string{"payload"}, Type: "GIN", Operator: "gin_trgm_ops",
		}},
	}

	got := goschematodb.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Indexes, qt.HasLen, 1)
	c.Assert(got.Indexes[0].Parts, qt.DeepEquals, []catalog.IndexPart{
		{Name: "payload", Operator: "gin_trgm_ops"},
	})
}

// TestToDBSchema_LeavesAPlainIndexWithoutSynthesizedParts is the control.
//
// Synthesizing key parts for every index would put a key list into a description
// that has none, which is a claim about what a reader found rather than about
// what was declared. The class is what makes the parts necessary, so an index
// without one keeps the shape it had.
func TestToDBSchema_LeavesAPlainIndexWithoutSynthesizedParts(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "payload", Type: "TEXT"}},
		Indexes: []schemamodel.Index{{
			StructName: "T", Name: "idx_t_payload", TableName: "t",
			Fields: []string{"payload"},
		}},
	}

	got := goschematodb.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Indexes, qt.HasLen, 1)
	c.Assert(got.Indexes[0].Parts, qt.IsNil)
	c.Assert(got.Indexes[0].Columns, qt.DeepEquals, []string{"payload"})
}

// TestToDBSchema_DescribesAnEnumColumnTheWayTheTargetHasIt is the second
// decision: the conversion answers with the column a reader of that target would
// report, not with the type the declaration wrote.
func TestToDBSchema_DescribesAnEnumColumnTheWayTheTargetHasIt(t *testing.T) {
	enumColumn := func() *schemamodel.Database {
		return &schemamodel.Database{
			Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
			Fields: []schemamodel.Field{{
				StructName: "T", Name: "state", Type: "ENUM",
				Enum: []string{"draft", "live"},
			}},
		}
	}

	tests := []struct {
		dialect  string
		wantType string
	}{
		{dialect: platform.MySQL, wantType: "enum('draft','live')"},
		{dialect: platform.MariaDB, wantType: "enum('draft','live')"},
		{dialect: platform.SQLite, wantType: "TEXT"},
		{dialect: platform.SQLServer, wantType: "NVARCHAR(255)"},
		{dialect: platform.Oracle, wantType: "VARCHAR2(255)"},
		// PostgreSQL has a real enum type, so the declared name stands.
		{dialect: platform.Postgres, wantType: "ENUM"},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			got := goschematodb.ToDBSchema(enumColumn(), test.dialect)
			c.Assert(got.Tables, qt.HasLen, 1)
			c.Assert(got.Tables[0].Columns, qt.HasLen, 1)
			c.Assert(got.Tables[0].Columns[0].DataType, qt.Equals, test.wantType)
		})
	}
}

// TestToDBSchema_DoesNotModifyItsInput is what makes the enum decision safe.
//
// The conversion rewrites a field's type before converting, and a caller hands
// over a document it is still holding -- the comparison's own desired side is
// the same pointer. Rewriting in place would make the desired side carry the
// target's spelling of a column the caller never asked to change.
func TestToDBSchema_DoesNotModifyItsInput(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{
			StructName: "T", Name: "state", Type: "ENUM",
			Enum: []string{"draft", "live"},
		}},
		Enums: []schemamodel.Enum{{Name: "state_kind", Values: []string{"draft", "live"}}},
	}

	goschematodb.ToDBSchema(db, platform.MySQL)

	c.Assert(db.Fields[0].Type, qt.Equals, "ENUM")
	c.Assert(db.Fields[0].Check, qt.Equals, "")
	c.Assert(db.Enums, qt.HasLen, 1)
}
