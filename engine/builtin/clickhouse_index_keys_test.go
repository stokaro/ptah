package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// skippingIndexSchema declares one ClickHouse table, events(id, payload,
// x-y), and one data-skipping index on it over key.
func skippingIndexSchema(key ...schemamodel.IndexPart) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event"}},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Event", Type: "UInt64"},
			{Name: "payload", StructName: "Event", Type: "String"},
			{Name: "x-y", StructName: "Event", Type: "Int32"},
		},
		Indexes: []schemamodel.Index{{Name: "idx_payload", StructName: "Event", TableName: "events", Parts: key}},
	}
}

// keyColumn is a key part that names a column.
func keyColumn(name string) schemamodel.IndexPart { return schemamodel.IndexPart{Name: name} }

// keyExpression is a key part written as an expression.
func keyExpression(text string) schemamodel.IndexPart { return schemamodel.IndexPart{Expr: text} }

// TestValidateSchema_SkippingIndexKey_HappyPath accepts a key over declared
// columns, and a misspelled name with an operator beside it, which the check
// leaves to the server: such a name need not be a column.
func TestValidateSchema_SkippingIndexKey_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		key  []schemamodel.IndexPart
	}{
		{name: "a column", key: []schemamodel.IndexPart{keyColumn("payload")}},
		{name: "a column whose name reads as an expression bare", key: []schemamodel.IndexPart{keyColumn("x-y")}},
		{name: "a call over a column", key: []schemamodel.IndexPart{keyExpression("lower(payload)")}},
		{name: "two columns", key: []schemamodel.IndexPart{keyColumn("id"), keyColumn("payload")}},
		{name: "a name with an operator beside it", key: []schemamodel.IndexPart{keyExpression("paylod + 1")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(builtin.ValidateSchema(skippingIndexSchema(test.key...), "clickhouse"), qt.IsNil)
		})
	}
}

// TestValidateSchema_SkippingIndexKey_FailurePath refuses a key that names a
// column the table does not declare. ClickHouse refuses the ADD INDEX, and
// for an index whose key changed it runs after the DROP INDEX, outside a
// transaction.
func TestValidateSchema_SkippingIndexKey_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		key     []schemamodel.IndexPart
		wantErr string
	}{
		{
			name:    "a misspelled column",
			key:     []schemamodel.IndexPart{keyColumn("paylod")},
			wantErr: `index "idx_payload" reads column "paylod", which table events does not declare; .*`,
		},
		{
			// Written bare, the name is pay minus load, which names no column
			// on its own, and the server built an index nobody declared.
			name:    "a missing column whose name reads as an expression bare",
			key:     []schemamodel.IndexPart{keyColumn("pay-load")},
			wantErr: `index "idx_payload" reads column "pay-load", which table events does not declare; .*`,
		},
		{
			name:    "a misspelled argument of a call",
			key:     []schemamodel.IndexPart{keyExpression("lower(paylod)")},
			wantErr: `index "idx_payload" reads column "paylod", .*`,
		},
		{
			name:    "a misspelled part of a tuple",
			key:     []schemamodel.IndexPart{keyColumn("id"), keyColumn("paylod")},
			wantErr: `index "idx_payload" reads column "paylod", .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := builtin.ValidateSchema(skippingIndexSchema(test.key...), "clickhouse")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestValidateSchema_SkippingIndexKey_OtherTargets is the control: the rule is
// ClickHouse's, and a PostgreSQL index over the same names reaches the checks
// PostgreSQL has.
func TestValidateSchema_SkippingIndexKey_OtherTargets(t *testing.T) {
	c := qt.New(t)

	c.Assert(builtin.ValidateSchema(skippingIndexSchema(keyExpression("lower(paylod)")), "postgres"), qt.IsNil)
}
