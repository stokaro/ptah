package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// skippingIndexSchema declares one ClickHouse table, events(id, payload), and
// one data-skipping index on it over key.
func skippingIndexSchema(key ...string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event"}},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Event", Type: "UInt64"},
			{Name: "payload", StructName: "Event", Type: "String"},
		},
		Indexes: []schemamodel.Index{{Name: "idx_payload", StructName: "Event", TableName: "events", Fields: key}},
	}
}

// TestValidateSchema_SkippingIndexKey_HappyPath accepts a key over declared
// columns, and a misspelled name with an operator beside it, which the check
// leaves to the server: such a name need not be a column.
func TestValidateSchema_SkippingIndexKey_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		key  []string
	}{
		{name: "a column", key: []string{"payload"}},
		{name: "a call over a column", key: []string{"lower(payload)"}},
		{name: "two columns", key: []string{"id", "payload"}},
		{name: "a name with an operator beside it", key: []string{"paylod + 1"}},
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
		key     []string
		wantErr string
	}{
		{
			name:    "a misspelled column",
			key:     []string{"paylod"},
			wantErr: `index "idx_payload" reads column "paylod", which table events does not declare; .*`,
		},
		{
			name:    "a misspelled argument of a call",
			key:     []string{"lower(paylod)"},
			wantErr: `index "idx_payload" reads column "paylod", .*`,
		},
		{
			name:    "a misspelled part of a tuple",
			key:     []string{"id", "paylod"},
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

	c.Assert(builtin.ValidateSchema(skippingIndexSchema("lower(paylod)"), "postgres"), qt.IsNil)
}
