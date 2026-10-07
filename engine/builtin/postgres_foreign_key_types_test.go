package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
)

// PostgreSQL accepts these foreign keys without widening either column. The
// renderer must keep each declared type instead of imposing MySQL's integer
// width rule on PostgreSQL.
func TestGetOrderedCreateStatements_PostgresComparableForeignKeyTypes(t *testing.T) {
	tests := []struct {
		name       string
		parentType string
		childType  string
	}{
		{name: "integer references bigserial", parentType: "BIGSERIAL", childType: "INTEGER"},
		{name: "bigint references integer", parentType: "INTEGER", childType: "BIGINT"},
		{name: "smallint references bigint", parentType: "BIGINT", childType: "SMALLINT"},
		{name: "bigint references smallint", parentType: "SMALLINT", childType: "BIGINT"},
		{name: "integer alias", parentType: "BIGINT", childType: "INT4"},
		{name: "shorter varchar", parentType: "VARCHAR(64)", childType: "VARCHAR(32)"},
		{name: "longer varchar", parentType: "VARCHAR(32)", childType: "VARCHAR(64)"},
		{name: "varchar alias", parentType: "CHARACTER VARYING(64)", childType: "VARCHAR(32)"},
		{name: "unbounded varchar", parentType: "VARCHAR", childType: "VARCHAR(32)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatements(
				simpleForeignKeyDatabase(test.parentType, test.childType), platform.Postgres,
			)
			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(rendered, qt.Contains, `"parent_id" `+test.childType)
			c.Assert(rendered, qt.Contains, `REFERENCES "parents"("id")`)
		})
	}
}
