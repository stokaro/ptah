package atlasschema_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
)

// A saved plan must scan the conversion its ALTER runs. A valid type spelling
// and the presence of the column's name do not establish that relationship.
func TestDecodePlanFile_RefusesARoundingTypeDifferentFromItsStatement(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		held      string
		declared  string
		temporal  bool
		statement string
	}{
		{"SQL Server decimal scale", "sqlserver", "DECIMAL(8,3)", "DECIMAL(8,2)", false,
			"ALTER TABLE [prices] ALTER COLUMN [amount] DECIMAL(8,1) NULL"},
		{"PostgreSQL decimal scale", "postgres", "numeric(8,3)", "numeric(8,2)", false,
			`ALTER TABLE "prices" ALTER COLUMN "amount" TYPE numeric(8,1)`},
		{"MySQL decimal scale", "mysql", "DECIMAL(8,3)", "DECIMAL(8,2)", false,
			"ALTER TABLE `prices` MODIFY COLUMN `amount` DECIMAL(8,1)"},
		{"MariaDB decimal scale", "mariadb", "DECIMAL(8,3)", "DECIMAL(8,2)", false,
			"ALTER TABLE `prices` MODIFY COLUMN `amount` DECIMAL(8,1)"},
		{"ClickHouse decimal scale", "clickhouse", "Decimal(8,3)", "Decimal(8,2)", false,
			"ALTER TABLE `prices` MODIFY COLUMN `amount` Decimal(8,1)"},
		{"SQL Server temporal precision", "sqlserver", "datetime2(7)", "datetime2(3)", true,
			"ALTER TABLE [prices] ALTER COLUMN [amount] datetime2(0) NULL"},
		{"PostgreSQL temporal precision", "postgres", "timestamp(6)", "timestamp(3)", true,
			`ALTER TABLE "prices" ALTER COLUMN "amount" TYPE timestamp(0)`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			contents, err := json.Marshal(atlasschema.PlanFile{
				FormatVersion: atlasschema.PlanFormatVersion, Dialect: test.dialect, FromFingerprint: "sha256:ab",
				Roundings: []atlasschema.PlanRounding{{
					Table: "prices", Column: "amount", HeldType: test.held, DeclaredType: test.declared, Temporal: test.temporal,
				}},
				Statements: []atlasschema.PlanStatement{{SQL: test.statement}},
			})
			c.Assert(err, qt.IsNil)

			plan, digest, err := atlasschema.DecodePlanFile(contents, "rounding.plan.json")

			c.Assert(err, qt.ErrorMatches, `invalid plan file rounding.plan.json: plan rounding 1 is not a change the plan records: .*declared type .* differs from the statement's type .*`)
			c.Assert(plan, qt.DeepEquals, atlasschema.PlanFile{})
			c.Assert(digest, qt.Equals, "")
		})
	}
}
