package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
)

// policySource is one table keyed on id, with a timestamp column created_at
// and an integer column expires, whose table directive carries attributes.
func policySource(attributes string) *schemamodel.Database {
	database := must.Must(goschema.ParseSource(builtintest.Annotations(), "events.go", `package entities

//ptah:schema:table name="events" `+attributes+`
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="created_at" type="TIMESTAMP"
	CreatedAt string
	//ptah:schema:field name="expires" type="BIGINT UNSIGNED"
	Expires uint64
}
`))
	return &database
}

// TestRowDeletionPolicy_ProviderPlatformPropertiesStayOnTheirTarget pins the
// source spelling of a row deletion policy for engines a provider serves:
// the properties of each engine's platform group are scoped to that engine,
// as every platform property is, so no built-in target renders them.
func TestRowDeletionPolicy_ProviderPlatformPropertiesStayOnTheirTarget(t *testing.T) {
	database := policySource(`platform.spanner.row_deletion_column="created_at" platform.spanner.row_deletion_interval="30 days" ` +
		`platform.ydb.row_deletion_column="expires" platform.ydb.row_deletion_interval="PT1H" platform.ydb.row_deletion_unit="seconds"`)
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.CockroachDB, caps: capability.CockroachDB26()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, test.dialect, test.caps)
			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(rendered, qt.Contains, "CREATE TABLE")
			c.Assert(rendered, qt.Not(qt.Contains), "TTL")
		})
	}
}
