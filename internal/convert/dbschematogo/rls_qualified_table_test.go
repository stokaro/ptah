package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// rlsSchema is one table carrying row-level security, in the schema named, with
// a policy on it.
//
// The policy is not decoration. It is the same object named twice in one
// description, so the two spellings have to agree; a fixture with only the
// enablement could not tell a qualified name from an unqualified one.
func rlsSchema(schemaName string) *catalog.Database {
	qualified := "users"
	if schemaName != "" {
		qualified = schemaName + ".users"
	}
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name:       "users",
			Schema:     schemaName,
			Type:       "BASE TABLE",
			RLSEnabled: true,
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "tenant", DataType: "text", IsNullable: "NO"},
			},
		}},
		RLSPolicies: []catalog.RLSPolicy{{
			Name:            "tenant_isolation",
			Table:           qualified,
			PolicyFor:       "ALL",
			ToRoles:         "PUBLIC",
			UsingExpression: "(tenant = CURRENT_USER)",
		}},
	}
}

// TestConvert_EnablesRLSOnTheSchemaQualifiedTable pins that the enablement
// names the same object the policy does.
//
// It did not. The conversion took the qualified name for the struct-name lookup
// and the bare one for the reference that is rendered, so a description of a
// table outside the connection's default schema emitted
// `CREATE POLICY tenant_isolation ON app.users` beside
// `ALTER TABLE users ENABLE ROW LEVEL SECURITY`. The ALTER resolves against the
// search path: with an unrelated public.users present it enabled row security
// on THAT table, leaving it with no policy -- which returns no rows to anyone
// but its owner -- while app.users kept a tenant policy that was never enforced
// because row security was never turned on for it (stokaro/ptah#2201).
func TestConvert_EnablesRLSOnTheSchemaQualifiedTable(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(rlsSchema("app"))

	c.Assert(database.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(database.RLSEnabledTables[0].Table, qt.Equals, "app.users")
	// The property the literal above is an instance of: one object, one
	// spelling. A fix that qualified only one of the two would still leave a
	// policy and an enablement pointing at different tables.
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSEnabledTables[0].Table, qt.Equals, database.RLSPolicies[0].Table)
}

// TestConvert_LeavesAnUnqualifiedRLSTableAlone is the control.
//
// A reader scoped to the connection's default schema reports no schema on the
// table, and the bare name is then the whole reference. Qualifying it would put
// an empty schema in front of every table a single-schema read describes.
func TestConvert_LeavesAnUnqualifiedRLSTableAlone(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(rlsSchema(""))

	c.Assert(database.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(database.RLSEnabledTables[0].Table, qt.Equals, "users")
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSEnabledTables[0].Table, qt.Equals, database.RLSPolicies[0].Table)
}
