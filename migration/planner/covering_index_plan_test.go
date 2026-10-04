package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The pin's covering variant changes one thing: users_name_ix gains email as
// its payload. Every dialect either plans the rebuild or refuses the payload
// with the render's message; none plans nothing (stokaro/ptah#4112).

// TestPlanIndexPayloadChange_HappyPath covers the targets that render and read
// a payload. Each rebuilds the index with it. CockroachDB, YugabyteDB and
// Spanner compared the index by name and planned nothing.
func TestPlanIndexPayloadChange_HappyPath(t *testing.T) {
	tests := []struct {
		dialect string
		drop    string
	}{
		{dialect: platform.Postgres, drop: `DROP INDEX IF EXISTS "users_name_ix";`},
		{dialect: platform.CockroachDB, drop: `DROP INDEX IF EXISTS "users"@"users_name_ix";`},
		{dialect: platform.YugabyteDB, drop: `DROP INDEX IF EXISTS "users_name_ix";`},
		{dialect: platform.Spanner, drop: `DROP INDEX IF EXISTS "users_name_ix";`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareSchemas(parsePinSchema(c, "covering"), parsePinSchema(c, "current"), test.dialect)

			planned, err := planner.GenerateSchemaDiffSQL(diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(planned, qt.Contains, test.drop)
			c.Assert(planned, qt.Contains, `CREATE INDEX IF NOT EXISTS "users_name_ix" ON "users" ("name") INCLUDE ("email");`)
		})
	}
}

// TestPlanIndexPayloadChange_FailurePath covers the targets that render no
// payload. The plan refuses as a render of the same schema does. ClickHouse
// planned a skipping index without the payload once the comparison reported
// the change, because its planner left the payload off the index it built.
func TestPlanIndexPayloadChange_FailurePath(t *testing.T) {
	for _, dialect := range []string{
		platform.ClickHouse, platform.MySQL, platform.MariaDB, platform.Oracle, platform.SQLite, platform.SQLServer,
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareSchemas(parsePinSchema(c, "covering"), parsePinSchema(c, "current"), dialect)

			planned, err := planner.GenerateSchemaDiffSQL(diff, dialect)

			c.Assert(err, qt.ErrorMatches, dialect+` does not support INCLUDE columns on index "users_name_ix"; target .*`)
			c.Assert(planned, qt.Equals, "")
		})
	}
}

// TestCompareIndexPayloadOnlyTheServerHolds is the control for the targets
// without a payload. SQL Server takes INCLUDE although Ptah renders none, so a
// database index can hold a payload no declaration names; the comparison
// leaves it alone rather than plan a rebuild that drops it.
func TestCompareIndexPayloadOnlyTheServerHolds(t *testing.T) {
	c := qt.New(t)
	database := goschematodb.ToDBSchema(parsePinSchema(c, "current"), platform.SQLServer)
	for i := range database.Indexes {
		database.Indexes[i].IncludeColumns = []string{"email"}
	}

	diff := schemadiff.CompareWithDialect(parsePinSchema(c, "current"), database, platform.SQLServer)

	c.Assert(diff.HasChanges(), qt.IsFalse)
}
