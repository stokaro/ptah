package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The pin's covering variant changes one thing: users_name_ix gains email as
// its payload. Every dialect either plans the rebuild or refuses the payload
// with the render's message; none plans nothing (stokaro/ptah#4112).

// TestPlanIndexPayloadChange_HappyPath covers the targets that render and read
// a payload. Each rebuilds the index with it. CockroachDB, YugabyteDB and
// Spanner compared the index by name and planned nothing, and SQL Server
// rendered no payload at all (stokaro/ptah#4114).
func TestPlanIndexPayloadChange_HappyPath(t *testing.T) {
	const quotedCreate = `CREATE INDEX IF NOT EXISTS "users_name_ix" ON "users" ("name") INCLUDE ("email");`
	tests := []struct {
		dialect string
		drop    string
		create  string
	}{
		{dialect: platform.Postgres, drop: `DROP INDEX IF EXISTS "users_name_ix";`, create: quotedCreate},
		{dialect: platform.CockroachDB, drop: `DROP INDEX IF EXISTS "users"@"users_name_ix";`, create: quotedCreate},
		{dialect: platform.YugabyteDB, drop: `DROP INDEX IF EXISTS "users_name_ix";`, create: quotedCreate},
		{dialect: platform.Spanner, drop: `DROP INDEX IF EXISTS "users_name_ix";`, create: quotedCreate},
		{
			dialect: platform.SQLServer,
			drop:    `DROP INDEX IF EXISTS [users_name_ix] ON [users];`,
			create:  `CREATE INDEX [users_name_ix] ON [users] ([name]) INCLUDE ([email]);`,
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareSchemas(parsePinSchema(c, "covering"), parsePinSchema(c, "current"), test.dialect)

			planned, err := planner.GenerateSchemaDiffSQL(diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(planned, qt.Contains, test.drop)
			c.Assert(planned, qt.Contains, test.create)
		})
	}
}

// TestPlanIndexPayloadChange_FailurePath covers the targets that render no
// payload. The plan refuses as a render of the same schema does. ClickHouse
// planned a skipping index without the payload once the comparison reported
// the change, because its planner left the payload off the index it built.
func TestPlanIndexPayloadChange_FailurePath(t *testing.T) {
	for _, dialect := range []string{
		platform.ClickHouse, platform.MySQL, platform.MariaDB, platform.Oracle, platform.SQLite,
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

// TestPlanIndexPayloadOnlyTheServerHolds_HappyPath reads a payload the
// declaration does not name. The declaration is the desired state, so the plan
// rebuilds the index without it, on SQL Server as on PostgreSQL.
func TestPlanIndexPayloadOnlyTheServerHolds_HappyPath(t *testing.T) {
	tests := []struct {
		dialect string
		create  string
	}{
		{dialect: platform.Postgres, create: `CREATE INDEX IF NOT EXISTS "users_name_ix" ON "users" ("name");`},
		{dialect: platform.SQLServer, create: `CREATE INDEX [users_name_ix] ON [users] ([name]);`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			database := goschematodb.ToDBSchema(withPayload(parsePinSchema(c, "current"), "email"), test.dialect)
			diff := schemadiff.CompareWithDialect(parsePinSchema(c, "current"), database, test.dialect)

			planned, err := planner.GenerateSchemaDiffSQL(diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(planned, qt.Contains, test.create)
		})
	}
}

// withPayload returns schema with the users_name_ix payload set to columns.
func withPayload(schema *schemamodel.Database, columns ...string) *schemamodel.Database {
	for i := range schema.Indexes {
		if schema.Indexes[i].Name == "users_name_ix" {
			schema.Indexes[i].IncludeColumns = columns
		}
	}
	return schema
}

// TestCompareIndexPayloadLeavesOutCockroachDBImplicitPrimaryKey declares a
// payload that names the table's primary key. CockroachDB holds the primary
// key in every secondary index and ignores it in STORING or INCLUDE, so the
// live index reads back without it, and the comparison must not plan a rebuild
// for it on every run.
func TestCompareIndexPayloadLeavesOutCockroachDBImplicitPrimaryKey(t *testing.T) {
	tests := []struct {
		name     string
		declared []string
		live     []string
	}{
		{name: "the primary key alone", declared: []string{"id"}},
		{name: "the primary key beside another column", declared: []string{"id", "email"}, live: []string{"email"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := goschematodb.ToDBSchema(withPayload(parsePinSchema(c, "current"), test.live...), platform.CockroachDB)

			diff := schemadiff.CompareWithDialect(withPayload(parsePinSchema(c, "current"), test.declared...), database, platform.CockroachDB)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff.IndexesAdded))
		})
	}
}

// TestCompareIndexPayloadKeepsAPrimaryKeyPayloadOnPostgreSQL is the control:
// PostgreSQL keeps a primary-key column in INCLUDE as declared, so the same
// declaration against a live index without it is a change there.
func TestCompareIndexPayloadKeepsAPrimaryKeyPayloadOnPostgreSQL(t *testing.T) {
	c := qt.New(t)
	database := goschematodb.ToDBSchema(parsePinSchema(c, "current"), platform.Postgres)

	diff := schemadiff.CompareWithDialect(withPayload(parsePinSchema(c, "current"), "id"), database, platform.Postgres)

	c.Assert(diff.HasChanges(), qt.IsTrue)
}
