package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// nodeSchema is one table with a key and a date, which every declaration below
// hangs off.
func nodeSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INT", Primary: true},
			{StructName: "Doc", Name: "day", Type: "DATE"},
		},
	}
}

// lostNodeProperties renders the schema for one target and returns the losses
// as "kind property" pairs, since these records span several object kinds.
func lostNodeProperties(c *qt.C, database *schemamodel.Database, dialect string) []string {
	c.Helper()
	_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		database, dialect, capability.ForDialect(dialect))
	c.Assert(err, qt.IsNil)
	var found []string
	for _, omission := range omissions {
		found = append(found, omission.Kind+" "+omission.Property)
	}
	return found
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedTablePartition
// pins a declared partitioning.
//
// Only the PostgreSQL family renders PARTITION BY, so every other target
// creates one ordinary table where the author declared a partitioned one: every
// row lands in the same place and no partition can be detached. ClickHouse is in
// the list even though it has a PARTITION BY of its own, because this renderer
// does not read the declared spec.
func TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedTablePartition(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "postgres writes it", dialect: platform.Postgres, want: nil},
		{name: "mysql drops it", dialect: platform.MySQL, want: []string{"table partition"}},
		{name: "mariadb drops it", dialect: platform.MariaDB, want: []string{"table partition"}},
		{name: "sqlite drops it", dialect: platform.SQLite, want: []string{"table partition"}},
		{name: "sql server drops it", dialect: platform.SQLServer, want: []string{"table partition"}},
		{name: "oracle drops it", dialect: platform.Oracle, want: []string{"table partition"}},
		{name: "clickhouse drops it", dialect: platform.ClickHouse, want: []string{"table partition"}},
	}

	database := nodeSchema()
	database.Tables[0].Partition = &schemamodel.PartitionSpec{
		Type:  "RANGE",
		Parts: []schemamodel.PartitionPart{{Name: "day"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostNodeProperties(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_APartitionIsNamedInTheRecord
// asks for the declaration back, not only for the fact that one was lost.
//
// A reader who sees "partition" and no method or key cannot tell which of two
// partitioned tables the line is about.
func TestGetOrderedCreateStatementsReportingOmissions_APartitionIsNamedInTheRecord(t *testing.T) {
	tests := []struct {
		name string
		spec *schemamodel.PartitionSpec
		want string
	}{
		{
			name: "a column key",
			spec: &schemamodel.PartitionSpec{Type: "RANGE", Parts: []schemamodel.PartitionPart{{Name: "day"}}},
			want: "RANGE (day)",
		},
		{
			name: "an expression key",
			spec: &schemamodel.PartitionSpec{Type: "HASH", Parts: []schemamodel.PartitionPart{{Expr: "abs(id)"}}},
			want: "HASH (abs(id))",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := nodeSchema()
			database.Tables[0].Partition = test.spec

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				database, platform.SQLite, capability.ForDialect(platform.SQLite))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 1)
			c.Assert(omissions[0].Detail, qt.Equals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedSchemaCharacterSet
// pins the schema-level character set and collation.
//
// SQLite and Oracle are absent because they report the schema itself as
// unsupported, which already names the loss; a second record about a property
// of an object nobody created would be noise.
func TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedSchemaCharacterSet(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "mysql writes both", dialect: platform.MySQL, want: nil},
		{name: "mariadb writes both", dialect: platform.MariaDB, want: nil},
		{
			name:    "postgres drops both",
			dialect: platform.Postgres,
			want:    []string{"schema character set", "schema collation"},
		},
		{
			name:    "sql server drops both",
			dialect: platform.SQLServer,
			want:    []string{"schema character set", "schema collation"},
		},
		{
			name:    "clickhouse drops both",
			dialect: platform.ClickHouse,
			want:    []string{"schema character set", "schema collation"},
		},
	}

	database := nodeSchema()
	database.Schemas = []schemamodel.Schema{
		{Name: "app", Charset: "utf8mb4", Collate: "utf8mb4_bin"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostNodeProperties(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedRoleComment
// extends the comment report to a fourth object kind.
//
// Only the PostgreSQL family has COMMENT ON ROLE. The other four write the text
// as a `--` line the server does not store, which is the shape stokaro/ptah#3008
// recorded for tables, columns and indexes: the author reads the comment in a
// file and the database has none of it. The issue named ClickHouse alone;
// measured here, four targets do it.
func TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedRoleComment(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "postgres stores it", dialect: platform.Postgres, want: nil},
		{name: "mysql writes a line comment", dialect: platform.MySQL, want: []string{"role comment"}},
		{name: "mariadb writes a line comment", dialect: platform.MariaDB, want: []string{"role comment"}},
		{name: "sql server writes a line comment", dialect: platform.SQLServer, want: []string{"role comment"}},
		{name: "oracle writes a line comment", dialect: platform.Oracle, want: []string{"role comment"}},
		{name: "clickhouse writes a line comment", dialect: platform.ClickHouse, want: []string{"role comment"}},
	}

	database := nodeSchema()
	database.Roles = []schemamodel.Role{{Name: "reader", Comment: "read-only access"}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostNodeProperties(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedRefreshSchedule
// pins a materialized view's refresh schedule.
//
// ClickHouse writes REFRESH EVERY. PostgreSQL and Oracle create the view and
// schedule nothing, so it is populated once and never again, which reaches the
// reader as stale data rather than as a missing clause. The three targets that
// refuse materialized views outright are absent: nothing was created for a
// property to go missing from.
func TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedRefreshSchedule(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "clickhouse writes it", dialect: platform.ClickHouse, want: nil},
		{
			name:    "postgres drops it",
			dialect: platform.Postgres,
			want:    []string{"materialized view refresh schedule"},
		},
		{
			name:    "oracle drops it",
			dialect: platform.Oracle,
			want:    []string{"materialized view refresh schedule"},
		},
	}

	database := nodeSchema()
	database.MaterializedViews = []schemamodel.MaterializedView{{
		Name:    "docs_by_day",
		Body:    "SELECT day FROM docs",
		Refresh: &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostNodeProperties(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_ATargetWritesTheNodeClausesItKeeps
// is the control the four tables above need.
//
// Each row there is satisfied by a report that fires whenever the property is
// set. These ask for the clause in the output on the targets the rows call
// keepers.
func TestGetOrderedCreateStatementsReportingOmissions_ATargetWritesTheNodeClausesItKeeps(t *testing.T) {
	partitioned := nodeSchema()
	partitioned.Tables[0].Partition = &schemamodel.PartitionSpec{
		Type:  "RANGE",
		Parts: []schemamodel.PartitionPart{{Name: "day"}},
	}
	withSchema := nodeSchema()
	withSchema.Schemas = []schemamodel.Schema{{Name: "app", Charset: "utf8mb4", Collate: "utf8mb4_bin"}}
	withRole := nodeSchema()
	withRole.Roles = []schemamodel.Role{{Name: "reader", Comment: "read-only access"}}
	withView := nodeSchema()
	withView.MaterializedViews = []schemamodel.MaterializedView{{
		Name:    "docs_by_day",
		Body:    "SELECT day FROM docs",
		Refresh: &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
	}}

	tests := []struct {
		name     string
		dialect  string
		database *schemamodel.Database
		want     string
	}{
		{
			name:     "postgres writes the partition",
			dialect:  platform.Postgres,
			database: partitioned,
			want:     `PARTITION BY RANGE ("day")`,
		},
		{
			name:     "mysql writes the schema character set",
			dialect:  platform.MySQL,
			database: withSchema,
			want:     "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
		},
		{
			name:     "postgres stores the role comment",
			dialect:  platform.Postgres,
			database: withRole,
			want:     `COMMENT ON ROLE "reader" IS 'read-only access'`,
		},
		{
			name:     "clickhouse writes the refresh schedule",
			dialect:  platform.ClickHouse,
			database: withView,
			want:     "REFRESH EVERY 1 HOUR",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				test.database, test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_ASchemaDeclaringNoneOfThisReportsNothing
// keeps the four reports off a schema that declared none of it.
//
// The assertion is a subtraction rather than an empty list: SQLite and Oracle
// report the schema and the role themselves as unsupported, which is an older
// record and still correct. What must not appear is a property of an object
// nobody declared one on.
func TestGetOrderedCreateStatementsReportingOmissions_ASchemaDeclaringNoneOfThisReportsNothing(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sql server", dialect: platform.SQLServer},
		{name: "oracle", dialect: platform.Oracle},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	database := nodeSchema()
	database.Schemas = []schemamodel.Schema{{Name: "app"}}
	database.Roles = []schemamodel.Role{{Name: "reader"}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			found := lostNodeProperties(c, database, test.dialect)
			for _, property := range []string{
				"table partition",
				"schema character set",
				"schema collation",
				"role comment",
				"materialized view refresh schedule",
			} {
				c.Assert(found, qt.Not(qt.Contains), property)
			}
		})
	}
}
