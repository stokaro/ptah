package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// commentedSchema declares a comment on a table, a column and an index.
//
// A comment is documentation the author wrote into the schema, and several
// targets answer it with a SQL line comment the server does not store. The
// render then looks like it kept the text while the database has none of it,
// which is indistinguishable from dropping it outright (stokaro/ptah#2983).
func commentedSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Item",
			Name:       "items",
			Comment:    "inventory items",
		}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "INT", Primary: true},
			{StructName: "Item", Name: "code", Type: "VARCHAR(64)", Comment: "external code"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Item",
			Name:       "idx_items_code",
			Fields:     []string{"code"},
			Comment:    "lookup",
		}},
	}
}

// omissionSubjects renders each omission as the object it belongs to and the
// property it lost, so a case row can carry the answer as data.
func omissionSubjects(c *qt.C, omissions []renderer.Omission) []string {
	c.Helper()

	subjects := make([]string, 0, len(omissions))
	for _, omission := range omissions {
		subjects = append(subjects, omission.Kind+" "+omission.Name+" "+omission.Property)
	}
	return subjects
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesEveryCommentATargetDrops
// covers the comment half of stokaro/ptah#2983.
//
// The rows are measured against what each renderer emits, not assumed: MySQL
// stores a table and a column comment and has no clause here for an index one;
// SQLite and SQL Server store none of the three and write two of them as line
// comments; Oracle and ClickHouse store the first two.
func TestGetOrderedCreateStatementsReportingOmissions_NamesEveryCommentATargetDrops(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{
			name:    "mysql keeps the table and column comments",
			dialect: platform.MySQL,
			want:    []string{"index idx_items_code comment"},
		},
		{
			name:    "mariadb keeps the table and column comments",
			dialect: platform.MariaDB,
			want:    []string{"index idx_items_code comment"},
		},
		{
			name:    "sqlite stores none of them",
			dialect: platform.SQLite,
			want: []string{
				"column items.code comment",
				"index idx_items_code comment",
				"table items comment",
			},
		},
		{
			name:    "sql server stores none of them",
			dialect: platform.SQLServer,
			want: []string{
				"column items.code comment",
				"index idx_items_code comment",
				"table items comment",
			},
		},
		{
			name:    "oracle keeps the table and column comments",
			dialect: platform.Oracle,
			want:    []string{"index idx_items_code comment"},
		},
		{
			name:    "clickhouse keeps the table and column comments",
			dialect: platform.ClickHouse,
			want:    []string{"index idx_items_code comment"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				commentedSchema(), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissionSubjects(c, omissions), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_PostgresStoresEveryComment is
// the control.
//
// Every row above is satisfied by a report that fires for any comment at all.
// PostgreSQL has COMMENT ON for all three objects, so a finding here would mean
// the check reports a declaration the target actually kept.
func TestGetOrderedCreateStatementsReportingOmissions_PostgresStoresEveryComment(t *testing.T) {
	c := qt.New(t)

	statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		commentedSchema(), platform.Postgres, capability.Postgres17())

	c.Assert(err, qt.IsNil)
	c.Assert(omissions, qt.HasLen, 0)
	rendered := strings.Join(statements, "\n")
	c.Assert(rendered, qt.Contains, "COMMENT ON TABLE")
	c.Assert(rendered, qt.Contains, "COMMENT ON COLUMN")
	c.Assert(rendered, qt.Contains, "COMMENT ON INDEX")
}

// TestGetOrderedCreateStatementsReportingOmissions_AStoredCommentIsNotReported
// is the other half of that control, per target rather than in aggregate.
//
// A row above claims MySQL, Oracle and ClickHouse keep the table and column
// comments. That claim is only worth having if the text is in their output, so
// each one is asked for it.
func TestGetOrderedCreateStatementsReportingOmissions_AStoredCommentIsNotReported(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "oracle", dialect: platform.Oracle},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, _, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				commentedSchema(), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(rendered, qt.Contains, "inventory items")
			c.Assert(rendered, qt.Contains, "external code")
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_ANoCommentSchemaReportsNothing
// keeps the check from firing on the absence of a declaration.
//
// A comment nobody wrote is not a comment the target dropped, and a report that
// counted empty strings would fail every schema on the targets above.
func TestGetOrderedCreateStatementsReportingOmissions_ANoCommentSchemaReportsNothing(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sql server", dialect: platform.SQLServer},
	}

	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INT", Primary: true}},
		Indexes: []schemamodel.Index{{
			StructName: "Item",
			Name:       "idx_items_id",
			Fields:     []string{"id"},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				database, test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
		})
	}
}
