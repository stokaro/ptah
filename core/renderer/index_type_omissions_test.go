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

// indexTypeSchema declares one index with the access method the caller names.
//
// The method decides how the server searches the index. A target that builds an
// ordinary tree instead answers a different set of queries quickly, on an object
// that looks created either way.
func indexTypeSchema(indexType string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INT", Primary: true},
			{StructName: "Doc", Name: "tags", Type: "TEXT"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Doc",
			Name:       "idx_docs_tags",
			Fields:     []string{"tags"},
			Type:       indexType,
		}},
	}
}

// indexTypeOmissionSubjects renders each omission as the object it belongs to
// and the property it lost, so a case row can carry the answer as data.
func indexTypeOmissionSubjects(c *qt.C, omissions []renderer.Omission) []string {
	c.Helper()

	var subjects []string
	for _, omission := range omissions {
		subjects = append(subjects, omission.Kind+" "+omission.Name+" "+omission.Property)
	}
	return subjects
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesAnIndexTypeATargetDrops
// covers the index-type slice of stokaro/ptah#2983.
//
// The rows are measured rather than assumed. GIN reaches the output on
// PostgreSQL as USING GIN and on ClickHouse as the skip-index TYPE; every other
// target here has no clause that could carry an access method, so the
// declaration is lost and now says so.
func TestGetOrderedCreateStatementsReportingOmissions_NamesAnIndexTypeATargetDrops(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{
			name:    "postgres writes USING GIN",
			dialect: platform.Postgres,
			want:    nil,
		},
		{
			name:    "mysql has no spelling for it",
			dialect: platform.MySQL,
			want:    []string{"index idx_docs_tags index type"},
		},
		{
			name:    "mariadb has no spelling for it",
			dialect: platform.MariaDB,
			want:    []string{"index idx_docs_tags index type"},
		},
		{
			name:    "sqlite builds one kind of index",
			dialect: platform.SQLite,
			want:    []string{"index idx_docs_tags index type"},
		},
		{
			name:    "sqlserver builds one kind of index",
			dialect: platform.SQLServer,
			want:    []string{"index idx_docs_tags index type"},
		},
		{
			name:    "oracle builds one kind of index",
			dialect: platform.Oracle,
			want:    []string{"index idx_docs_tags index type"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				indexTypeSchema("GIN"), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(indexTypeOmissionSubjects(c, omissions), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_KeepsAMethodTheTargetWrites
// is the control that the record follows the output rather than the dialect.
//
// HASH has a spelling in the MySQL family, `USING HASH`, and PostgreSQL writes
// it too. A finding on either would mean the record names a declaration the
// target actually kept.
func TestGetOrderedCreateStatementsReportingOmissions_KeepsAMethodTheTargetWrites(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "mysql", dialect: platform.MySQL},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				indexTypeSchema("HASH"), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "HASH")
			c.Assert(indexTypeOmissionSubjects(c, omissions), qt.HasLen, 0)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_LeavesADeclaredBtreeAlone is
// the rule the issue said this slice needed before it could be reported at all.
//
// BTREE names the default access method on every target that has the concept,
// and none of them writes a clause for it -- PostgreSQL included. A render that
// leaves it out normalized a default, and the author gets the index they asked
// for, so a record here would report a loss that did not happen.
func TestGetOrderedCreateStatementsReportingOmissions_LeavesADeclaredBtreeAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "mysql", dialect: platform.MySQL},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sqlserver", dialect: platform.SQLServer},
		{name: "oracle", dialect: platform.Oracle},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				indexTypeSchema("BTREE"), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(indexTypeOmissionSubjects(c, omissions), qt.HasLen, 0)
		})
	}
}
