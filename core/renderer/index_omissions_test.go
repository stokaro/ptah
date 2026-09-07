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

// partialIndexSchema declares an index over a subset of the rows.
//
// A dropped condition is not a cosmetic loss. The index is created over every
// row instead, and on a unique index that changes which rows the server accepts
// (stokaro/ptah#2983).
func partialIndexSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INT", Primary: true},
			{StructName: "Doc", Name: "state", Type: "VARCHAR(16)"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Doc",
			Name:       "idx_docs_state",
			Fields:     []string{"state"},
			Condition:  "state <> 'archived'",
		}},
	}
}

// operatorClassSchema declares a PostgreSQL operator class on an index.
func operatorClassSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INT", Primary: true},
			{StructName: "Doc", Name: "body", Type: "TEXT"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Doc",
			Name:       "idx_docs_body",
			Fields:     []string{"body"},
			Operator:   "gin_trgm_ops",
		}},
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedPartialIndexCondition
// covers the loss with the sharpest consequence in stokaro/ptah#2983.
//
// Measured on this tree: the MySQL family and ClickHouse render the index over
// the whole table and say nothing, so the author gets an index that indexes
// more rows than they asked for and an exit status of zero.
func TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedPartialIndexCondition(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				partialIndexSchema(), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 1)
			c.Assert(omissions[0].Property, qt.Equals, "partial index condition")
			c.Assert(omissions[0].Detail, qt.Equals, "state <> 'archived'")
			c.Assert(omissions[0].Name, qt.Equals, "idx_docs_state")
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_APartialIndexThatRendersIsNotReported
// is that test's control.
//
// Three targets write the condition, and reporting it there would call a
// rendered clause a loss. Each is asked for the clause rather than trusted.
func TestGetOrderedCreateStatementsReportingOmissions_APartialIndexThatRendersIsNotReported(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sql server", dialect: platform.SQLServer},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				partialIndexSchema(), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "WHERE state <> 'archived'")
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedOperatorClass
// covers the second index property.
//
// Only the PostgreSQL family has the clause, so every other target drops a
// declared class with no diagnostic of any kind.
func TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedOperatorClass(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sql server", dialect: platform.SQLServer},
		{name: "oracle", dialect: platform.Oracle},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				operatorClassSchema(), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 1)
			c.Assert(omissions[0].Property, qt.Equals, "operator class")
			c.Assert(omissions[0].Detail, qt.Equals, "gin_trgm_ops")
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_PostgresRendersTheOperatorClass
// is that test's control.
func TestGetOrderedCreateStatementsReportingOmissions_PostgresRendersTheOperatorClass(t *testing.T) {
	c := qt.New(t)

	statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		operatorClassSchema(), platform.Postgres, capability.Postgres17())

	c.Assert(err, qt.IsNil)
	c.Assert(omissions, qt.HasLen, 0)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "gin_trgm_ops")
}

// TestGetOrderedCreateStatementsReportingOmissions_AnOperatorClassIsReportedOncePerValue
// keeps a shared class from reading as several losses.
//
// A class declared on the index and repeated on its parts is one thing the
// author loses, so the report names it once.
func TestGetOrderedCreateStatementsReportingOmissions_AnOperatorClassIsReportedOncePerValue(t *testing.T) {
	c := qt.New(t)
	database := operatorClassSchema()
	database.Indexes[0].Parts = []schemamodel.IndexPart{
		{Name: "body", Operator: "gin_trgm_ops"},
	}

	_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		database, platform.SQLite, capability.SQLite3())

	c.Assert(err, qt.IsNil)
	c.Assert(omissions, qt.HasLen, 1)
	c.Assert(omissions[0].Detail, qt.Equals, "gin_trgm_ops")
}

// TestGetOrderedCreateStatementsReportingOmissions_AnIndexWithoutThoseClausesReportsNothing
// keeps the check from firing on the absence of a declaration.
func TestGetOrderedCreateStatementsReportingOmissions_AnIndexWithoutThoseClausesReportsNothing(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "oracle", dialect: platform.Oracle},
	}

	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INT", Primary: true},
			{StructName: "Doc", Name: "state", Type: "VARCHAR(16)"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Doc",
			Name:       "idx_docs_state",
			Fields:     []string{"state"},
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
