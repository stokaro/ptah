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

// indexSchema declares one index over a table with a key, which ClickHouse
// needs before it will render a MergeTree table at all.
func indexSchema(index schemamodel.Index) *schemamodel.Database {
	index.StructName = "Doc"
	index.Name = "idx_docs_title"
	if len(index.Fields) == 0 {
		index.Fields = []string{"title"}
	}
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INT", Primary: true},
			{StructName: "Doc", Name: "title", Type: "VARCHAR(80)"},
		},
		Indexes: []schemamodel.Index{index},
	}
}

// lostIndexProperties renders the schema for one target and returns what it
// reported losing.
func lostIndexProperties(c *qt.C, database *schemamodel.Database, dialect string) []string {
	c.Helper()
	_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		database, dialect, capability.ForDialect(dialect))
	c.Assert(err, qt.IsNil)
	var properties []string
	for _, omission := range omissions {
		c.Assert(omission.Kind, qt.Equals, "index")
		properties = append(properties, omission.Property)
	}
	return properties
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedFulltextParser
// pins the MySQL FULLTEXT parser.
//
// It names a server plugin -- ngram, mecab -- that decides how the indexed text
// is split, so an index built without it matches different queries. No other
// family has a clause that could carry the name.
func TestGetOrderedCreateStatementsReportingOmissions_NamesADroppedFulltextParser(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "mysql writes it", dialect: platform.MySQL, want: nil},
		{name: "mariadb writes it", dialect: platform.MariaDB, want: nil},
		{name: "postgres drops it", dialect: platform.Postgres, want: []string{"fulltext parser"}},
		{name: "sqlite drops it", dialect: platform.SQLite, want: []string{"fulltext parser"}},
		{name: "sql server drops it", dialect: platform.SQLServer, want: []string{"fulltext parser"}},
		{name: "oracle drops it", dialect: platform.Oracle, want: []string{"fulltext parser"}},
		{name: "clickhouse drops it", dialect: platform.ClickHouse, want: []string{"fulltext parser"}},
	}

	database := indexSchema(schemamodel.Index{Parser: "ngram"})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostIndexProperties(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesEveryDroppedStorageParameter
// pins PostgreSQL's index storage parameters.
//
// Each key is its own record, so an index that keeps some of them shortens the
// report rather than leaving it unchanged.
func TestGetOrderedCreateStatementsReportingOmissions_NamesEveryDroppedStorageParameter(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "postgres writes them", dialect: platform.Postgres, want: nil},
		{
			name:    "mysql drops both",
			dialect: platform.MySQL,
			want:    []string{"index storage parameter fillfactor", "index storage parameter pages_per_range"},
		},
		{
			name:    "sqlite drops both",
			dialect: platform.SQLite,
			want:    []string{"index storage parameter fillfactor", "index storage parameter pages_per_range"},
		},
		{
			name:    "sql server drops both",
			dialect: platform.SQLServer,
			want:    []string{"index storage parameter fillfactor", "index storage parameter pages_per_range"},
		},
		{
			name:    "oracle drops both",
			dialect: platform.Oracle,
			want:    []string{"index storage parameter fillfactor", "index storage parameter pages_per_range"},
		},
		{
			name:    "clickhouse drops both",
			dialect: platform.ClickHouse,
			want:    []string{"index storage parameter fillfactor", "index storage parameter pages_per_range"},
		},
	}

	database := indexSchema(schemamodel.Index{
		StorageParams: map[string]string{"pages_per_range": "64", "fillfactor": "70"},
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostIndexProperties(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesWhatClickHouseDropsFromAnIndex
// covers the two an index loses on that target alone.
//
// Its renderer reads the column list rather than the parts, so a descending
// part is built ascending, and it turns a unique index into a data-skipping one
// that enforces nothing while writing a `--` line the server does not store.
//
// The detail is asserted, not only the property: a part carries either a column
// name or an expression, and a record that read one field would name the other
// kind of part as a bare ` DESC`. Both spellings are rows here for that reason.
func TestGetOrderedCreateStatementsReportingOmissions_NamesWhatClickHouseDropsFromAnIndex(t *testing.T) {
	tests := []struct {
		name       string
		index      schemamodel.Index
		wantProp   string
		wantDetail string
	}{
		{
			name: "a descending column part",
			index: schemamodel.Index{
				Fields: []string{"title"},
				Parts:  []schemamodel.IndexPart{{Name: "title", Desc: true}},
			},
			wantProp:   "index part order",
			wantDetail: "title DESC",
		},
		{
			name: "a descending expression part",
			index: schemamodel.Index{
				Fields: []string{"lower(title)"},
				Parts:  []schemamodel.IndexPart{{Expr: "lower(title)", Desc: true}},
			},
			wantProp:   "index part order",
			wantDetail: "lower(title) DESC",
		},
		{
			name:       "a unique index",
			index:      schemamodel.Index{Unique: true},
			wantProp:   "unique index",
			wantDetail: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				indexSchema(test.index), platform.ClickHouse, capability.ForDialect(platform.ClickHouse))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 1)
			c.Assert(omissions[0].Kind, qt.Equals, "index")
			c.Assert(omissions[0].Property, qt.Equals, test.wantProp)
			c.Assert(omissions[0].Detail, qt.Equals, test.wantDetail)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_ATargetWritesTheIndexClausesItKeeps
// is the control the tables above need.
//
// Each row there is satisfied by a report that fires for any index carrying the
// property. These ask for the clause in the output on the targets the rows call
// keepers.
func TestGetOrderedCreateStatementsReportingOmissions_ATargetWritesTheIndexClausesItKeeps(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		index   schemamodel.Index
		want    string
	}{
		{
			name:    "mysql writes the fulltext parser",
			dialect: platform.MySQL,
			index:   schemamodel.Index{Parser: "ngram"},
			want:    "WITH PARSER `ngram`",
		},
		{
			name:    "postgres writes the storage parameters",
			dialect: platform.Postgres,
			index:   schemamodel.Index{StorageParams: map[string]string{"fillfactor": "70"}},
			want:    "WITH (fillfactor='70')",
		},
		{
			name:    "postgres writes a descending part",
			dialect: platform.Postgres,
			index: schemamodel.Index{
				Fields: []string{"title"},
				Parts:  []schemamodel.IndexPart{{Expr: "title", Desc: true}},
			},
			want: "DESC",
		},
		{
			name:    "postgres writes a unique index",
			dialect: platform.Postgres,
			index:   schemamodel.Index{Unique: true},
			want:    "CREATE UNIQUE INDEX",
		},
		{
			name:    "sqlite writes a unique index",
			dialect: platform.SQLite,
			index:   schemamodel.Index{Unique: true},
			want:    "CREATE UNIQUE INDEX",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				indexSchema(test.index), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_AnOrdinaryIndexReportsNothing
// keeps the report off an index that declared none of this.
//
// An ascending part is the row that matters: ascending is what every target
// builds, so a part that says so lost nothing, and a check that reported it
// would fire on almost every index.
func TestGetOrderedCreateStatementsReportingOmissions_AnOrdinaryIndexReportsNothing(t *testing.T) {
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

	database := indexSchema(schemamodel.Index{
		Fields: []string{"title"},
		Parts:  []schemamodel.IndexPart{{Expr: "title"}},
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostIndexProperties(c, database, test.dialect), qt.HasLen, 0)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_IncludeColumnsAreRefusedRatherThanLost
// records why one property in stokaro/ptah#2983 gets no report.
//
// A covering index's INCLUDE payload is not dropped anywhere. Every target
// without the clause refuses the whole render before writing a statement, which
// is a louder answer than a finding and needs no second one. This test exists so
// that a renderer which later starts accepting and ignoring the payload is
// caught rather than read as a fixed refusal.
func TestGetOrderedCreateStatementsReportingOmissions_IncludeColumnsAreRefusedRatherThanLost(t *testing.T) {
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

	database := indexSchema(schemamodel.Index{IncludeColumns: []string{"id"}})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				database, test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.ErrorMatches, `(?s).*does not support INCLUDE columns.*`)
		})
	}
}
