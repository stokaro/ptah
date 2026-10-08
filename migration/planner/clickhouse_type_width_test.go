package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/chtype"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
)

// widDeclared is a ClickHouse table wid (id Int32, d) as a declaration states
// it.
func widDeclared(d schemamodel.Field) *schemamodel.Database {
	d.StructName, d.Name = "W", "d"
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "W", Name: "wid"}},
		Fields: []schemamodel.Field{{StructName: "W", Name: "id", Type: "Int32"}, d},
	}
}

// widLive is the same table as the ClickHouse reader describes it: the type
// as system.columns stores it, kept verbatim, and nullable when the type
// admits NULL.
func widLive(dType string) *catalog.Database {
	column := func(name, columnType string, position int) catalog.Column {
		nullable := "NO"
		if chtype.AdmitsNull(columnType) {
			nullable = "YES"
		}
		return catalog.Column{
			Name: name, DataType: columnType, ColumnType: columnType, TypeIsDeclaredText: true,
			IsNullable: nullable, OrdinalPosition: position,
		}
	}
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "wid", Type: "TABLE",
		Columns: []catalog.Column{column("id", "Int32", 1), column("d", dType, 2)},
	}}}
}

// A ClickHouse column whose type changes width is planned. The comparison
// folded every integer to one family, so Int32 against a declared Int64
// recorded no change and nothing was planned (stokaro/ptah#4105). A narrowing
// is planned too: the server accepts it, and the plan's safety report is what
// says it can lose data.
func TestGenerateSchemaDiffSQLStatements_ClickHouseTypeWidth_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		live       string
		declared   schemamodel.Field
		wantChange string
		want       string
	}{
		{name: "Int32 widened", live: "Int32", declared: schemamodel.Field{Type: "Int64"},
			wantChange: "Int32 -> Int64", want: "ALTER TABLE wid MODIFY COLUMN d Int64"},
		{name: "Int64 narrowed", live: "Int64", declared: schemamodel.Field{Type: "Int32"},
			wantChange: "Int64 -> Int32", want: "ALTER TABLE wid MODIFY COLUMN d Int32"},
		{name: "Int8 widened", live: "Int8", declared: schemamodel.Field{Type: "Int16"},
			wantChange: "Int8 -> Int16", want: "ALTER TABLE wid MODIFY COLUMN d Int16"},
		{name: "UInt8 widened", live: "UInt8", declared: schemamodel.Field{Type: "UInt16"},
			wantChange: "UInt8 -> UInt16", want: "ALTER TABLE wid MODIFY COLUMN d UInt16"},
		{name: "signedness", live: "Int32", declared: schemamodel.Field{Type: "UInt32"},
			wantChange: "Int32 -> UInt32", want: "ALTER TABLE wid MODIFY COLUMN d UInt32"},
		{name: "a portable declaration widens", live: "Int32", declared: schemamodel.Field{Type: "BIGINT"},
			wantChange: "Int32 -> Int64", want: "ALTER TABLE wid MODIFY COLUMN d Int64"},
		{name: "Float32 widened", live: "Float32", declared: schemamodel.Field{Type: "Float64"},
			wantChange: "Float32 -> Float64", want: "ALTER TABLE wid MODIFY COLUMN d Float64"},
		{name: "decimal precision", live: "Decimal(9, 2)", declared: schemamodel.Field{Type: "Decimal(18, 2)"},
			wantChange: "Decimal(9, 2) -> Decimal(18, 2)", want: "ALTER TABLE wid MODIFY COLUMN d Decimal(18, 2)"},
		{name: "DateTime to DateTime64", live: "DateTime", declared: schemamodel.Field{Type: "DateTime64(3)"},
			wantChange: "DateTime -> DateTime64(3)", want: "ALTER TABLE wid MODIFY COLUMN d DateTime64(3)"},
		{name: "enum width", live: "Enum8('a' = 1)", declared: schemamodel.Field{Type: "Enum16('a' = 1)"},
			wantChange: "Enum8('a' = 1) -> Enum16('a' = 1)", want: "ALTER TABLE wid MODIFY COLUMN d Enum16('a' = 1)"},
		{name: "array element width", live: "Array(Int32)", declared: schemamodel.Field{Type: "Array(Int64)"},
			wantChange: "Array(Int32) -> Array(Int64)", want: "ALTER TABLE wid MODIFY COLUMN d Array(Int64)"},
		{name: "low cardinality removed", live: "LowCardinality(String)", declared: schemamodel.Field{Type: "String"},
			wantChange: "LowCardinality(String) -> String", want: "ALTER TABLE wid MODIFY COLUMN d String"},
		{name: "nullable width, wrapper declared", live: "Nullable(Int32)",
			declared:   schemamodel.Field{Type: "Nullable(Int64)", Nullable: true},
			wantChange: "Int32 -> Int64", want: "ALTER TABLE wid MODIFY COLUMN d Nullable(Int64)"},
		{name: "nullable width, flag declared", live: "Nullable(Int32)",
			declared:   schemamodel.Field{Type: "BIGINT", Nullable: true},
			wantChange: "Int32 -> Int64", want: "ALTER TABLE wid MODIFY COLUMN d Nullable(Int64)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), widDeclared(test.declared), widLive(test.live), platform.ClickHouse, must.Must(builtin.New())))

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, platform.ClickHouse, planner.Options{Capabilities: capability.ClickHouse2411()},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []string{test.want})
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals, map[string]string{"type": test.wantChange})
		})
	}
}

// A declaration that spells the live type another way plans nothing. Each
// live type here is what system.columns stores for the declaration beside it,
// measured on 24.10 and 26.9, or what the renderer writes for a portable one.
func TestGenerateSchemaDiffSQLStatements_ClickHouseTypeWidth_SameTypePlansNothing(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		declared schemamodel.Field
	}{
		{name: "the same spelling", live: "Int64", declared: schemamodel.Field{Type: "Int64"}},
		{name: "a native Int8", live: "Int8", declared: schemamodel.Field{Type: "Int8"}},
		{name: "a native DateTime", live: "DateTime", declared: schemamodel.Field{Type: "DateTime"}},
		{name: "a native DateTime with a time zone", live: "DateTime('UTC')", declared: schemamodel.Field{Type: "DateTime('UTC')"}},
		{name: "a portable integer", live: "Int32", declared: schemamodel.Field{Type: "INTEGER"}},
		{name: "a portable bigint", live: "Int64", declared: schemamodel.Field{Type: "BIGINT"}},
		{name: "a portable DATETIME", live: "DateTime64(3)", declared: schemamodel.Field{Type: "DATETIME"}},
		{name: "a portable TIMESTAMPTZ", live: "DateTime64(3, 'UTC')", declared: schemamodel.Field{Type: "TIMESTAMPTZ"}},
		{name: "a portable TEXT", live: "String", declared: schemamodel.Field{Type: "TEXT"}},
		{name: "a portable NUMERIC", live: "Decimal(10, 2)", declared: schemamodel.Field{Type: "NUMERIC(10,2)"}},
		{name: "a decimal family", live: "Decimal(9, 2)", declared: schemamodel.Field{Type: "Decimal32(2)"}},
		{name: "an alias of Bool", live: "Bool", declared: schemamodel.Field{Type: "Boolean"}},
		{name: "an enum without a width", live: "Enum8('a' = 1, 'b' = 2)", declared: schemamodel.Field{Type: "Enum('a', 'b')"}},
		{name: "a map without a space", live: "Map(String, UInt64)", declared: schemamodel.Field{Type: "Map(String,UInt64)"}},
		{name: "a nullable column declared with the flag", live: "Nullable(Int32)",
			declared: schemamodel.Field{Type: "Int32", Nullable: true}},
		{name: "a nullable column declared with the wrapper", live: "Nullable(Int32)",
			declared: schemamodel.Field{Type: "Nullable(Int32)", Nullable: true}},
		{name: "a nullable low-cardinality column", live: "LowCardinality(Nullable(String))",
			declared: schemamodel.Field{Type: "LowCardinality(Nullable(String))", Nullable: true}},
		{name: "an array of a nullable type", live: "Array(Nullable(Int32))",
			declared: schemamodel.Field{Type: "Array(Nullable(Int32))"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), widDeclared(test.declared), widLive(test.live), platform.ClickHouse, must.Must(builtin.New())))

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, platform.ClickHouse, planner.Options{Capabilities: capability.ClickHouse2411()},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, 0)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// Comparing two declarations reads both sides as the renderer writes them,
// since neither is a catalog. A portable INT8 is 64 bits wide and ClickHouse's
// own Int8 is 8, so the two differ, and two portable spellings of one type do
// not.
func TestGenerateSchemaDiffSQLStatements_ClickHouseTypeWidthBetweenDeclarations(t *testing.T) {
	tests := []struct {
		name    string
		current string
		desired string
		want    []string
	}{
		{name: "a native Int8 against a portable INT8", current: "Int8", desired: "INT8",
			want: []string{"ALTER TABLE wid MODIFY COLUMN d Int64"}},
		{name: "a portable DATETIME on both sides", current: "DATETIME", desired: "DATETIME", want: make([]string, 0)},
		{name: "two portable spellings of Int32", current: "INT", desired: "INTEGER", want: make([]string, 0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := widDeclared(schemamodel.Field{Type: test.desired})
			current := widDeclared(schemamodel.Field{Type: test.current})
			// Memory has no sorting-key requirement. This fixture varies only
			// the column type, and both documents must describe a valid CREATE.
			desired.Tables[0].Engine = "Memory"
			current.Tables[0].Engine = "Memory"
			diff := must.Must(schemadiff.CompareSchemas(t.Context(), desired, current, platform.ClickHouse, must.Must(builtin.New())))

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, platform.ClickHouse, planner.Options{Capabilities: capability.ClickHouse2411()},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A narrowing the comparison now plans is judged destructive. Measured on 24.10
// and 26.9, the server accepts `MODIFY COLUMN d Int32` over an Int64 and stores
// 30000000000 as -64771072, and a decimal narrowing whose values do not fit
// fails the conversion and leaves the column unreadable, so the safety report
// is what stands between the plan and the data. A widening loses nothing and
// stays a warning.
func TestGenerateSchemaDiffAST_ClickHouseTypeNarrowingIsDestructive(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		declared schemamodel.Field
		want     safety.Severity
	}{
		{name: "Int64 narrowed", live: "Int64", declared: schemamodel.Field{Type: "Int32"}, want: safety.Destructive},
		{name: "signed made unsigned", live: "Int32", declared: schemamodel.Field{Type: "UInt32"}, want: safety.Destructive},
		{name: "nullable narrowed", live: "Nullable(Int64)",
			declared: schemamodel.Field{Type: "Nullable(Int32)", Nullable: true}, want: safety.Destructive},
		{name: "decimal narrowed", live: "Decimal(18, 2)", declared: schemamodel.Field{Type: "Decimal(9, 2)"}, want: safety.Destructive},
		{name: "Int32 widened", live: "Int32", declared: schemamodel.Field{Type: "Int64"}, want: safety.Warning},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), widDeclared(test.declared), widLive(test.live), platform.ClickHouse, must.Must(builtin.New())))

			nodes, err := planner.GenerateSchemaDiffASTWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, platform.ClickHouse, planner.Options{Capabilities: capability.ClickHouse2411()},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			c.Assert(safety.Classify(nodes[0]), qt.Equals, test.want)
		})
	}
}
