package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// asnSchema is a ClickHouse table asn whose column n is the field given.
func asnSchema(n schemamodel.Field) *schemamodel.Database {
	n.StructName, n.Name = "Asn", "n"
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Asn",
			Name:       "asn",
			Overrides:  map[string]map[string]string{platform.ClickHouse: {"engine": "MergeTree", "order_by": "id"}},
		}},
		Fields: []schemamodel.Field{{StructName: "Asn", Name: "id", Type: "INTEGER", Primary: true}, n},
	}
}

// asnDiff compares two states of column n, the way stokaro/ptah#4020 measured
// the plan.
func asnDiff(desired, current schemamodel.Field) *difftypes.SchemaDiff {
	return schemadiff.CompareSchemas(asnSchema(desired), asnSchema(current), platform.ClickHouse)
}

// A ClickHouse column change states the column's default with its type. The
// planner records that the column was nullable and the renderer reads it, so
// the rows here go through the public path that joins the two.
//
// 26.3 and 26.9 refuse a nullable column made non-nullable unless the MODIFY
// COLUMN names a DEFAULT, and fill the NULL rows from it when it does. A MODIFY
// COLUMN naming only a type keeps the default the column had, so a default-only
// change planned that way changed nothing (stokaro/ptah#4020).
func TestGenerateSchemaDiffSQLStatements_ClickHouseColumnDefault_HappyPath(t *testing.T) {
	nullable := schemamodel.Field{Type: "INTEGER", Nullable: true}
	tests := []struct {
		name    string
		caps    capability.Capabilities
		desired schemamodel.Field
		current schemamodel.Field
		want    []string
	}{
		{
			name:    "NOT NULL with a literal default, on 24.11 and above",
			caps:    capability.ClickHouse2411(),
			desired: schemamodel.Field{Type: "INTEGER", Default: "7"},
			current: nullable,
			want: []string{
				"ALTER TABLE asn UPDATE n = '7' WHERE n IS NULL SETTINGS mutations_sync = 2",
				"ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT '7'",
			},
		},
		{
			name:    "NOT NULL with an expression default, on 24.11 and above",
			caps:    capability.ClickHouse2411(),
			desired: schemamodel.Field{Type: "INTEGER", DefaultExpr: "toInt32(40 + 2)"},
			current: nullable,
			want: []string{
				"ALTER TABLE asn UPDATE n = toInt32(40 + 2) WHERE n IS NULL SETTINGS mutations_sync = 2",
				"ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT toInt32(40 + 2)",
			},
		},
		{
			name:    "NOT NULL with a literal default, on 24.10",
			caps:    capability.ClickHouse24(),
			desired: schemamodel.Field{Type: "INTEGER", Default: "7"},
			current: nullable,
			want: []string{
				"ALTER TABLE asn UPDATE n = '7' WHERE n IS NULL SETTINGS mutations_sync = 2",
				"ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT '7'",
			},
		},
		{
			name:    "a default set on a NOT NULL column",
			caps:    capability.ClickHouse2411(),
			desired: schemamodel.Field{Type: "INTEGER", Default: "7"},
			current: schemamodel.Field{Type: "INTEGER"},
			want:    []string{"ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT '7'"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				asnDiff(test.desired, test.current), platform.ClickHouse, planner.Options{Capabilities: test.caps},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A nullable column made NOT NULL without a default is refused on every line,
// rather than planned as a MODIFY COLUMN that 26.3 and later refuse at apply,
// or that 24.10 and 25.8 accept and then fail on a NULL row, leaving the table
// unreadable (stokaro/ptah#4025). No value is invented for the NULL rows.
func TestGenerateSchemaDiffSQLStatements_ClickHouseColumnDefault_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
	}{
		{name: "24.11 and above", caps: capability.ClickHouse2411()},
		{name: "24.10", caps: capability.ClickHouse24()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				asnDiff(schemamodel.Field{Type: "INTEGER"}, schemamodel.Field{Type: "INTEGER", Nullable: true}),
				platform.ClickHouse,
				planner.Options{Capabilities: test.caps},
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `column asn\.n cannot be made NOT NULL without a default: .*; give the column a default for its NULL rows to take`)
			c.Assert(got, qt.IsNil)
		})
	}
}
