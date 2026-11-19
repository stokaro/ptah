package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/migration/schemadiff/internal/compare"
)

// spelledTable is one declared column against the catalog's read-back of it.
func spelledTable(declaredType, declaredDefault, liveType, liveDefault string) (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "integer", Primary: true},
			{StructName: "T", Name: "c", Type: declaredType, Nullable: true, DefaultExpr: declaredDefault},
		},
	}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "t", Type: "BASE TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "c", DataType: liveType, UDTName: liveType, IsNullable: "YES", ColumnDefault: optional(liveDefault)},
	}}}}
	return desired, current
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func spellingsDiff(desired *schemamodel.Database, current *catalog.Database, spellings map[string]config.ColumnSpelling) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	compare.TablesAndColumnsWithTableContext(desired, current, diff, "postgres",
		identifier.ForDialect("postgres"), compare.CoverageOf(desired, current),
		compare.TableContext{Columns: spellings}, capability.Capabilities{})
	return diff
}

var spelledColumnKey = exprkey.Column("postgres", "", "t", "c")

// A column the server spells the way the catalog reports it is the same
// column, whatever text declared it (stokaro/ptah#3617).
func TestColumnSpellings_TheServersSpellingSettlesTheColumn(t *testing.T) {
	tests := []struct {
		name                          string
		declaredType, declaredDefault string
		liveType, liveDefault         string
	}{
		{
			name: "an array of varchar", declaredType: "varchar(10)[]",
			liveType: "character varying(10)[]",
		},
		{
			name: "a timestamp default", declaredType: "timestamptz", declaredDefault: "'2020-01-01'::timestamp with time zone",
			liveType: "timestamp with time zone", liveDefault: "'2020-01-01 00:00:00+00'::timestamp with time zone",
		},
		{
			name: "a chain of casts", declaredType: "text", declaredDefault: "'x'::text::character varying",
			liveType: "text", liveDefault: "('x'::text)::character varying",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := spelledTable(test.declaredType, test.declaredDefault, test.liveType, test.liveDefault)

			diff := spellingsDiff(desired, current, map[string]config.ColumnSpelling{
				spelledColumnKey: {Type: test.liveType, Default: test.liveDefault, Resolved: true},
			})

			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// The server's spelling is compared, not trusted: a spelling that differs from
// the catalog's is still a difference, and an unresolved one falls back to the
// comparison's own folding, which tells two widths apart and cannot settle a default.
func TestColumnSpellings_ADifferentOrMissingSpellingStillPlans(t *testing.T) {
	tests := []struct {
		name       string
		declared   string
		spelling   config.ColumnSpelling
		wantChange string
	}{
		{
			name:       "the type is spelled differently",
			declared:   "varchar(20)[]",
			spelling:   config.ColumnSpelling{Type: "character varying(20)[]", Default: "'2020-01-02 00:00:00+00'::timestamp with time zone", Resolved: true},
			wantChange: "type",
		},
		{
			name:       "the default is spelled differently",
			declared:   "varchar(10)[]",
			spelling:   config.ColumnSpelling{Type: "character varying(10)[]", Default: "'2020-01-02 00:00:00+00'::timestamp with time zone", Resolved: true},
			wantChange: "default_expr",
		},
		{
			name:       "the server refused the column",
			declared:   "varchar(20)[]",
			spelling:   config.ColumnSpelling{Type: "character varying(10)[]", Default: "'2020-01-01 00:00:00+00'::timestamp with time zone"},
			wantChange: "type",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := spelledTable(test.declared, "'2020-01-01'::timestamp with time zone",
				"character varying(10)[]", "'2020-01-01 00:00:00+00'::timestamp with time zone")

			diff := spellingsDiff(desired, current, map[string]config.ColumnSpelling{spelledColumnKey: test.spelling})

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes[test.wantChange], qt.Not(qt.Equals), "",
				qt.Commentf("changes: %v", diff.TablesModified[0].ColumnsModified[0].Changes))
		})
	}
}

// A declared default the server spells otherwise than the catalog holds it is
// read by the value it stands for, so two spellings of one value are the same
// default. The second row is the catalog after ALTER COLUMN TYPE, which keeps
// the default as the old type spelled it.
func TestColumnSpellings_ADefaultSpelledOtherwiseIsReadByValue(t *testing.T) {
	tests := []struct {
		name           string
		declaredType   string
		declared       string
		spelled        string
		liveType, live string
	}{
		{
			name: "a cast to text on a varchar column", declaredType: "varchar(10)", declared: "'x'::text",
			spelled: "'x'::text", liveType: "character varying(10)", live: "'x'::character varying",
		},
		{
			name: "a default kept from the old type", declaredType: "text", declared: "'x'",
			spelled: "'x'::text", liveType: "text", live: "'x'::character varying",
		},
		{
			name: "a cast of an integer to bigint", declaredType: "bigint", declared: "(0::bigint)",
			spelled: "'0'::bigint", liveType: "bigint", live: "0",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := spelledTable(test.declaredType, test.declared, test.liveType, test.live)

			diff := spellingsDiff(desired, current, map[string]config.ColumnSpelling{
				spelledColumnKey: {Type: test.liveType, Default: test.spelled, Resolved: true},
			})

			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// A declared default the server spells otherwise than the catalog holds it,
// with a cast that changes the value, is another default.
func TestColumnSpellings_ADefaultSpelledOtherwiseWithAnotherValueIsAChange(t *testing.T) {
	tests := []struct {
		name           string
		declaredType   string
		declared       string
		spelled        string
		liveType, live string
	}{
		{
			name: "a cast that rounds", declaredType: "numeric", declared: "(1.5::integer)",
			spelled: "(1.5)::integer", liveType: "numeric", live: "1.5",
		},
		{
			name: "a length that truncates", declaredType: "varchar(10)", declared: "'abcd'::varchar(3)",
			spelled: "'abcd'::character varying(3)", liveType: "character varying(10)", live: "'abcd'::character varying",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := spelledTable(test.declaredType, test.declared, test.liveType, test.live)

			diff := spellingsDiff(desired, current, map[string]config.ColumnSpelling{
				spelledColumnKey: {Type: test.liveType, Default: test.spelled, Resolved: true},
			})

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals,
				map[string]string{"default_expr": test.live + " -> " + test.declared})
		})
	}
}
