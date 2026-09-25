package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/migration/schemadiff/difftypes"
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
	compare.TablesAndColumnsWithServerSpellings(desired, current, diff, "postgres",
		identifier.ForDialect("postgres"), compare.CoverageOf(desired, current),
		compare.ServerSpellings{Columns: spellings})
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
// comparison's own folding, which cannot settle these.
func TestColumnSpellings_ADifferentOrMissingSpellingStillPlans(t *testing.T) {
	tests := []struct {
		name       string
		spelling   config.ColumnSpelling
		wantChange string
	}{
		{
			name:       "the type is spelled differently",
			spelling:   config.ColumnSpelling{Type: "character varying(20)[]", Default: "'2020-01-02 00:00:00+00'::timestamp with time zone", Resolved: true},
			wantChange: "type",
		},
		{
			name:       "the default is spelled differently",
			spelling:   config.ColumnSpelling{Type: "character varying(10)[]", Default: "'2020-01-02 00:00:00+00'::timestamp with time zone", Resolved: true},
			wantChange: "default_expr",
		},
		{
			name:       "the server refused the column",
			spelling:   config.ColumnSpelling{Type: "character varying(10)[]", Default: "'2020-01-01 00:00:00+00'::timestamp with time zone"},
			wantChange: "type",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := spelledTable("varchar(10)[]", "'2020-01-01'::timestamp with time zone",
				"character varying(10)[]", "'2020-01-01 00:00:00+00'::timestamp with time zone")

			diff := spellingsDiff(desired, current, map[string]config.ColumnSpelling{spelledColumnKey: test.spelling})

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes[test.wantChange], qt.Not(qt.Equals), "",
				qt.Commentf("changes: %v", diff.TablesModified[0].ColumnsModified[0].Changes))
		})
	}
}
