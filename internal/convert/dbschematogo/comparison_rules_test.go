package dbschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/convert/dbschematogo"
)

// literalDefaults reads a default spelled lit(<value>) as that value, and
// leaves every comparison to the shared rules' callers.
type literalDefaults struct{}

func (literalDefaults) TypeChange(schemamodel.Field, catalog.Column, string) string { return "" }

func (literalDefaults) DefaultChange(_ schemamodel.Field, _ catalog.Column, _, _ string) string {
	return ""
}

func (literalDefaults) AlwaysNotNull(schemamodel.Field) bool { return false }

func (literalDefaults) SequenceChanges(schemamodel.Field, catalog.Column) map[string]string {
	return nil
}

func (literalDefaults) DefaultValue(reported string) (string, bool) {
	return strings.CutSuffix(strings.TrimPrefix(reported, "lit("), ")")
}

// literalCatalog is one table whose column reports default lit(5).
func literalCatalog() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{Name: "items", Type: "TABLE", Columns: []catalog.Column{
		{Name: "price", DataType: "NUMBER", ColumnType: "NUMBER", IsNullable: "YES", ColumnDefault: new("lit(5)")},
	}}}}
}

// TestConvertDBSchemaToGoSchema_ReadsDefaultsThroughTheTargetsRules shows that
// a catalog read back into a declaration carries a reported default as the
// value the target's column rules read it as. The control registers none, and
// the shared rule keeps the default as the expression it was reported as.
func TestConvertDBSchemaToGoSchema_ReadsDefaultsThroughTheTargetsRules(t *testing.T) {
	tests := []struct {
		name        string
		rules       schemacomparison.Rules
		wantDefault string
		wantExpr    string
		wantSet     bool
	}{
		{name: "registered", rules: schemacomparison.Rules{Columns: literalDefaults{}}, wantDefault: "5", wantSet: true},
		{name: "absent", wantExpr: "lit(5)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(engine.New(engine.Provider{ID: "example.org/custom",
				Targets: []engine.Target{{Name: "custom", Comparison: test.rules}}}))

			database, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), literalCatalog(), "custom", runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Fields, qt.HasLen, 1)
			c.Assert(database.Fields[0].Default, qt.Equals, test.wantDefault)
			c.Assert(database.Fields[0].DefaultExpr, qt.Equals, test.wantExpr)
			c.Assert(database.Fields[0].DefaultSet, qt.Equals, test.wantSet)
		})
	}
}

// TestConvertDBSchemaToGoSchema_RefusesAnUnregisteredTarget refuses a catalog
// read from a target the runtime did not register, whose rules for reading a
// default back it cannot know.
func TestConvertDBSchemaToGoSchema_RefusesAnUnregisteredTarget(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{Name: "custom"}}}))

	database, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), literalCatalog(), "unregistered", runtime)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(database, qt.IsNil)
}
