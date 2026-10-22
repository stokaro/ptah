package providerhost

// White-box testing required: capture refusal must be checked before the
// private adapter can dispatch an incomplete schema to a provider process.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

func validationCapture() planningCapture {
	rules := identifier.Semantics{TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
	return planningCapture{comparisonCapture: comparisonCapture{target: &target{host: &Host{}, engine: &protocol.Engine{Id: "fixture"}}, request: &protocol.CompareModelsRequest{Engine: "fixture"}}, identifiers: rules}
}
func validationSchema() *schemamodel.Database {
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: "records", StructName: "Record", Schema: "app", PrimaryKey: []string{"id"}}}, Fields: []schemamodel.Field{{StructName: "Record", Name: "id", Type: "INTEGER", Comment: "retained"}}, Schemas: []schemamodel.Schema{{Name: "app", Comment: "namespace comment"}}, NotDescribed: coverage.Set{}.WithKind(coverage.Role), Dependencies: map[string][]string{"app.records": {"app.parent"}}}
}
func TestSchemaValidationCaptureRetainsContextAndSource(t *testing.T) {
	c := qt.New(t)
	capture := validationCapture()
	source := validationSchema()
	captured, err := capture.validation(source)
	c.Assert(err, qt.IsNil)
	c.Assert(captured.Tables, qt.HasLen, 1)
	c.Assert(captured.Tables[0].Declaration.Columns[0].Comment, qt.Equals, "retained")
	c.Assert(captured.Namespaces[0].Comment, qt.Equals, "namespace comment")
	c.Assert(captured.NotDescribed[0].Kind, qt.Equals, "role")
	c.Assert(captured.Dependencies["app.records"].Tables, qt.DeepEquals, []string{"app.parent"})
	captured.Tables[0].Declaration.PrimaryKey[0] = "changed"
	captured.Dependencies["app.records"].Tables[0] = "changed"
	c.Assert(source.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})
	c.Assert(source.Dependencies["app.records"], qt.DeepEquals, []string{"app.parent"})
}
func TestSchemaValidationRefusesLostOrAmbiguousDeclarations(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*schemamodel.Database)
	}{
		{"orphan field", func(s *schemamodel.Database) { s.Fields[0].StructName = "Missing" }},
		{"ambiguous field", func(s *schemamodel.Database) {
			s.Tables = append(s.Tables, schemamodel.Table{Name: "other", StructName: "Record"})
		}},
		{"orphan index", func(s *schemamodel.Database) { s.Indexes = []schemamodel.Index{{Name: "idx", TableName: "missing"}} }},
		{"orphan constraint", func(s *schemamodel.Database) {
			s.Constraints = []schemamodel.Constraint{{Name: "ck", Type: "CHECK", Table: "missing"}}
		}},
		{"orphan trigger", func(s *schemamodel.Database) { s.Triggers = []schemamodel.Trigger{{Name: "trg", Table: "missing"}} }},
		{"uncaptured view", func(s *schemamodel.Database) { s.Views = []schemamodel.View{{Name: "visible"}} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			capture := validationCapture()
			source := validationSchema()
			test.change(source)
			result, err := capture.validation(source)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(result, qt.IsNil)
		})
	}
}
