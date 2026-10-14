package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

func TestRefreshYAMLRequiresAnOwnerAndAValidSchedule(t *testing.T) {
	for _, test := range []struct {
		name           string
		owners         yamlext.Set
		clause, reason string
	}{
		{"no owner", yamlext.None(), "EVERY 1 HOUR", `unknown key "refresh"`},
		{"unsupported settings", builtintest.Runtime().YAML(), "EVERY 1 HOUR SETTINGS refresh_retries = 2", "refresh SETTINGS is not modeled"},
		{"invalid interval", builtintest.Runtime().YAML(), "EVERY 0 SECOND", "interval must be positive"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse(test.owners, []byte("matviews:\n  mv:\n    body: SELECT 1\n    refresh: "+test.clause+"\n"))
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.reason+".*")
			c.Assert(db, qt.IsNil)
		})
	}
}

func TestRefreshHCLRequiresAnOwnerAndAValidSchedule(t *testing.T) {
	for _, test := range []struct {
		name          string
		owners        hclext.Set
		value, reason string
	}{
		{"no owner", hclext.None(), `"EVERY 1 HOUR"`, `unsupported materialized attribute "refresh"`},
		{"unsupported settings", builtintest.Runtime().HCL(), `"EVERY 1 HOUR SETTINGS refresh_retries = 2"`, "refresh SETTINGS is not modeled"},
		{"invalid after offset", builtintest.Runtime().HCL(), `"AFTER 1 HOUR OFFSET 5 MINUTE"`, "OFFSET"},
		{"wrong value type", builtintest.Runtime().HCL(), "false", "must be a string"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.ParseWithOptions([]byte("materialized \"mv\" {\n as = \"SELECT 1\"\n refresh = "+test.value+"\n}"), "schema.hcl", atlashcl.Options{HCL: test.owners})
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.reason+".*")
			c.Assert(db, qt.IsNil)
		})
	}
}

func TestRefreshHCLExportRefusesUncapturedSchedules(t *testing.T) {
	view := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "", "mv")
	for _, state := range []schemaext.KnowledgeState{schemaext.Uninspected, schemaext.Unrepresentable} {
		for _, known := range []schemaext.Coverage{
			must.Must(chschema.RefreshCoverage(schemaext.Desired, schemaext.Knowledge{State: state, Reason: "not read"}, nil)),
			must.Must(chschema.RefreshCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
				{Kind: chschema.RefreshKind, Subject: view, Knowledge: schemaext.Knowledge{State: state, Reason: "not read"}},
			})),
		} {
			c := qt.New(t)
			db := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT 1"}}, FeatureCoverage: known}
			result, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "clickhouse"})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*not captured \(not read\).*HCL schema.*`)
			c.Assert(result.Data, qt.IsNil)
		}
	}
}
