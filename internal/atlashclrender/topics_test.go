package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// A document leaves a YDB topic out, since Atlas HCL has no block for one, and
// says so twice: a loss diagnostic per topic, and a header recording that it
// does not describe topics. A document for another dialect whose schema holds
// no topic records nothing.
func TestRenderForDialect_Topics(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		db          *schemamodel.Database
		diagnostics []atlashclrender.Diagnostic
		recorded    bool
	}{
		{name: "a YDB document holding a topic", dialect: platform.YDB,
			db: &schemamodel.Database{Topics: []schemamodel.Topic{{Name: "events", Schema: "app"}}},
			diagnostics: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning, Path: "topics.app.events",
				Message: "a YDB topic is not represented in HCL"}},
			recorded: true},
		{name: "a YDB document holding none", dialect: platform.YDB, db: &schemamodel.Database{}, recorded: true},
		{name: "a PostgreSQL document holding none", dialect: platform.Postgres, db: &schemamodel.Database{}, recorded: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := atlashclrender.RenderForDialect(test.db, test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, test.diagnostics)
			c.Assert(result.NotDescribed.Describes(coverage.Topic), qt.Equals, !test.recorded)
		})
	}
}
