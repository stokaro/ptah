package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/engine/builtin"
	"ptah.run/internal/ydbstream"
	"ptah.run/migration/safety"
)

func TestStreamingQueryCreateGuardsAgreeAcrossAssessments(t *testing.T) {
	for _, test := range []struct {
		name     string
		options  ydbstream.CreateOptions
		severity safety.Severity
	}{
		{name: "create", severity: safety.Safe},
		{name: "replace", options: ydbstream.CreateOptions{OrReplace: true}, severity: safety.Destructive},
		{name: "guard", options: ydbstream.CreateOptions{IfNotExists: true}, severity: safety.Safe},
		{name: "guarded replacement", options: ydbstream.CreateOptions{OrReplace: true, IfNotExists: true}, severity: safety.Safe},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			payload := &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Name: "copy", Creation: ydbast.StreamingCreation{OrReplace: test.options.OrReplace, IfNotExists: test.options.IfNotExists},
				Spec: ast.StreamingQuerySpec{Text: "INSERT INTO sink SELECT * FROM source;"}}
			node := &ast.ExtensionStatement{Payload: payload}
			assessed := safety.Assess([]ast.Node{node})
			c.Assert(assessed[0].Severity, qt.Equals, test.severity)
			rendered, err := safety.AssessRenderedWithCapabilities(c.Context(), must.Must(builtin.New()), []ast.Node{node}, "ydb", capability.YDB262().With(capability.StreamingQueries, true))
			c.Assert(err, qt.IsNil)
			c.Assert(rendered[0].Severity, qt.Equals, test.severity)
			c.Assert(safety.AssessSQL(ydbstream.Create(payload.QualifiedName(), payload.Spec, test.options)).Severity, qt.Equals, test.severity)
		})
	}
}
