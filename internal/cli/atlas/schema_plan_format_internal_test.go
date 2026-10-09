package atlas

// White-box testing required: renderAtlasSchemaPlanFormat maps a saved plan
// onto the `schema plan --format` payload, and no feature owner shipped today
// records an access assessment, so no command run can produce a plan that
// carries one for the template to read.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

func TestRenderAtlasSchemaPlanFormat_ExposesTheAccessAssessment(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{Name: "p", Dialect: "postgres", Statements: []atlasschema.PlanStatement{
		{SQL: "CREATE POLICY p ON t USING (true)", Severity: safety.Destructive, Reason: "can widen access: admits every row",
			Access: schemaext.AccessWidens, AccessReason: "admits every row"},
		{SQL: "CREATE TABLE t (id integer)", Severity: safety.Safe, Reason: "does not remove data or tighten constraints"},
	}}

	rendered, err := renderAtlasSchemaPlanFormat(`{{ range .Changes }}{{ .Severity }}|{{ .Access }}|{{ .AccessReason }};{{ end }}`, plan)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Equals, "destructive|widens|admits every row;safe||;")
}
