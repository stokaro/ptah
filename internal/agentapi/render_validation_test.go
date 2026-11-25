package agentapi_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentapi"
	"ptah.run/internal/agentdiag"
)

// writeYAMLSchema puts a YAML schema whose column n declares the value 'ok ',
// with the platform group of the column, in a temporary directory and returns
// the source that names it.
func writeYAMLSchema(c *qt.C, platform string) agentapi.SchemaSource {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(path, []byte(`tables:
  c:
    columns:
      id: {type: INTEGER, primary: true}
      n:
        enum: [happy, 'ok ']
`+platform), 0o600), qt.IsNil)
	return agentapi.SchemaSource{SchemaFiles: []string{path}}
}

// A render request carries a declared source, so it is validated before it
// is rendered, as `ptah schema render` validates it. Rendered alone, the
// answer would be DDL for a column MySQL builds without the space, while the
// validate request reports the same schema as invalid.
func TestRenderSchema_ValidatesTheDeclaration_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := writeYAMLSchema(c, "")

	response, err := schemaSession(c, source).RenderSchema(context.Background(),
		agentapi.RenderSchemaRequest{Source: source, Dialect: "mysql"})

	code, coded := agentdiag.CodeOf(err)
	c.Assert(coded, qt.IsTrue)
	c.Assert(code, qt.Equals, agentdiag.CodeRenderFailed)
	c.Assert(err, qt.ErrorMatches, `render mysql: column "n" of table "c" declares the enum value "ok ", and mysql `+
		`removes the trailing spaces of an ENUM value, so the column would never match its declaration: declare "ok", `+
		`or name the binary character set on the column or its table; a binary schema default does not count \(stokaro/ptah#6665\)`)
	c.Assert(response, qt.IsNil)
}

// A column of the binary character set keeps the space, so the same value
// renders.
func TestRenderSchema_ValidatesTheDeclaration_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := writeYAMLSchema(c, "        platform:\n          mysql: {charset: binary}\n")

	response, err := schemaSession(c, source).RenderSchema(context.Background(),
		agentapi.RenderSchemaRequest{Source: source, Dialect: "mysql"})

	c.Assert(err, qt.IsNil)
	c.Assert(indexOfContaining(response.Statements, "ENUM('happy', 'ok ') CHARACTER SET binary") >= 0, qt.IsTrue,
		qt.Commentf("statements: %q", response.Statements))
}
