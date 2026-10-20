package pgdefaultprivilegesource_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/goschema"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
)

func TestSources_RenderThroughBuiltinComposition(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	fromGo := must.Must(goschema.ParseSource(runtime.Annotations(), "privileges.go", goSource(annotationPrefix+` schema="app" privileges="SELECT,INSERT" grantable="INSERT"`)))
	fromYAML := must.Must(yamlschema.Parse(t.Context(), runtime.YAML(), []byte(yamlPrefix+"    schema: app\n    privileges: [SELECT, INSERT]\n    grantable: INSERT\n")))
	fromHCL := must.Must(parseHCL(t.Context(), hclBlock(hclIdentity+"schema = schema.app\nprivileges = [SELECT, INSERT]\ngrantable = [INSERT]")))
	fromSQL := readSQL(c, alterDefault+"GRANT SELECT ON TABLES TO reader;\n"+alterDefault+"GRANT INSERT ON TABLES TO reader WITH GRANT OPTION;")
	for _, db := range []*schemamodel.Database{&fromGo, fromYAML, fromHCL, &fromSQL} {
		whole, err := renderer.RenderSchema(t.Context(), runtime, renderer.SchemaRequest{Target: "postgres", Capabilities: capability.Postgres18(), Identifiers: identifier.ForDialect("postgres"), Schema: db})
		c.Assert(err, qt.IsNil)
		c.Assert(whole.Omissions, qt.HasLen, 0)
		c.Assert(strings.Count(strings.Join(whole.Statements, "\n"), "ALTER DEFAULT PRIVILEGES FOR ROLE"), qt.Equals, 2)
		c.Assert(renderDeclarations(c, db), qt.DeepEquals, []string{
			`ALTER DEFAULT PRIVILEGES FOR ROLE "owner" IN SCHEMA "app" GRANT SELECT ON TABLES TO "reader";`,
			`ALTER DEFAULT PRIVILEGES FOR ROLE "owner" IN SCHEMA "app" GRANT INSERT ON TABLES TO "reader" WITH GRANT OPTION;`,
		})
	}
}

func renderDeclarations(c *qt.C, db *schemamodel.Database) []string {
	c.Helper()
	request := featureplan.DeclarationRequest{Target: "postgres", Identifiers: identifier.ForDialect("postgres"),
		Capabilities: capability.Postgres17(), Objects: must.Must(db.FeatureObjects.All())}
	planned, err := builtintest.Runtime().PlanDeclarations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	var statements []string
	for _, contribution := range planned.Contributions {
		for _, step := range contribution.Steps {
			sql, err := builtin.RenderSQL("postgres", &ast.ExtensionStatement{Payload: step.Payload.Payload})
			c.Assert(err, qt.IsNil)
			statements = append(statements, strings.TrimSpace(sql))
		}
	}
	return statements
}
