package schemafile_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/hclext"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/engine"
	"ptah.run/internal/schemafile"
)

type sourceContextKey struct{}
type cancelingGrammar struct{ check func(context.Context) }

func (g cancelingGrammar) ReadSQL(ctx context.Context, _ sqlext.GrammarRequest) (sqlext.GrammarResult, error) {
	g.check(ctx)
	return sqlext.GrammarResult{}, ctx.Err()
}
func (g cancelingGrammar) DecodeHCL(ctx context.Context, _ hclext.Request) (hclext.Result, error) {
	g.check(ctx)
	return hclext.Result{}, &hclext.ClaimError{Claim: 0, Err: ctx.Err()}
}

func TestSchemaLoadPropagatesCancellationToOwners(t *testing.T) {
	for _, test := range []struct{ name, file, source string }{
		{"SQL", "schema.sql", "owner syntax"},
		{"HCL", "schema.hcl", `shape "owned" {}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(context.WithValue(t.Context(), sourceContextKey{}, "load"))
			defer cancel()
			calls := 0
			owner := cancelingGrammar{check: func(received context.Context) {
				calls++
				c.Assert(received.Value(sourceContextKey{}), qt.Equals, "load")
				cancel()
				c.Assert(received.Err(), qt.ErrorIs, context.Canceled)
			}}
			runtime, err := engine.New(engine.Provider{ID: "example.org/cancel", Targets: []engine.Target{{Name: "custom"}},
				SQL: []sqlext.Extension{{Owner: "example.org/cancel", Targets: []string{"custom"}, Grammar: owner}},
				HCL: []hclext.Extension{{Owner: "example.org/cancel", Decoder: owner, Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName, Dialects: []string{"custom"}}}}},
			})
			c.Assert(err, qt.IsNil)
			path := filepath.Join(c.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.source), 0o600), qt.IsNil)
			db, err := schemafile.LoadSources(ctx, []schemafile.Source{{URL: path}}, schemafile.Options{Dialect: "custom", YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL()})
			c.Assert(err, qt.ErrorIs, context.Canceled)
			c.Assert(db, qt.IsNil)
			c.Assert(calls, qt.Equals, 1)
		})
	}
}

func TestCanceledSchemaLoadDoesNotReadTheFilesystem(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	db, err := schemafile.LoadPath(ctx, filepath.Join(c.TempDir(), "absent.yaml"), schemafile.Options{YAML: yamlext.None()})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(db, qt.IsNil)
}
