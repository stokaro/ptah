package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

func TestRegisteredTargetSpellingsPreserveDeclarationScope(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	for _, spelling := range platform.DialectSpellings() {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			selected := must.Must(runtime.ResolveTarget(" " + strings.ToUpper(spelling) + " "))
			c.Assert(selected.Name(), qt.Equals, platform.NormalizeDialect(spelling))
			schema := &schemamodel.Database{Functions: []schemamodel.Function{{Name: "scoped", Dialects: []string{spelling}}}}
			projected := must.Must(schemamodel.ScopeToTarget(schema, selected))
			c.Assert(projected.Functions, qt.HasLen, 1)
		})
	}
	// Transport aliases previously missed by the renderer's advertised subset.
	for _, spelling := range []string{"ydbs", "mysql+unix", "maria+unix", "libsql+ws", "pgx"} {
		selected := must.Must(runtime.ResolveTarget(spelling))
		c.Assert(selected.Includes([]string{spelling}), qt.IsTrue)
	}
}
