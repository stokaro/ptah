package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func TestRuntimeResolvesOnlyFrozenRegisteredTargetNames(t *testing.T) {
	c := qt.New(t)
	provider := engine.Provider{ID: "example.org/selection", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alias"}}}}
	runtime := mustRuntime(c, provider)
	provider.Targets[0].Aliases[0] = "changed"
	selected, err := runtime.ResolveTarget(" ALIAS ")
	c.Assert(err, qt.IsNil)
	c.Assert(selected.Name(), qt.Equals, "custom")
	c.Assert(selected.Names(), qt.DeepEquals, []string{"alias", "custom"})
	_, err = runtime.ResolveTarget("changed")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	_, err = runtime.ResolveTarget("postgres")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	empty := mustRuntime(c)
	selected, err = empty.ResolveTarget("custom")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(selected.Name(), qt.Equals, "")
	c.Assert(selected.Names(), qt.HasLen, 0)
	c.Assert(selected.Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
}
