package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaprojection"
	"ptah.run/engine"
)

// textReadBack reports every column as TEXT.
type textReadBack struct{}

func (textReadBack) ColumnType(string) (string, bool)     { return "TEXT", true }
func (textReadBack) IndexMethod(string) (string, bool)    { return "", false }
func (textReadBack) UniqueIndexDetail(catalog.Index) bool { return false }

func readBackProvider(readBack schemaprojection.ReadBack) engine.Provider {
	return engine.Provider{ID: "example.org/readback", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, ReadBack: readBack}}}
}

func TestReadBack_HappyPath(t *testing.T) {
	t.Run("the registered service under an alias", func(t *testing.T) {
		c := qt.New(t)
		runtime := mustRuntime(c, readBackProvider(textReadBack{}))

		got, err := runtime.ReadBack("alternate")

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, schemaprojection.ReadBack(textReadBack{}))
	})

	t.Run("a target with no service", func(t *testing.T) {
		c := qt.New(t)
		runtime := mustRuntime(c, readBackProvider(nil))

		got, err := runtime.ReadBack("custom")

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.IsNil)
	})
}

func TestReadBack_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, readBackProvider(textReadBack{}))

	got, err := runtime.ReadBack("unregistered")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(got, qt.IsNil)
}

func TestNew_RefusesATypedNilReadBack(t *testing.T) {
	c := qt.New(t)
	var readBack *textReadBack

	runtime, err := engine.New(readBackProvider(readBack))

	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(err, qt.ErrorMatches, `.*typed-nil read-back service`)
	c.Assert(runtime, qt.IsNil)
}
