package yamlschema_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

func TestCanceledYAMLParseDoesNotReadFile(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	db, err := yamlschema.ParseFile(ctx, yamlext.None(), filepath.Join(c.TempDir(), "absent.yaml"))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(db, qt.IsNil)
}

func TestYAMLParseDiscardsResultAfterCancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	owners, err := yamlext.NewSet(yamlext.Extension{Owner: "example.org/cancel", Coverage: func() (schemaext.Coverage, error) {
		cancel()
		return schemaext.Coverage{}, nil
	}})
	c.Assert(err, qt.IsNil)
	db, err := yamlschema.Parse(ctx, owners, []byte("tables: {}\n"))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(db, qt.IsNil)
}
