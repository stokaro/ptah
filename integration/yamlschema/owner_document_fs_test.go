//go:build integration

package yamlschema_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

func TestOwnerDocumentRetainsFilenameAndNullPresence(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(path, []byte("shades: null\n"), 0o600), qt.IsNil)
	owners, err := yamlext.NewSet(yamlext.Extension{Owner: "example.org/paint", Sections: []yamlext.Section{{Key: "shades"}, {Key: "palettes"}},
		Document: func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
			c.Assert(document.Filename, qt.Equals, path)
			c.Assert(document.Fragments, qt.HasLen, 1)
			c.Assert(document.Fragments[0].Path, qt.DeepEquals, []string{"shades"})
			c.Assert(document.Fragments[0].Node.Tag, qt.Equals, "!!null")
			return yamlext.DocumentResult{}, nil
		},
	})
	c.Assert(err, qt.IsNil)
	_, err = yamlschema.ParseFile(t.Context(), owners, path)
	c.Assert(err, qt.IsNil)
}
