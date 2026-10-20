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

func TestOwnerFileRetainsNumbersBeyondMachineRanges(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "numbers.yaml")
	source := []byte("shades: {integer: !!int 340282366920938463463374607431768211457, decimal: !!float 1.234567890123456789e1000}\n")
	c.Assert(os.WriteFile(path, source, 0o600), qt.IsNil)
	calls := 0
	owners, err := yamlext.NewSet(yamlext.Extension{Owner: "example.org/paint", Sections: []yamlext.Section{{Key: "shades"}},
		Document: func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
			calls++
			c.Assert(document.Filename, qt.Equals, path)
			c.Assert(document.Source, qt.DeepEquals, source)
			c.Assert(document.Fragments[0].Node.Content[1].Tag, qt.Equals, "!!int")
			c.Assert(document.Fragments[0].Node.Content[1].Value, qt.Equals, "340282366920938463463374607431768211457")
			c.Assert(document.Fragments[0].Node.Content[3].Tag, qt.Equals, "!!float")
			c.Assert(document.Fragments[0].Node.Content[3].Value, qt.Equals, "1.234567890123456789e1000")
			return yamlext.DocumentResult{}, nil
		},
	})
	c.Assert(err, qt.IsNil)
	_, err = yamlschema.ParseFile(t.Context(), owners, path)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
}

func TestOwnerFileRangesKeepOriginalBytes(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "ranges.yaml")
	source := []byte("shades: &colors [\"é😀\"]\r\npalettes: *colors\r\n")
	c.Assert(os.WriteFile(path, source, 0o600), qt.IsNil)
	owners, err := yamlext.NewSet(yamlext.Extension{Owner: "example.org/paint",
		Sections: []yamlext.Section{{Key: "shades"}, {Key: "palettes"}},
		Document: func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
			c.Assert(document.Filename, qt.Equals, path)
			c.Assert(document.Source, qt.DeepEquals, source)
			definition := document.Fragments[0].Node
			alias := document.Fragments[1].Node
			c.Assert(alias.Alias, qt.Equals, definition)
			c.Assert(string(document.Source[definition.Range.Start.ByteOffset:definition.Range.End.ByteOffset]), qt.Equals, "&colors [\"é😀\"]")
			c.Assert(string(document.Source[alias.Range.Start.ByteOffset:alias.Range.End.ByteOffset]), qt.Equals, "*colors")
			return yamlext.DocumentResult{}, nil
		},
	})
	c.Assert(err, qt.IsNil)
	_, err = yamlschema.ParseFile(t.Context(), owners, path)
	c.Assert(err, qt.IsNil)
}
