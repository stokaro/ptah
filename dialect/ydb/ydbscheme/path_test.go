package ydbscheme_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/dialect/ydb/ydbscheme"
)

func TestSchemePathsKeepDirectoryAndNameSeparate(t *testing.T) {
	c := qt.New(t)
	paths := []objectidentity.ID{
		ydbscheme.Path("app", "locks.v1"),
		ydbscheme.Path("app.locks", "v1"),
		ydbscheme.Path("App", "locks.v1"),
		ydbscheme.Path("", "app.locks.v1"),
	}
	seen := make(map[objectidentity.Key]bool)
	for _, path := range paths {
		c.Assert(path.Kind, qt.Equals, ydbscheme.PathKind)
		c.Assert(path.Parent, qt.Equals, objectidentity.Part{})
		c.Assert(seen[path.Key()], qt.IsFalse)
		seen[path.Key()] = true
	}
}
