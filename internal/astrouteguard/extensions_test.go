package astrouteguard_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/astrouteguard"
)

func TestExtensionKinds_RecognizesInheritedMethods(t *testing.T) {
	c := qt.New(t)
	root := extensionRepository(c)
	kinds, err := astrouteguard.ExtensionKinds(root)
	c.Assert(err, qt.IsNil)
	c.Assert(kinds, qt.DeepEquals, []astrouteguard.ExtensionKind{
		{Package: "ptah.run/feature/sample", Name: "Base"},
		{Package: "ptah.run/feature/sample", Name: "Embedded"},
	})
}

func TestExtensionKinds_RefusesExcludedSource(t *testing.T) {
	c := qt.New(t)
	root := extensionRepository(c)
	ignored := "//go:build ignore\n\npackage sample\n\ntype Hidden struct{ Base }\n"
	c.Assert(os.WriteFile(filepath.Join(root, "feature", "sample", "hidden.go"), []byte(ignored), 0o600), qt.IsNil)
	_, err := astrouteguard.ExtensionKinds(root)
	c.Assert(err, qt.ErrorMatches, `extension contract file is excluded from type checking: .*hidden.go`)
}

func extensionRepository(c *qt.C) string {
	c.Helper()
	root := c.TempDir()
	files := map[string]string{
		"go.mod": "module ptah.run\n\ngo 1.23\n",
		"core/ast/extension.go": `package ast
type ExtensionPayload interface {
 Kind() string
 CloneExtension() ExtensionPayload
}
`,
		"feature/sample/payload.go": `package sample
import "ptah.run/core/ast"
type Base struct{}
func (*Base) Kind() string { return "example.org/base" }
func (*Base) CloneExtension() ast.ExtensionPayload { return &Base{} }
type Embedded struct { Base }
type Alias = Base
type Unrelated struct{}
func (*Unrelated) Kind() int { return 1 }
func (*Unrelated) CloneExtension() ast.ExtensionPayload { return &Base{} }
`,
	}
	for name, source := range files {
		file := filepath.Join(root, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(file), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(file, []byte(source), 0o600), qt.IsNil)
	}
	command := exec.Command("git", "init")
	command.Dir = root
	c.Assert(command.Run(), qt.IsNil)
	return root
}
