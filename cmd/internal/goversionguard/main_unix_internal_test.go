//go:build !windows

package main

// White-box testing required: the local-action resolver has no exported API,
// and its repository-containment boundary must be exercised through symlinks.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestAuditCompositeActionsRefusesSymlinkedLocalActionEscape(t *testing.T) {
	tests := []struct {
		name      string
		reference string
	}{
		{name: "workspace path", reference: "./vendor/tool"},
		{name: "repository path", reference: "$/vendor/tool"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			outside := c.TempDir()
			writeCurrentCanonicalAction(c, root)
			writeFile(c, root, ".github/workflows/vendor-action.yml", "jobs:\n  test:\n    steps:\n      - uses: "+test.reference+"\n")
			writeFile(c, outside, "action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.6"
`)
			c.Assert(os.MkdirAll(filepath.Join(root, "vendor"), 0o750), qt.IsNil)
			c.Assert(os.Symlink(outside, filepath.Join(root, "vendor", "tool")), qt.IsNil)

			_, err := auditCompositeActions(root, "1.26.6")

			c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
		})
	}
}

func TestAuditCompositeActionsRefusesSymlinkedLocalManifestEscape(t *testing.T) {
	tests := []struct {
		name      string
		reference string
	}{
		{name: "workspace path", reference: "./vendor/tool"},
		{name: "repository path", reference: "$/vendor/tool"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			outside := c.TempDir()
			writeCurrentCanonicalAction(c, root)
			writeFile(c, root, ".github/workflows/vendor-action.yml", "jobs:\n  test:\n    steps:\n      - uses: "+test.reference+"\n")
			writeFile(c, outside, "action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.6"
`)
			c.Assert(os.MkdirAll(filepath.Join(root, "vendor", "tool"), 0o750), qt.IsNil)
			c.Assert(os.Symlink(
				filepath.Join(outside, "action.yml"),
				filepath.Join(root, "vendor", "tool", "action.yml"),
			), qt.IsNil)

			_, err := auditCompositeActions(root, "1.26.6")

			c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
		})
	}
}

func TestAuditCompositeActionsRefusesDiscoveredManifestSymlinkEscape(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	outside := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, outside, "action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.6"
`)
	c.Assert(os.MkdirAll(filepath.Join(root, ".github", "actions", "tool"), 0o750), qt.IsNil)
	c.Assert(os.Symlink(
		filepath.Join(outside, "action.yml"),
		filepath.Join(root, ".github", "actions", "tool", "action.yml"),
	), qt.IsNil)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `(?s).*outside allowed root.*`)
}

func TestAuditCompositeActionsRefusesCanonicalManifestSymlinkEscape(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	outside := c.TempDir()
	writeFile(c, outside, "action.yml", `inputs:
  go-version:
    default: "1.26.6"
runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: ${{ inputs.go-version }}
`)
	c.Assert(os.MkdirAll(filepath.Join(root, ".github", "actions", "ptah"), 0o750), qt.IsNil)
	c.Assert(os.Symlink(
		filepath.Join(outside, "action.yml"),
		filepath.Join(root, ".github", "actions", "ptah", "action.yml"),
	), qt.IsNil)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `(?s).*outside allowed root.*`)
}
