//go:build !windows

package manageddata_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/manageddata"
	"ptah.run/core/schemamodel"
)

// TestLoaders_RefuseASymlinkLeavingTheRoot holds the shape a checkout can carry
// that no spelling reveals: the name stays inside and the bytes do not.
func TestLoaders_RefuseASymlinkLeavingTheRoot(t *testing.T) {
	setup := qt.New(t)
	project, sourceDir, outside := rowFileProject(setup)
	setup.Assert(os.Symlink(outside, filepath.Join(sourceDir, "link.yaml")), qt.IsNil)
	setup.Assert(os.Symlink(filepath.Dir(outside), filepath.Join(sourceDir, "linkdir")), qt.IsNil)

	tests := []struct {
		name string
		file string
	}{
		{name: "the file is the link", file: "link.yaml"},
		{name: "a directory on the way is the link", file: "linkdir/secret.yaml"},
	}
	for _, loader := range loaders() {
		for _, test := range tests {
			t.Run(loader.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				count, err := loader.load(project, schemamodel.ManagedData{
					Table: "countries", File: test.file, SourceDir: sourceDir,
				})
				c.Assert(err, qt.ErrorIs, manageddata.ErrOutsideRoot)
				c.Assert(count, qt.Equals, 0)
			})
		}
	}
}

// TestLoaders_FollowASymlinkStayingInside is the control for the refusal above.
func TestLoaders_FollowASymlinkStayingInside(t *testing.T) {
	setup := qt.New(t)
	project, sourceDir, _ := rowFileProject(setup)
	setup.Assert(os.Symlink("rows.yaml", filepath.Join(sourceDir, "link.yaml")), qt.IsNil)

	for _, loader := range loaders() {
		t.Run(loader.name, func(t *testing.T) {
			c := qt.New(t)
			count, err := loader.load(project, schemamodel.ManagedData{
				Table: "countries", File: "link.yaml", SourceDir: sourceDir,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(count, qt.Equals, 1)
		})
	}
}
