package atlas

// White-box testing required: the refusal guards an unexported field on
// atlasProject that only the project loader sets, and no exported API reports
// whether a migration directory was pulled from a registry.

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasargs"
)

// readOnlyRegistryProject builds the project shape the remote_dir data source
// produces: a virtual directory whose bytes came from a registry and which
// therefore has nowhere to write back to.
func readOnlyRegistryProject(dir atlasargs.LocalDir, readOnly bool) atlasProject {
	return atlasProject{
		migrationDirResolved: true,
		migrationDir:         dir,
		migrationVirtual:     true,
		migrationFS:          fstest.MapFS{"20260101000000_init.sql": {Data: []byte("SELECT 1;")}},
		migrationDisplay:     dir.Path,
		migrationOrigin:      "oci://registry.example/acme/app:latest",
		migrationReadOnly:    readOnly,
	}
}

func TestRefuseWriteToReadOnlyMigrationDirNamesTheRegistryReference(t *testing.T) {
	c := qt.New(t)
	dir := atlasargs.LocalDir{Path: "mem:///remote_dir/app"}

	err := readOnlyRegistryProject(dir, true).
		refuseWriteToReadOnlyMigrationDir(dir, "atlas migrate new")

	c.Assert(err, qt.IsNotNil)
	// The caller never typed the mem:// URL, so the refusal has to name the
	// reference they did type, and say where a write does belong.
	c.Assert(err.Error(), qt.Contains, "oci://registry.example/acme/app:latest")
	c.Assert(err.Error(), qt.Contains, "atlas migrate new")
	c.Assert(err.Error(), qt.Contains, "ptah migrations push")
	c.Assert(err.Error(), qt.Not(qt.Contains), "mem:///")
}

func TestRefuseWriteToReadOnlyMigrationDirAllowsAWritableVirtualDir(t *testing.T) {
	c := qt.New(t)
	dir := atlasargs.LocalDir{Path: "mem:///template_dir/app"}

	// data.template_dir is virtual too, but it writes back to its source, so
	// the refusal must not generalize from "virtual" to "read-only".
	err := readOnlyRegistryProject(dir, false).
		refuseWriteToReadOnlyMigrationDir(dir, "atlas migrate new")

	c.Assert(err, qt.IsNil)
}

func TestRefuseWriteToReadOnlyMigrationDirIgnoresAnOrdinaryLocalDir(t *testing.T) {
	c := qt.New(t)
	project := readOnlyRegistryProject(atlasargs.LocalDir{Path: "mem:///remote_dir/app"}, true)

	// An explicit --dir pointing somewhere real is not the registry directory,
	// even while the project carries one.
	err := project.refuseWriteToReadOnlyMigrationDir(
		atlasargs.LocalDir{Path: "migrations"}, "atlas migrate new")

	c.Assert(err, qt.IsNil)
}
