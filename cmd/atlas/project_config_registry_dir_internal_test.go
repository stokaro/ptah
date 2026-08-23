package atlas

// White-box testing required: the registry branch sets unexported fields on
// atlasProject that decide whether a later verb may write, and no exported API
// reports where a migration directory came from.

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
)

// TestResolveMigrationDirArg_SendsARegistryReferenceToTheRegistryBranch pins
// the dispatch, which is the whole of what this seam decides.
//
// The namespace is deliberately left unset, so the registry branch is
// identified by the refusal it produces rather than by a network call. A
// dispatch that fell through to the local parser would answer "only local
// file:// migration directories are supported" instead, which is exactly what
// `--dir atlas://app` used to say (stokaro/ptah#1210).
func TestResolveMigrationDirArg_SendsARegistryReferenceToTheRegistryBranch(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name:    "a bare repository",
			raw:     "atlas://app",
			wantErr: "atlas:// references require an OCI backing registry",
		},
		{
			name:    "a tagged repository",
			raw:     "atlas://app?tag=prod",
			wantErr: "atlas:// references require an OCI backing registry",
		},
		{
			name:    "a versioned repository",
			raw:     "atlas://app?version=20260806123000",
			wantErr: "atlas:// references require an OCI backing registry",
		},
		{
			name:    "a scheme with no Ptah meaning keeps the local answer",
			raw:     "s3://bucket/migrations",
			wantErr: "only local file:// migration directories are supported",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("PTAH_ATLAS_REGISTRY", "")
			project := atlasProject{}

			_, err := project.resolveMigrationDirArg(context.Background(), test.raw)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.wantErr)
		})
	}
}

// TestResolveMigrationDirArg_LeavesAnOrdinaryPathAlone is the control the
// dispatch needs.
//
// A branch that claimed every value would pass every row above and break every
// ordinary run.
func TestResolveMigrationDirArg_LeavesAnOrdinaryPathAlone(t *testing.T) {
	c := qt.New(t)
	project := atlasProject{}

	dir, err := project.resolveMigrationDirArg(context.Background(), "file://migrations")

	c.Assert(err, qt.IsNil)
	c.Assert(dir.Path, qt.Equals, "migrations")
	c.Assert(project.migrationVirtual, qt.IsFalse)
	c.Assert(project.migrationReadOnly, qt.IsFalse)
}

// TestAdoptVirtualMigrationDir_MarksTheDirectoryReadOnly is what stands between
// a registry directory and a writing verb.
//
// Without the flag the writer is handed a path with no local directory behind
// it, writes nowhere, and the run exits 0 while the operator is told it
// succeeded and the registry is untouched.
func TestAdoptVirtualMigrationDir_MarksTheDirectoryReadOnly(t *testing.T) {
	c := qt.New(t)
	project := atlasProject{}

	project.adoptVirtualMigrationDir(
		"atlas://app", "oci://registry.example/acme/app:latest",
		fstest.MapFS{"1_init.sql": {Data: []byte("SELECT 1;")}})

	c.Assert(project.migrationVirtual, qt.IsTrue)
	c.Assert(project.migrationReadOnly, qt.IsTrue)
	c.Assert(project.migrationDirResolved, qt.IsTrue)
	// No write path at all: joining a registry reference to the project root
	// would create a directory named after it.
	c.Assert(project.migrationWriteDir.Path, qt.Equals, "")
	c.Assert(project.migrationOrigin, qt.Equals, "oci://registry.example/acme/app:latest")
	// The sentinel is a digest rather than the reference, because every verb
	// compares directory identity by path.
	c.Assert(project.migrationDir.Path, qt.Not(qt.Contains), "atlas://")

	err := project.refuseWriteToReadOnlyMigrationDir(project.migrationDir, "atlas migrate new")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "oci://registry.example/acme/app:latest")
}

// TestAdoptVirtualMigrationDir_GivesTwoReferencesTwoIdentities keeps one run's
// directory from being mistaken for another's.
//
// Identity is compared by path everywhere downstream, so two references that
// produced the same sentinel would make an explicit --dir look like the
// project's directory, and the wrong filesystem would be read.
func TestAdoptVirtualMigrationDir_GivesTwoReferencesTwoIdentities(t *testing.T) {
	c := qt.New(t)
	first, second := atlasProject{}, atlasProject{}

	first.adoptVirtualMigrationDir("atlas://app", "oci://r/app:latest", fstest.MapFS{})
	second.adoptVirtualMigrationDir("atlas://app?tag=prod", "oci://r/app:prod", fstest.MapFS{})

	c.Assert(first.migrationDir.Path, qt.Not(qt.Equals), second.migrationDir.Path)
}
