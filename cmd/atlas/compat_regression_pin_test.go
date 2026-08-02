package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// The native command tree (#850) reuses the binary-agnostic internals behind
// the Atlas-compatible verbs — atlasmigrate.Set gained native options, the
// migrator's revision-set path gained a ptah-format branch, and the license
// text moved to a shared helper. These pins hold the Atlas-compatible surface
// byte-identical across that refactor.

func runAtlasArgs(args ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestCompatPinAtlasLicenseOutput(t *testing.T) {
	c := qt.New(t)

	out, err := runAtlasArgs("license")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Equals, `Ptah
License: MIT
Copyright (c) 2025, 2026 Denis Voytyuk
Source: https://github.com/stokaro/ptah
Atlas compatibility: independent implementation; Ptah does not use Atlas source code.
`)
}

func TestCompatPinAtlasMigrateSetOutput(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	files := map[string]string{
		"1_users.sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"2_orders.sql": "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(content), 0o600), qt.IsNil)
	}
	dbPath := filepath.Join(t.TempDir(), "pin.db")

	out, err := runAtlasArgs("migrate", "set", "2",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "Current version is 2 (2 set):\n\n  + 1 (users)\n  + 2 (orders)\n\n")

	// Moving the boundary downward keeps the historical removal output.
	out, err = runAtlasArgs("migrate", "set", "1",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "Current version is 1 (1 removed):\n\n  - 2 (orders)\n\n")
}
