//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema"
)

// capabilityReportDefaultLogLevel is the threshold
// cmd/internal/cliobs.QuietDefaultLogger installs before any command runs, and
// therefore the level at which a library slog call reaches a user's stderr on
// a default invocation. It is duplicated rather than imported because cliobs
// lives under cmd/internal.
const capabilityReportDefaultLogLevel = slog.LevelWarn

// TestLiveCapabilityResolutionStaysOffDefaultStderrE2E pins that connecting to
// a supported server writes nothing to the error stream.
//
// It has to run against a live server because the defect is a function of the
// banner the server reports. `postgres:18` is what
// .github/workflows/go-integration-tests.yml and docker-compose.yaml pin, and
// PostgreSQL 18 is past the newest measured capability line (17), so the
// resolution is saturated on exactly the image CI runs. A diagnostic emitted
// for that condition fires on every single connection, and every test that
// asserts a clean error stream goes red — which is what happened: 25 subtests
// across four migrate-lint E2E files.
//
// The test is written to hold on any supported PostgreSQL, not only 18: the
// contract is "a clean run against a server Ptah supports emits nothing at the
// default level", saturated or not. The resolution is asserted to have seen
// the real banner so the assertion cannot pass by never connecting.
func TestLiveCapabilityResolutionStaysOffDefaultStderrE2E(t *testing.T) {
	dbURL := requirePostgresE2EDatabaseURL(t)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c.Run("the library writes nothing at the default log level", func(c *qt.C) {
		var output bytes.Buffer
		previousLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{
			Level: capabilityReportDefaultLogLevel,
		})))
		defer slog.SetDefault(previousLogger)

		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer conn.Close()

		info := conn.Info()
		c.Assert(output.String(), qt.Equals, "",
			qt.Commentf("emitted on default stderr for %s %q", info.Dialect, info.Version))

		// Non-vacuity: the buffer being empty means something only if a live
		// banner was actually resolved.
		c.Assert(info.Version, qt.Not(qt.Equals), "")
		resolution := capability.ResolveServerVersion(info.Dialect, info.Version)
		c.Assert(resolution.Capabilities, qt.Not(qt.IsNil))
		c.Assert(resolution.Saturated && resolution.VersionSpecific, qt.IsFalse,
			qt.Commentf("version %q", info.Version))
	})

	c.Run("a clean binary run writes nothing to stderr", func(c *qt.C) {
		repoRoot := e2eRepoRoot(t)
		binaryPath := filepath.Join(c.TempDir(), "ptah-compat")
		buildPtahCompat(c, ctx, repoRoot, binaryPath)

		adminDB, err := sql.Open("pgx", dbURL)
		c.Assert(err, qt.IsNil)
		defer adminDB.Close()

		testDBName := fmt.Sprintf("ptah_caps_stderr_e2e_%d", time.Now().UnixNano())
		createE2EDatabase(c, ctx, adminDB, testDBName)
		defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

		migrationsDir := c.TempDir()
		writeLintE2EFile(c, migrationsDir, "1.sql", "CREATE TABLE widgets (id int);\n")

		stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
			"migrate", "lint",
			"--dir", "file://"+migrationsDir,
			"--dev-url", replaceDatabaseName(c, dbURL, testDBName),
			"--latest", "1",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stderr, qt.Equals, "")
	})
}
