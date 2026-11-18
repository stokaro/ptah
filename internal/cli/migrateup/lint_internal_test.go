package migrateup

// White-box testing required: lintPendingDestructive is the native command's
// final safety-gate adapter and its compatibility profile is not observable
// through an exported API without opening a real database connection.

import (
	"context"
	"io/fs"
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/internal/migrationsnapshot"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

func TestLintPendingDestructive_DoesNotApplyAtlasFileSuppression(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_drop.up.sql": {
			Data: []byte("-- atlas:nolint destructive\n\nDROP TABLE users;\n"),
		},
		"0000000001_drop.down.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
	}

	findings, err := lintPendingDestructive(c.Context(), builtintest.Runtime(), fsys, migrationlintgate.Pending{Versions: []int64{1}}, "sqlite", "")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
}

// TestLintPendingDestructive_SuppressionSpellingsAtTheApplyGate records which
// directives reopen the apply-time destructive gate, because resolving Atlas
// analyzer names on the native surface changed one of these rows.
//
// The gate has always honored a statement-local directive an author wrote above
// the statement: on a binary built from master, `-- ptah:nolint DS101` and
// `-- atlas:nolint DS101` above `DROP TABLE users;` both take
// `ptah migrations up` from exit 2 to exit 0. `-- atlas:nolint destructive`
// did not, because the analyzer name resolved to nothing natively; it now does,
// which is the row this change moves. The whole-file header keeps blocking, and
// keeps blocking for the same reason as before: the header form is
// compatibility-scoped, and a blank line detaches it from the statement below.
//
// Reverting the change prints []string{"DS101"} for the "Atlas analyzer name"
// row instead of an empty slice.
func TestLintPendingDestructive_SuppressionSpellingsAtTheApplyGate(t *testing.T) {
	tests := []struct {
		name string
		up   string
		want []string
	}{
		{name: "no directive", up: "DROP TABLE users;\n", want: []string{"DS101"}},
		{name: "native code", up: "-- ptah:nolint DS101\nDROP TABLE users;\n", want: make([]string, 0)},
		{name: "Atlas code spelling", up: "-- atlas:nolint DS101\nDROP TABLE users;\n", want: make([]string, 0)},
		{name: "Atlas analyzer name", up: "-- atlas:nolint destructive\nDROP TABLE users;\n", want: make([]string, 0)},
		{name: "Atlas file header", up: "-- atlas:nolint destructive\n\nDROP TABLE users;\n", want: []string{"DS101"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fstest.MapFS{
				"0000000001_drop.up.sql":   {Data: []byte(test.up)},
				"0000000001_drop.down.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
			}

			findings, err := lintPendingDestructive(c.Context(), builtintest.Runtime(), fsys, migrationlintgate.Pending{Versions: []int64{1}}, "sqlite", "")

			c.Assert(err, qt.IsNil)
			codes := make([]string, 0, len(findings))
			for _, finding := range findings {
				codes = append(codes, finding.Rule)
			}
			c.Assert(codes, qt.DeepEquals, test.want)
		})
	}
}

func TestLintPendingDestructive_HonorsDirectoryPrefixedExclusion(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {
			Data: []byte("rules:\n  DS101:\n    exclude:\n      - migrations/legacy/**\n"),
		},
		"legacy/0000000001_drop.up.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"legacy/0000000001_drop.down.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
	}

	findings, err := lintPendingDestructive(c.Context(), builtintest.Runtime(), fsys, migrationlintgate.Pending{Versions: []int64{1}}, "sqlite", "migrations")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

func TestLintPendingDestructive_RejectsConfiguredDialectMismatch(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {
			Data: []byte("dialect: mysql\n"),
		},
		"0000000001_hash_comment.up.sql": {
			Data: []byte("# decoy; DROP TABLE users;\nCREATE TABLE current_users (id INTEGER);\n"),
		},
		"0000000001_hash_comment.down.sql": {
			Data: []byte("DROP TABLE current_users;\n"),
		},
	}

	findings, err := lintPendingDestructive(c.Context(), builtintest.Runtime(), fsys, migrationlintgate.Pending{Versions: []int64{1}}, "sqlite", "")

	c.Assert(err, qt.ErrorMatches, `lint dialect "mysql" does not match database dialect "sqlite"`)
	c.Assert(findings, qt.IsNil)
}

func TestLintPathPrefixForSource_PreservesLocalSpelling(t *testing.T) {
	c := qt.New(t)

	got := lintPathPrefixForSource("db/migrations", migrationsource.Source{
		Display: "/absolute/db/migrations",
	})

	c.Assert(got, qt.Equals, "db/migrations")
}

func TestLintPathPrefixForSource_UsesCanonicalOCIDisplay(t *testing.T) {
	c := qt.New(t)

	got := lintPathPrefixForSource("oci://REGISTRY.EXAMPLE/acme/migrations:latest", migrationsource.Source{
		Display: "oci://registry.example/acme/migrations:latest",
		OCI:     &migrationsource.OCI{},
	})

	c.Assert(got, qt.Equals, "oci://registry.example/acme/migrations:latest")
}

func TestCapturedMigrationsFeedProviderAndDestructiveGateFromSameBytes(t *testing.T) {
	c := qt.New(t)
	source := &changingMigrationFS{
		files: fstest.MapFS{
			"0000000001_drop.up.sql": {
				Data: []byte("DROP TABLE users;\n"),
			},
			"0000000001_drop.down.sql": {
				Data: []byte("CREATE TABLE users (id INTEGER);\n"),
			},
		},
		reads: make(map[string]int),
	}

	snapshot, err := migrationsnapshot.Capture(source)
	c.Assert(err, qt.IsNil)
	provider, err := migrator.NewFSMigrationProvider(
		snapshot,
		migrator.WithMigrationDirFormat(migrationfile.DirFormatPtah),
	)
	c.Assert(err, qt.IsNil)
	findings, err := lintPendingDestructive(c.Context(), builtintest.Runtime(), snapshot, migrationlintgate.Pending{Versions: []int64{1}}, "sqlite", "")
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].UpSQL, qt.Equals, "DROP TABLE users;\n")
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
	c.Assert(source.reads, qt.DeepEquals, map[string]int{
		"0000000001_drop.down.sql": 1,
		"0000000001_drop.up.sql":   1,
	})
}

func TestLockedDestructiveLintHookUsesLockedPlanVersions(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_create.up.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
		"0000000001_create.down.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"0000000002_drop.up.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"0000000002_drop.down.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
	}
	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), fsys, "sqlite")
	c.Assert(err, qt.IsNil)
	hook := lockedDestructiveLintHook(builtintest.Runtime(), fsys, policy, "", nil)

	err = hook(context.Background(), migrator.MigrationPlan{
		Versions: []int64{2},
	})

	c.Assert(err, qt.ErrorMatches, "(?s)pending migrations contain destructive statements.*0000000002_drop.up.sql.*DS101.*")
}

func TestLockedDestructiveLintHookAllowsSafeLockedPlan(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_create.up.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
		"0000000001_create.down.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"0000000002_drop.up.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"0000000002_drop.down.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
	}
	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), fsys, "sqlite")
	c.Assert(err, qt.IsNil)
	hook := lockedDestructiveLintHook(builtintest.Runtime(), fsys, policy, "", nil)

	err = hook(context.Background(), migrator.MigrationPlan{
		Versions: []int64{1},
	})

	c.Assert(err, qt.IsNil)
}

type changingMigrationFS struct {
	files fstest.MapFS
	reads map[string]int
}

func (f *changingMigrationFS) Open(name string) (fs.File, error) {
	return f.files.Open(name)
}

func (f *changingMigrationFS) ReadFile(name string) ([]byte, error) {
	f.reads[name]++
	contents, err := fs.ReadFile(f.files, name)
	responses := [][]byte{contents, []byte("SELECT 1;\n")}
	return slices.Clone(responses[min(f.reads[name]-1, 1)]), err
}

// regeneratedTotalFS drops the column total of orders and adds it back as a
// generated column.
var regeneratedTotalFS = fstest.MapFS{
	"0000000001_orders.up.sql": {
		Data: []byte("CREATE TABLE orders (id integer, price numeric, qty integer, total numeric);\n"),
	},
	"0000000002_total.up.sql": {
		Data: []byte("ALTER TABLE orders DROP COLUMN total;\n" +
			"ALTER TABLE orders ADD COLUMN total numeric GENERATED ALWAYS AS (price * qty) STORED;\n"),
	},
}

// targetColumns reports total as the target holds it, and records the version
// the hook asked for.
type targetColumns struct {
	generated bool
	asked     []int64
}

func (t *targetColumns) read(_ context.Context, version int64) ([]lintext.BaselineColumn, error) {
	t.asked = append(t.asked, version)
	return []lintext.BaselineColumn{{Version: version, Schema: "public", Table: "orders", Name: "total", Generated: t.generated}}, nil
}

// regeneratedTotalHook is the gate over regeneratedTotalFS for a PostgreSQL
// target that reports total as target holds it.
func regeneratedTotalHook(c *qt.C, target *targetColumns) migrator.PreMigrationHook {
	c.Helper()
	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), regeneratedTotalFS, "postgres")
	c.Assert(err, qt.IsNil)
	return lockedDestructiveLintHook(builtintest.Runtime(), regeneratedTotalFS, policy, "", target.read)
}

// The gate reads the target's generated columns on the path where it would
// refuse, under the first pending version, and lets a generated column the
// migration adds back as generated through.
func TestLockedDestructiveLintHookReadsTheTargetsGeneratedColumns_HappyPath(t *testing.T) {
	c := qt.New(t)
	target := &targetColumns{generated: true}

	err := regeneratedTotalHook(c, target)(context.Background(), migrator.MigrationPlan{Versions: []int64{2}})

	c.Assert(err, qt.IsNil)
	c.Assert(target.asked, qt.DeepEquals, []int64{2})
}

// A column that stores what clients wrote, turned into a generated one, is
// refused after the read.
func TestLockedDestructiveLintHookReadsTheTargetsGeneratedColumns_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := &targetColumns{generated: false}

	err := regeneratedTotalHook(c, target)(context.Background(), migrator.MigrationPlan{Versions: []int64{2}})

	c.Assert(err, qt.ErrorMatches, "(?s)pending migrations contain destructive statements.*0000000002_total.up.sql:1 DS102.*")
	c.Assert(target.asked, qt.DeepEquals, []int64{2})
}
