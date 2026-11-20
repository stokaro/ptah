package migrateup

// White-box testing required: lintPendingDestructive is the native command's
// final safety-gate adapter and its compatibility profile is not observable
// through an exported API without opening a real database connection.

import (
	"context"
	"errors"
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
	c.Assert(findings[0].Rule, qt.Equals, "destructive/drop-table")
}

// TestLintPendingDestructive_SuppressionSpellingsAtTheApplyGate records which
// directives reopen the apply-time destructive gate. The gate honors a
// statement-local `ptah:nolint` written above the statement, by rule name. An
// `atlas:nolint` comment, with a code or an analyzer name, is Atlas's
// directive: the native gate refuses it rather than read a code as a rule.
func TestLintPendingDestructive_SuppressionSpellingsAtTheApplyGate(t *testing.T) {
	tests := []struct {
		name string
		up   string
		want []string
	}{
		{name: "no directive", up: "DROP TABLE users;\n", want: []string{"destructive/drop-table"}},
		{name: "native name", up: "-- ptah:nolint destructive/drop-table\nDROP TABLE users;\n", want: nil},
		{name: "native category", up: "-- ptah:nolint destructive\nDROP TABLE users;\n", want: nil},
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
			var codes []string
			for _, finding := range findings {
				codes = append(codes, finding.Rule)
			}
			c.Assert(codes, qt.DeepEquals, test.want)
		})
	}
}

func TestLintPendingDestructive_AtlasDirectivesAreRefusedAtTheApplyGate(t *testing.T) {
	for _, up := range []string{"-- atlas:nolint DS102\nDROP TABLE users;\n", "-- atlas:nolint destructive\nDROP TABLE users;\n"} {
		t.Run(up, func(t *testing.T) {
			c := qt.New(t)
			fsys := fstest.MapFS{
				"0000000001_drop.up.sql":   {Data: []byte(up)},
				"0000000001_drop.down.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
			}

			findings, err := lintPendingDestructive(c.Context(), builtintest.Runtime(), fsys, migrationlintgate.Pending{Versions: []int64{1}}, "sqlite", "")

			c.Assert(err, qt.ErrorMatches, `.*atlas:nolint is an Atlas directive that ptah-compat reads.*`)
			c.Assert(findings, qt.IsNil)
		})
	}
}

func TestLintPendingDestructive_HonorsDirectoryPrefixedExclusion(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {
			Data: []byte("rules:\n  destructive/drop-table:\n    exclude:\n      - migrations/legacy/**\n"),
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
	c.Assert(findings[0].Rule, qt.Equals, "destructive/drop-table")
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

	c.Assert(err, qt.ErrorMatches, "(?s)pending migrations contain destructive statements.*0000000002_drop.up.sql.*destructive/drop-table.*")
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

	c.Assert(err, qt.ErrorMatches, "(?s)pending migrations contain destructive statements.*0000000002_total.up.sql:1 destructive/drop-column.*")
	c.Assert(target.asked, qt.DeepEquals, []int64{2})
}

// storedThenRegeneratedFS makes total of orders a stored column in version 1,
// and drops it and adds it back as generated in version 2.
var storedThenRegeneratedFS = fstest.MapFS{
	"0000000001_stored.up.sql": {
		Data: []byte("ALTER TABLE orders ALTER COLUMN total DROP EXPRESSION;\n"),
	},
	"0000000002_total.up.sql": {
		Data: []byte("ALTER TABLE orders DROP COLUMN total;\n" +
			"ALTER TABLE orders ADD COLUMN total numeric GENERATED ALWAYS AS (price * qty) STORED;\n"),
	},
}

// The target's columns are the state of the first pending version only: an
// earlier pending version may change a column, as version 1 here makes total
// stored, so version 2 is judged without them and refused.
func TestLockedDestructiveLintHookReadsTheTargetsGeneratedColumnsForTheFirstVersion_FailurePath(t *testing.T) {
	c := qt.New(t)
	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), storedThenRegeneratedFS, "postgres")
	c.Assert(err, qt.IsNil)
	target := &targetColumns{generated: true}
	hook := lockedDestructiveLintHook(builtintest.Runtime(), storedThenRegeneratedFS, policy, "", target.read)

	err = hook(context.Background(), migrator.MigrationPlan{Versions: []int64{1, 2}})

	c.Assert(err, qt.ErrorMatches, "(?s)pending migrations contain destructive statements.*0000000002_total.up.sql:1 destructive/drop-column.*")
	c.Assert(target.asked, qt.DeepEquals, []int64{1})
}

// errTargetRead is the failure a target read reports.
var errTargetRead = errors.New("connection reset")

// A failed read of the target refuses the plan, naming the read, rather than
// judging without it or letting the plan through.
func TestLockedDestructiveLintHookRefusesWhenTheTargetReadFails_FailurePath(t *testing.T) {
	c := qt.New(t)
	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), regeneratedTotalFS, "postgres")
	c.Assert(err, qt.IsNil)
	failing := func(context.Context, int64) ([]lintext.BaselineColumn, error) { return nil, errTargetRead }
	hook := lockedDestructiveLintHook(builtintest.Runtime(), regeneratedTotalFS, policy, "", failing)

	err = hook(context.Background(), migrator.MigrationPlan{Versions: []int64{2}})

	c.Assert(err, qt.ErrorIs, errTargetRead)
	c.Assert(err, qt.ErrorMatches, "error reading the generated columns of the target: connection reset")
}

// Where the target is not PostgreSQL the hook reads nothing, and the drop of a
// column added back as generated stays refused.
func TestLockedDestructiveLintHookWithoutATargetRead_FailurePath(t *testing.T) {
	c := qt.New(t)
	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), regeneratedTotalFS, "postgres")
	c.Assert(err, qt.IsNil)
	hook := lockedDestructiveLintHook(builtintest.Runtime(), regeneratedTotalFS, policy, "", nil)

	err = hook(context.Background(), migrator.MigrationPlan{Versions: []int64{2}})

	c.Assert(err, qt.ErrorMatches, "(?s)pending migrations contain destructive statements.*0000000002_total.up.sql:1 destructive/drop-column.*")
}
