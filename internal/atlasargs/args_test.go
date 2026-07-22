package atlasargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasargs"
)

func TestMap_HappyPathMigrateDownNativeFlags(t *testing.T) {
	c := qt.New(t)

	got, err := atlasargs.Map("migrate", "down", migrateDownFlags(), []string{
		"--url", "postgres://localhost/db",
		"--dir=file://migrations",
		"--to-version", "20260721120000",
		"--dry-run",
		"--revisions-schema", "atlas_schema_revisions",
		"--lock-timeout=10s",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--db-url", "postgres://localhost/db",
		"--migrations-dir=migrations",
		"--target", "20260721120000",
		"--dry-run",
		"--migrations-schema", "atlas_schema_revisions",
		"--migration-lock-timeout=10s",
	})
}

func TestMap_HappyPathMigrateLintLatestMapsToNativeFlag(t *testing.T) {
	c := qt.New(t)

	got, err := atlasargs.Map("migrate", "lint", migrateLintFlags(), []string{
		"--dir=file://migrations",
		"--latest", "2",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--dir=migrations",
		"--latest",
		"2",
	})
}

func TestMap_HappyPathSchemaCleanAutoApproveMapsToNativeFlag(t *testing.T) {
	c := qt.New(t)

	got, err := atlasargs.Map("schema", "clean", schemaCleanFlags(), []string{
		"--url", "sqlite://test.db",
		"--auto-approve",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--db-url", "sqlite://test.db",
		"--auto-approve",
	})
}

func TestMap_HappyPathSchemaCleanAutoApproveEnvIsIgnored(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_AUTO_APPROVE", "true")

	got, err := atlasargs.Map("schema", "clean", schemaCleanFlags(), []string{
		"--url", "sqlite://test.db",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--db-url", "sqlite://test.db",
	})
}

func TestMap_HappyPathEnvFlagsMapToNativeFlags(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_URL", "postgres://env/db")
	t.Setenv("PTAH_DIR", "file://env-migrations")

	got, err := atlasargs.Map("migrate", "down", migrateDownFlags(), nil)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--db-url=postgres://env/db",
		"--migrations-dir=env-migrations",
	})
}

func TestMap_HappyPathCLIFlagWinsOverEnvFlag(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_URL", "postgres://env/db")

	got, err := atlasargs.Map("migrate", "down", migrateDownFlags(), []string{
		"--url", "postgres://cli/db",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"--db-url", "postgres://cli/db"})
}

func TestMap_HappyPathFalseBoolEnvDoesNotEnableFlag(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_PLAN", "false")

	got, err := atlasargs.Map("migrate", "down", migrateDownFlags(), nil)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 0)
}

func TestMap_FailurePathRejectsRemoteDir(t *testing.T) {
	c := qt.New(t)

	_, err := atlasargs.Map("migrate", "down", migrateDownFlags(), []string{
		"--dir", "atlas://repo/migrations",
	})

	c.Assert(err, qt.ErrorMatches, `atlas migrate down --dir: only local file:// migration directories are supported`)
}

func TestMap_FailurePathUnsupportedFlagsFailExplicitly(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "dev_url",
			args: []string{"--dev-url", "sqlite://dev"},
			want: "atlas migrate down accepts --dev-url, but Ptah does not implement its behavior yet",
		},
		{
			name: "skip_checks",
			args: []string{"--skip-checks"},
			want: "atlas migrate down accepts --skip-checks, but Ptah does not implement its behavior yet",
		},
		{
			name: "to_tag",
			args: []string{"--to-tag", "release-v1"},
			want: "atlas migrate down accepts --to-tag, but Ptah does not implement its behavior yet",
		},
		{
			name: "format",
			args: []string{"--format", "{{ json . }}"},
			want: "atlas migrate down accepts --format, but Ptah does not implement its behavior yet",
		},
		{
			name: "plan",
			args: []string{"--plan"},
			want: "atlas migrate down accepts --plan, but Ptah does not implement its behavior yet",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			_, err := atlasargs.Map("migrate", "down", migrateDownFlags(), tt.args)
			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func schemaCleanFlags() []atlasargs.Flag {
	return []atlasargs.Flag{
		atlasargs.NativeString("url", "u", "Database URL to clean", "db-url"),
		atlasargs.NativeBool("dry-run", "", "Show planned cleanup without applying it", "dry-run"),
		atlasargs.ExplicitNativeBool("auto-approve", "", "Skip interactive approval", "auto-approve"),
	}
}

func migrateDownFlags() []atlasargs.Flag {
	return []atlasargs.Flag{
		atlasargs.NativeString("url", "u", "Database URL", "db-url"),
		atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
		atlasargs.UnsupportedString("dev-url", "", "Dev database URL used by Atlas for dynamic down planning"),
		atlasargs.NativeString("to-version", "", "Target version to roll back to", "target"),
		atlasargs.UnsupportedString("to-tag", "", "Target migration tag to roll back to"),
		atlasargs.NativeBool("dry-run", "", "Show rollback plan without applying it", "dry-run"),
		atlasargs.UnsupportedString("format", "", "Atlas Go template output format"),
		atlasargs.NativeString("revisions-schema", "", "Schema for the revision table", "migrations-schema"),
		atlasargs.NativeString("lock-timeout", "", "Timeout for acquiring migration locks", "migration-lock-timeout"),
		atlasargs.UnsupportedBool("skip-checks", "", "Skip Atlas down migration safety checks"),
		atlasargs.UnsupportedBool("plan", "", "Force Atlas dynamic down planning"),
	}
}

func migrateLintFlags() []atlasargs.Flag {
	return []atlasargs.Flag{
		atlasargs.UnsupportedString("dev-url", "", "Dev database URL"),
		atlasargs.NativeLocalDir("dir", "", "Migration directory", "dir"),
		atlasargs.NativeUint("latest", "", "Number of latest migrations to lint", "latest"),
	}
}
