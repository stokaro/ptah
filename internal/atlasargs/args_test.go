package atlasargs_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasargs"
)

func TestParseLocalDir_HappyPath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name      string
		input     string
		wantPath  string
		wantQuery url.Values
	}{
		{
			name:      "plain path",
			input:     "migrations",
			wantPath:  "migrations",
			wantQuery: url.Values{},
		},
		{
			name:      "plain path preserves URL escapes",
			input:     "migrations%2Farchive",
			wantPath:  "migrations%2Farchive",
			wantQuery: url.Values{},
		},
		{
			name:      "plain path preserves trailing question mark",
			input:     "migrations?",
			wantPath:  "migrations?",
			wantQuery: url.Values{},
		},
		{
			name:      "plain path preserves query-like suffix",
			input:     "migrations?format=atlas",
			wantPath:  "migrations?format=atlas",
			wantQuery: url.Values{},
		},
		{
			name:      "file URL",
			input:     "file://migrations",
			wantPath:  "migrations",
			wantQuery: url.Values{},
		},
		{
			name:      "empty file URL",
			input:     "file://",
			wantPath:  "",
			wantQuery: url.Values{},
		},
		{
			name:      "encoded path and format",
			input:     "file://migration%20files?format=atlas",
			wantPath:  "migration files",
			wantQuery: url.Values{"format": []string{"atlas"}},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := atlasargs.ParseLocalDir(tt.input)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Path, qt.Equals, tt.wantPath)
			c.Assert(got.Query, qt.DeepEquals, tt.wantQuery)
		})
	}
}

func TestParseLocalDir_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("remote URL", func(c *qt.C) {
		got, err := atlasargs.ParseLocalDir("atlas://repo/migrations")

		c.Assert(err, qt.ErrorMatches, "only local file:// migration directories are supported")
		c.Assert(got, qt.DeepEquals, atlasargs.LocalDir{})
	})

	c.Run("malformed query", func(c *qt.C) {
		got, err := atlasargs.ParseLocalDir("file://migrations?format=%zz")

		c.Assert(err, qt.ErrorMatches, `parse migration directory URL query:.*invalid URL escape.*`)
		c.Assert(got, qt.DeepEquals, atlasargs.LocalDir{})
	})
}

func TestLocalDirValue_FailurePathRejectsQuery(t *testing.T) {
	c := qt.New(t)

	got, err := atlasargs.LocalDirValue("file://migrations?format=atlas")

	c.Assert(err, qt.ErrorMatches, "migration directory URL query parameters are not supported for this command")
	c.Assert(got, qt.Equals, "")
}

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

func TestMap_HappyPathStringDefaultsMapToNativeFlags(t *testing.T) {
	c := qt.New(t)

	got, err := atlasargs.Map("migrate", "hash", migrateHashFlags(), []string{
		"--dir=file://migrations",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--dir=migrations",
		"--dir-format=atlas",
	})
}

func TestMap_HappyPathPlainQuestionMarkDirIsPreserved(t *testing.T) {
	c := qt.New(t)

	got, err := atlasargs.Map("migrate", "hash", migrateHashFlags(), []string{
		"--dir=migrations?format=atlas",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--dir=migrations?format=atlas",
		"--dir-format=atlas",
	})
}

func TestMap_HappyPathCLIFlagWinsOverStringDefault(t *testing.T) {
	c := qt.New(t)

	got, err := atlasargs.Map("migrate", "hash", migrateHashFlags(), []string{
		"--dir=file://migrations",
		"--dir-format", "ptah",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--dir=migrations",
		"--dir-format",
		"ptah",
	})
}

func TestMap_HappyPathEnvFlagWinsOverStringDefault(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DIR_FORMAT", "ptah")

	got, err := atlasargs.Map("migrate", "hash", migrateHashFlags(), nil)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"--dir-format=ptah"})
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

func TestMap_FailurePathUnsupportedReasonFlagsCarryRationale(t *testing.T) {
	c := qt.New(t)
	flags := []atlasargs.Flag{
		atlasargs.UnsupportedStringReason("to-tag", "", "Target migration tag",
			"migration tags exist only in Atlas Registry"),
		atlasargs.UnsupportedBoolReason("skip-checks", "", "Skip safety checks",
			"Ptah has no generated checks to skip"),
	}

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "string_reason",
			args: []string{"--to-tag", "release-v1"},
			want: "atlas migrate down accepts --to-tag, but Ptah does not implement its behavior: migration tags exist only in Atlas Registry",
		},
		{
			name: "bool_reason",
			args: []string{"--skip-checks"},
			want: "atlas migrate down accepts --skip-checks, but Ptah does not implement its behavior: Ptah has no generated checks to skip",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			_, err := atlasargs.Map("migrate", "down", flags, tt.args)
			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestUnsupportedFlagError_DefaultsDisplayNameToLongFlag(t *testing.T) {
	c := qt.New(t)
	flag := atlasargs.UnsupportedBoolReason("plan", "", "Force dynamic planning", "bound to the cloud approval flow")

	err := atlasargs.UnsupportedFlagError("migrate", "down", flag, "")

	c.Assert(err, qt.ErrorMatches,
		"atlas migrate down accepts --plan, but Ptah does not implement its behavior: bound to the cloud approval flow")
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

func migrateHashFlags() []atlasargs.Flag {
	return []atlasargs.Flag{
		atlasargs.NativeLocalDir("dir", "", "Migration directory", "dir"),
		atlasargs.NativeStringDefault("dir-format", "", "Migration directory format", "dir-format", "atlas"),
	}
}
