package atlassource_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

// resolvedPath mirrors the resolver's pathguard resolution (symlinks such as
// macOS /var -> /private/var are followed) so path assertions stay portable.
func resolvedPath(t *testing.T, path string) string {
	t.Helper()
	c := qt.New(t)
	resolved, err := pathguard.ResolveWithinRoot(path, "")
	c.Assert(err, qt.IsNil)
	return resolved
}

func writeMigrationDir(t *testing.T) string {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
		[]byte("CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func TestClassify_Kinds(t *testing.T) {
	schemaDir := t.TempDir()
	schemaFile := filepath.Join(schemaDir, "schema.sql")
	c := qt.New(t)
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	tests := []struct {
		name        string
		url         string
		want        atlassource.Kind
		wantDialect string
	}{
		{name: "file url", url: "file://" + schemaFile, want: atlassource.KindLocalFile},
		{name: "plain path", url: schemaFile, want: atlassource.KindLocalFile},
		{name: "missing file", url: "file://" + filepath.Join(schemaDir, "missing.sql"), want: atlassource.KindLocalFile},
		{name: "directory without atlas.sum", url: "file://" + schemaDir, want: atlassource.KindLocalFile},
		{name: "postgres url", url: "postgres://app_user@localhost:5432/app", want: atlassource.KindDatabase, wantDialect: "postgres"},
		{name: "mysql tcp url", url: "mysql://app_user@tcp(localhost:3306)/app", want: atlassource.KindDatabase, wantDialect: "mysql"},
		{name: "sqlite url", url: "sqlite://app.db", want: atlassource.KindDatabase, wantDialect: "sqlite"},
		{name: "platform-safe sqlite path", url: atlasurl.SQLiteURLFromPath(schemaFile), want: atlassource.KindDatabase, wantDialect: "sqlite"},
		{name: "windows sqlite drive path", url: "sqlite:C:/work/app.db", want: atlassource.KindDatabase, wantDialect: "sqlite"},
		{name: "windows sqlite3 drive path alias", url: "sqlite3:C:/work/app.db", want: atlassource.KindDatabase, wantDialect: "sqlite"},
		{name: "sqlite url with query", url: "sqlite://dev?mode=memory", want: atlassource.KindDatabase, wantDialect: "sqlite"},
		{name: "env reference", url: "env://src", want: atlassource.KindEnv},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			source, err := atlassource.Classify(tc.url)

			c.Assert(err, qt.IsNil)
			c.Assert(source.Kind, qt.Equals, tc.want)
			c.Assert(source.Raw, qt.Equals, tc.url)
			c.Assert(source.Dialect, qt.Equals, tc.wantDialect)
		})
	}
}

func TestClassify_MigrationDirectory(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)

	source, err := atlassource.Classify("file://" + dir)

	c.Assert(err, qt.IsNil)
	c.Assert(source.Kind, qt.Equals, atlassource.KindMigrationDir)
	c.Assert(source.Path, qt.Equals, resolvedPath(t, dir))
}

func TestClassify_PlainPathMigrationDirectory(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)

	source, err := atlassource.Classify(dir)

	c.Assert(err, qt.IsNil)
	c.Assert(source.Kind, qt.Equals, atlassource.KindMigrationDir)
}

func TestClassify_Errors(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "empty", url: "", want: `schema file URL is required`},
		{name: "blank", url: "   ", want: `schema file URL is required`},
		{
			name: "file query params",
			url:  "file://schema.sql?format=atlas",
			want: `schema file URL query parameters are not supported yet`,
		},
		{
			name: "env without attribute",
			url:  "env://",
			want: `env:// desired-state reference is missing the env attribute \(for example env://src\)`,
		},
		{
			name: "env query params",
			url:  "env://src?x=1",
			want: `env:// desired-state references do not accept query parameters`,
		},
		{
			name: "docker",
			url:  "docker://postgres/16/dev",
			want: `docker:// URLs provision Atlas dev databases and cannot be used as a desired-state source; pass a directly connectable database URL`,
		},
		{
			name: "reserved external-schema marker scheme",
			url:  "ptah-external-schema://app",
			want: `ptah-external-schema:// is a reserved internal marker scheme; reference data\.external_schema\.<name>\.url from an atlas\.hcl env src instead`,
		},
		{
			name: "hosted registry URL",
			url:  "atlas://remote/app",
			want: `atlas:// registry URLs are not supported; use oci:// with a native Ptah command, or use a local schema file, a migration directory, a database URL, or an env:// reference`,
		},
		{
			name: "ent",
			url:  "ent://schema",
			want: `unsupported desired-state URL scheme "ent": supported sources are local schema files, migration directories, database URLs, and env:// references`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlassource.Classify(tc.url)

			c.Assert(err, qt.ErrorMatches, tc.want)
		})
	}
}

func TestClassifySet_MixedKindsConflict(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaFile := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaFile, []byte(""), 0o600), qt.IsNil)

	_, err := atlassource.ClassifySet("--to", []string{
		"file://" + schemaFile,
		"sqlite://other.db",
	}, atlassource.ProjectEnv{})

	c.Assert(err, qt.ErrorMatches,
		`--to mixes desired-state source kinds: "file://.*" is a local schema file, but "sqlite://other\.db" is a database URL; use one source kind per flag`)
}

func TestClassifySet_MultipleDatabaseURLsConflict(t *testing.T) {
	c := qt.New(t)

	_, err := atlassource.ClassifySet("--to", []string{
		"sqlite://a.db",
		"sqlite://b.db",
	}, atlassource.ProjectEnv{})

	c.Assert(err, qt.ErrorMatches, `--to accepts one database URL desired-state source, got 2`)
}

func TestClassifySet_EnvMustBeOnlyValue(t *testing.T) {
	c := qt.New(t)

	_, err := atlassource.ClassifySet("--to", []string{
		"env://src",
		"sqlite://a.db",
	}, atlassource.ProjectEnv{})

	c.Assert(err, qt.ErrorMatches, `--to "env://src": an env:// desired-state reference must be the only --to value`)
}

func TestClassifySet_UnsupportedSchemeIsWrapped(t *testing.T) {
	c := qt.New(t)

	_, err := atlassource.ClassifySet("--from", []string{"atlas://remote/app"}, atlassource.ProjectEnv{})

	c.Assert(err, qt.ErrorMatches, `--from "atlas://remote/app": atlas:// registry URLs are not supported; use oci://.*`)
}

func TestClassifySet_EnvRequiresLoadedConfig(t *testing.T) {
	c := qt.New(t)

	_, err := atlassource.ClassifySet("--to", []string{"env://src"}, atlassource.ProjectEnv{})

	c.Assert(err, qt.ErrorMatches,
		`--to "env://src": env:// desired-state references require an evaluated atlas.hcl project configuration; pass --config and --env to select one`)
}

func TestClassifySet_EnvSrcResolvesRelativePaths(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(baseDir, "schema.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	env := atlassource.ProjectEnv{
		Loaded:  true,
		BaseDir: baseDir,
		Config:  projectconfig.Config{SchemaSources: []string{"schema.sql"}},
	}

	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)

	c.Assert(err, qt.IsNil)
	c.Assert(set.Kind, qt.Equals, atlassource.KindLocalFile)
	c.Assert(set.Sources, qt.HasLen, 1)
	c.Assert(set.Sources[0].Raw, qt.Equals,
		"file://"+filepath.ToSlash(resolvedPath(t, filepath.Join(baseDir, "schema.sql"))))
}

func TestClassifySet_EnvSchemaSrcAliasResolves(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(baseDir, "schema.hcl"), []byte(""), 0o600), qt.IsNil)
	env := atlassource.ProjectEnv{
		Loaded:  true,
		BaseDir: baseDir,
		Config:  projectconfig.Config{SchemaSources: []string{"file://schema.hcl"}},
	}

	set, err := atlassource.ClassifySet("--to", []string{"env://schema.src"}, env)

	c.Assert(err, qt.IsNil)
	c.Assert(set.Kind, qt.Equals, atlassource.KindLocalFile)
	c.Assert(set.Sources, qt.HasLen, 1)
}

func TestClassifySet_EnvSrcEmptyFails(t *testing.T) {
	c := qt.New(t)
	env := atlassource.ProjectEnv{Loaded: true, BaseDir: t.TempDir()}

	_, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)

	c.Assert(err, qt.ErrorMatches,
		`--to "env://src": the selected atlas.hcl env does not define schema sources \(env.src or env.schema.src\)`)
}

func TestClassifySet_EnvURLResolvesToDatabase(t *testing.T) {
	c := qt.New(t)
	env := atlassource.ProjectEnv{
		Loaded:  true,
		BaseDir: t.TempDir(),
		Config:  projectconfig.Config{DatabaseURL: "sqlite://app.db"},
	}

	set, err := atlassource.ClassifySet("--from", []string{"env://url"}, env)

	c.Assert(err, qt.IsNil)
	c.Assert(set.Kind, qt.Equals, atlassource.KindDatabase)
	c.Assert(set.Sources[0].Raw, qt.Equals, "sqlite://app.db")
}

func TestClassifySet_EnvDevResolvesToDatabase(t *testing.T) {
	c := qt.New(t)
	env := atlassource.ProjectEnv{
		Loaded:  true,
		BaseDir: t.TempDir(),
		Config:  projectconfig.Config{DevURL: "sqlite://dev.db"},
	}

	set, err := atlassource.ClassifySet("--from", []string{"env://dev"}, env)

	c.Assert(err, qt.IsNil)
	c.Assert(set.Kind, qt.Equals, atlassource.KindDatabase)
}

func TestClassifySet_EnvURLMustBeDatabase(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(baseDir, "schema.sql"), []byte(""), 0o600), qt.IsNil)
	env := atlassource.ProjectEnv{
		Loaded:  true,
		BaseDir: baseDir,
		Config:  projectconfig.Config{DatabaseURL: "file://" + filepath.Join(baseDir, "schema.sql")},
	}

	_, err := atlassource.ClassifySet("--to", []string{"env://url"}, env)

	c.Assert(err, qt.ErrorMatches, `--to "env://url": env://url must resolve to a database URL, got a local schema file`)
}

func TestClassifySet_EnvURLMissingFails(t *testing.T) {
	c := qt.New(t)
	env := atlassource.ProjectEnv{Loaded: true, BaseDir: t.TempDir()}

	_, err := atlassource.ClassifySet("--to", []string{"env://url"}, env)

	c.Assert(err, qt.ErrorMatches, `--to "env://url": the selected atlas.hcl env does not define url`)
}

func TestClassifySet_EnvNestedReferenceFails(t *testing.T) {
	c := qt.New(t)
	env := atlassource.ProjectEnv{
		Loaded:  true,
		BaseDir: t.TempDir(),
		Config:  projectconfig.Config{DatabaseURL: "env://url"},
	}

	_, err := atlassource.ClassifySet("--to", []string{"env://url"}, env)

	c.Assert(err, qt.ErrorMatches, `--to "env://url": nested env:// references are not supported`)
}

func TestClassifySet_EnvNestedSchemaSourceFails(t *testing.T) {
	c := qt.New(t)
	env := atlassource.ProjectEnv{
		Loaded:  true,
		BaseDir: t.TempDir(),
		Config:  projectconfig.Config{SchemaSources: []string{"env://src"}},
	}

	_, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)

	c.Assert(err, qt.ErrorMatches,
		`--to "env://src": atlas.hcl schema source "env://src": nested env:// references are not supported`)
}

func TestClassifySet_EnvMigrationDirResolvesRelativePath(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(baseDir, "migrations"), 0o755), qt.IsNil)
	env := atlassource.ProjectEnv{
		Loaded:  true,
		BaseDir: baseDir,
		Config:  projectconfig.Config{Migration: projectconfig.MigrationConfig{Dir: "file://migrations"}},
	}

	set, err := atlassource.ClassifySet("--to", []string{"env://migration.dir"}, env)

	c.Assert(err, qt.IsNil)
	c.Assert(set.Kind, qt.Equals, atlassource.KindMigrationDir)
	c.Assert(set.Sources[0].Path, qt.Equals, resolvedPath(t, filepath.Join(baseDir, "migrations")))
}

func TestClassifySet_EnvMigrationDirMissingFails(t *testing.T) {
	c := qt.New(t)
	env := atlassource.ProjectEnv{Loaded: true, BaseDir: t.TempDir()}

	_, err := atlassource.ClassifySet("--to", []string{"env://migration"}, env)

	c.Assert(err, qt.ErrorMatches, `--to "env://migration": the selected atlas.hcl env does not define migration.dir`)
}

func TestClassifySet_EnvUnsupportedAttributeFails(t *testing.T) {
	c := qt.New(t)
	env := atlassource.ProjectEnv{Loaded: true, BaseDir: t.TempDir()}

	_, err := atlassource.ClassifySet("--to", []string{"env://orm"}, env)

	c.Assert(err, qt.ErrorMatches,
		`--to "env://orm": unsupported env:// attribute "orm": supported attributes are src, schema.src, url, dev, migration, and migration.dir`)
}

func TestSetEnsureDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)
	set, err := atlassource.ClassifySet("--to", []string{"file://" + dir}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	c.Assert(set.EnsureDevDatabase(""), qt.ErrorMatches,
		`--to "file://.*" is a migration directory; --dev-url is required to replay it on a dev database`)
	c.Assert(set.EnsureDevDatabase("sqlite://dev.db"), qt.IsNil)
}

func TestSetEnsureDevDatabaseIgnoresOtherKinds(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet("--to", []string{"sqlite://a.db"}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	c.Assert(set.EnsureDevDatabase(""), qt.IsNil)
}

func TestSetEnsureDevIsolation_RejectsAliasedDatabase(t *testing.T) {
	c := qt.New(t)
	env := atlassource.ProjectEnv{
		Loaded: true,
		Config: projectconfig.Config{
			DevURL: "postgres://dev_user@localhost/app?sslmode=disable",
		},
	}
	set, err := atlassource.ClassifySet("--to", []string{"env://dev"}, env)
	c.Assert(err, qt.IsNil)

	err = set.EnsureDevIsolation("postgresql://planner@localhost:5432/app?sslmode=require")

	c.Assert(err, qt.ErrorMatches,
		`--to database must differ from --dev-url because the dev database is reset during planning`)
}

func TestSetEnsureDevIsolation_RejectsPotentialHostAlias(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet(
		"--to",
		[]string{"postgres://desired.example/app"},
		atlassource.ProjectEnv{},
	)
	c.Assert(err, qt.IsNil)

	err = set.EnsureDevIsolation("postgres://dev.example/app")

	c.Assert(err, qt.ErrorMatches,
		`--to database must differ from --dev-url because the dev database is reset during planning`)
}

func TestSetEnsureDevIsolation_AllowsIndependentDatabase(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet("--to", []string{"postgres://localhost/desired"}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	err = set.EnsureDevIsolation("postgres://localhost/dev")

	c.Assert(err, qt.IsNil)
}

func TestSetEnsureDevIsolation_IgnoresLocalFiles(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE users (id integer);"), 0o600), qt.IsNil)
	set, err := atlassource.ClassifySet("--to", []string{"file://" + path}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	err = set.EnsureDevIsolation("sqlite://dev.db")

	c.Assert(err, qt.IsNil)
}

func TestPinDialect_DevURLWins(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet("--from", []string{"sqlite://a.db"}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	dialect, pinnedBy, err := atlassource.PinDialect("sqlite://dev.db", set)

	c.Assert(err, qt.IsNil)
	c.Assert(dialect, qt.Equals, "sqlite")
	c.Assert(pinnedBy, qt.Equals, "--dev-url")
}

func TestPinDialect_DatabaseSourcePinsWithoutDevURL(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet("--from", []string{"postgres://localhost/app"}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	dialect, pinnedBy, err := atlassource.PinDialect("", set)

	c.Assert(err, qt.IsNil)
	c.Assert(dialect, qt.Equals, "postgres")
	c.Assert(pinnedBy, qt.Equals, "--from")
}

func TestPinDialect_ConflictWithDevURL(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet("--from", []string{"postgres://localhost/app"}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	_, _, err = atlassource.PinDialect("sqlite://dev.db", set)

	c.Assert(err, qt.ErrorMatches, `--from database dialect "postgres" does not match --dev-url dialect "sqlite"`)
}

func TestPinDialect_ConflictBetweenSides(t *testing.T) {
	c := qt.New(t)
	fromSet, err := atlassource.ClassifySet("--from", []string{"postgres://localhost/app"}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)
	toSet, err := atlassource.ClassifySet("--to", []string{"mysql://localhost/app"}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	_, _, err = atlassource.PinDialect("", fromSet, toSet)

	c.Assert(err, qt.ErrorMatches, `--to database dialect "mysql" does not match --from dialect "postgres"`)
}

func TestPinDialect_NothingPins(t *testing.T) {
	c := qt.New(t)

	dialect, _, err := atlassource.PinDialect("")

	c.Assert(err, qt.IsNil)
	c.Assert(dialect, qt.Equals, "")
}
