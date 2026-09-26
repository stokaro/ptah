package atlassource_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/migratesum"
	"ptah.run/internal/pathguard"
	"ptah.run/migration/migrationfile"
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
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
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
			want: `atlas:// registry URLs name a hosted namespace; set PTAH_ATLAS_REGISTRY to the OCI ` +
				`namespace they stand for, or use oci:// with a native Ptah command, a local schema ` +
				`file, a migration directory, a database URL, or an env:// reference`,
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
			// The atlas:// row answers one thing with a namespace configured
			// and another without, so the table states which run it is
			// rather than inheriting whatever the developer has exported.
			t.Setenv("PTAH_ATLAS_REGISTRY", "")

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

// TestClassifySet_AtlasSchemeNeedsANamespace pins both answers the vendor
// spelling can get, because the environment decides which one and the two are
// the whole compatibility argument.
//
// Unset is the refusal the surface has always given: the reference names a
// hosted namespace, the community binary answers it from an account nobody here
// has, and rule 1 of docs/conformance.md is that nothing may succeed on Ptah
// which that binary refuses. The message now names the way in rather than only
// the way out.
//
// Set is an operator saying which registry the namespace stands for. The
// artifact is an ordinary OCI one that `schema push` writes and
// `data "remote_schema"` already reads, so refusing the flag spelling was
// costing a capability the repository had (stokaro/ptah#1210).
func TestClassifySet_AtlasSchemeNeedsANamespace(t *testing.T) {
	tests := []struct {
		name string
		// namespace is what PTAH_ATLAS_REGISTRY holds for the run.
		namespace string
		wantErr   string
		// wantReference is the OCI reference the source resolves to, empty
		// when the run is refused.
		wantReference string
	}{
		{
			name:      "no namespace keeps the refusal",
			namespace: "",
			wantErr: `--from "atlas://remote/app": atlas:// registry URLs name a hosted namespace; ` +
				`set PTAH_ATLAS_REGISTRY to the OCI namespace they stand for.*`,
		},
		{
			name:          "a namespace resolves it against that registry",
			namespace:     "ghcr.io/acme",
			wantReference: "oci://ghcr.io/acme/remote/app:latest",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("PTAH_ATLAS_REGISTRY", test.namespace)

			set, err := atlassource.ClassifySet("--from", []string{"atlas://remote/app"}, atlassource.ProjectEnv{})

			c.Assert(errorText(err), qt.Matches, orEmpty(test.wantErr))
			c.Assert(referenceOf(set), qt.Equals, test.wantReference)
		})
	}
}

// errorText is the message or the empty string, so a row states its outcome as
// a value rather than the test branching on which outcome it expects.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// orEmpty turns an absent expectation into a pattern that matches the empty
// string, for the same reason.
func orEmpty(pattern string) string {
	if pattern == "" {
		return "^$"
	}
	return pattern
}

// referenceOf names the OCI reference a classified set carries, empty when the
// set holds no remote-schema source.
func referenceOf(set atlassource.Set) string {
	for _, source := range set.Sources {
		if source.Kind == atlassource.KindRemoteSchema {
			return source.OCIReference
		}
	}
	return ""
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

// TestSetEnsureDevIsolation_SkipsADockerDevURL pins that a dev database this
// build will provision is not asked whether it aliases the desired state.
//
// A `docker://` URL names a container that does not exist yet, so it cannot BE
// the `--to` database. Asking anyway is not merely pointless: measured on
// 2026-08-13, `ptah-compat migrate diff --to postgres://... --dev-url
// docker://postgres/16/dev` exited 1 with `compare --to database identity with
// --dev-url: unsupported database URL dialect` and never reached the
// provisioner, where the pinned community binary v1.3.0 exits 0 on the same
// argv.
//
// Teaching [atlasurl.MayAddressSameDatabase] to parse docker URLs instead would
// be worse than the bug. It fails CLOSED — see
// TestSetEnsureDevIsolation_RejectsPotentialHostAlias, where two different
// hosts naming the same database are refused — so a docker URL carrying no
// readable database identity would come back "may be the same" and refuse every
// legitimate run.
func TestSetEnsureDevIsolation_SkipsADockerDevURL(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet("--to", []string{"postgres://localhost/desired"}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	c.Assert(set.EnsureDevIsolation("docker://postgres/16/dev"), qt.IsNil)

	// The skip is scoped to the scheme, not to "anything unparseable": a dev URL
	// whose database name cannot be read is still refused, because that is the
	// fail-closed case the check exists for.
	c.Assert(set.EnsureDevIsolation("postgres://localhost/desired"), qt.ErrorMatches,
		`--to database must differ from --dev-url because the dev database is reset during planning`)
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

func TestSetValidateLocalSchemaSourcesRunsBeforeResolution(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet("--to", []string{
		"file://schema.sql",
		"file://schema.yaml",
	}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)
	seen := make([]string, 0)
	validationErrors := map[string]error{
		"schema.yaml": errors.New("YAML refused before resolution"),
	}

	err = set.ValidateLocalSchemaSources(func(source string) error {
		base := filepath.Base(source)
		seen = append(seen, base)
		return validationErrors[base]
	})

	c.Assert(err, qt.ErrorMatches, "YAML refused before resolution")
	c.Assert(seen, qt.DeepEquals, []string{"schema.sql", "schema.yaml"})
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

// TestPinDialect_MySQLFamilySpellingsAgree pins a MySQL-family dev URL beside a
// source spelled as the other member of the family. The schemes do not say
// whether a server is MySQL or MariaDB, and the pinned community binary v1.3.0
// accepts any pair of them (stokaro/ptah#3756).
func TestPinDialect_MySQLFamilySpellingsAgree(t *testing.T) {
	tests := []struct {
		name   string
		devURL string
		source string
		want   string
	}{
		{name: "mysql dev URL, mariadb source", devURL: "mysql://localhost/dev", source: "mariadb://localhost/app", want: "mysql"},
		{name: "mariadb dev URL, mysql source", devURL: "mariadb://localhost/dev", source: "mysql://localhost/app", want: "mariadb"},
		{name: "maria dev URL, mysql source", devURL: "maria://localhost/dev", source: "mysql://localhost/app", want: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := atlassource.ClassifySet("--from", []string{test.source}, atlassource.ProjectEnv{})
			c.Assert(err, qt.IsNil)

			dialect, pinnedBy, err := atlassource.PinDialect(test.devURL, set)

			c.Assert(err, qt.IsNil)
			c.Assert(dialect, qt.Equals, test.want)
			c.Assert(pinnedBy, qt.Equals, "--dev-url")
		})
	}
}

func TestPinDialect_NothingPins(t *testing.T) {
	c := qt.New(t)

	dialect, _, err := atlassource.PinDialect("")

	c.Assert(err, qt.IsNil)
	c.Assert(dialect, qt.Equals, "")
}

// A local file is not one answer to "does this need a dev database". Measured
// against the pinned community binary with no --dev-url: `schema apply --to
// file://x.hcl` applies and `--to file://x.sql` refuses (stokaro/ptah#1334),
// and `schema diff` refuses both in two different sentences.
func TestSet_DeclarativeLocalFiles(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		want  bool
	}{
		{
			name:  "an HCL document is already a schema definition",
			files: []string{"schema.hcl"},
			want:  true,
		},
		{
			// Ptah's own spelling of the same declarative document. There is
			// no community-binary answer to match, because that binary has no
			// YAML source; AGENTS.md decides it, since compatibility never
			// removes a capability.
			name:  "so is YAML, in both spellings",
			files: []string{"schema.yaml", "other.yml"},
			want:  true,
		},
		{
			name:  "SQL has to be replayed to become one",
			files: []string{"schema.sql"},
			want:  false,
		},
		{
			// The set must be declarative in FULL: the SQL half still needs
			// the dev database, so the whole set does.
			name:  "a mixture is not declarative",
			files: []string{"schema.hcl", "extra.sql"},
			want:  false,
		},
		{
			// Case is not a format. A file the filesystem spells loudly is the
			// same document.
			name:  "the extension is matched case-insensitively",
			files: []string{"SCHEMA.HCL"},
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dir := t.TempDir()
			urls := make([]string, 0, len(tt.files))
			for _, name := range tt.files {
				path := filepath.Join(dir, name)
				c.Assert(os.WriteFile(path, []byte(""), 0o600), qt.IsNil)
				urls = append(urls, "file://"+path)
			}

			set, err := atlassource.ClassifySet("--to", urls, atlassource.ProjectEnv{})
			c.Assert(err, qt.IsNil)

			c.Assert(set.DeclarativeLocalFiles(), qt.Equals, tt.want)
		})
	}
}

// A schema directory is judged by the files it holds, because its own name has
// no extension to read. Measured against the pinned community binary with no
// --dev-url, `schema apply --to file://hcldir` applies and `--to
// file://sqldir` refuses (stokaro/ptah#3676).
func TestSet_DeclarativeLocalFilesReadsADirectory(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		want  bool
	}{
		{name: "a directory of HCL files", files: []string{"a.hcl", "b.hcl"}, want: true},
		{name: "a file the loader ignores changes nothing", files: []string{"a.hcl", "README.md"}, want: true},
		{name: "a directory of SQL files", files: []string{"a.sql"}, want: false},
		{name: "a directory holding both formats", files: []string{"a.hcl", "b.sql"}, want: false},
		{name: "a directory holding neither", files: []string{"README.md"}, want: false},
		// The loader reads .sql and .hcl in a directory and nothing else, so a
		// directory of YAML holds no schema file for it.
		{name: "a directory of YAML files", files: []string{"a.yaml"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dir := t.TempDir()
			for _, name := range tt.files {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(""), 0o600), qt.IsNil)
			}

			set, err := atlassource.ClassifySet("--to", []string{"file://" + dir}, atlassource.ProjectEnv{})
			c.Assert(err, qt.IsNil)

			c.Assert(set.DeclarativeLocalFiles(), qt.Equals, tt.want)
		})
	}
}

// A directory of HCL files beside a SQL file is a set with a SQL half, and the
// whole set needs the dev database that half needs.
func TestSet_DeclarativeLocalFilesJoinsADirectoryAndAFile(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	dir := filepath.Join(root, "hcldir")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "a.hcl"), []byte(""), 0o600), qt.IsNil)
	sqlFile := filepath.Join(root, "extra.sql")
	c.Assert(os.WriteFile(sqlFile, []byte(""), 0o600), qt.IsNil)

	set, err := atlassource.ClassifySet("--to", []string{"file://" + dir, "file://" + sqlFile}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)

	c.Assert(set.DeclarativeLocalFiles(), qt.IsFalse)
}

// The controls on the kind, which the format check must never answer for: a
// database URL needs no dev database for a different reason, and an empty set
// has nothing to classify.
func TestSet_DeclarativeLocalFilesIsAboutLocalFilesOnly(t *testing.T) {
	tests := []struct {
		name string
		urls []string
	}{
		{name: "a database URL", urls: []string{"sqlite://target.db"}},
		{name: "no sources at all", urls: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := atlassource.ClassifySet("--to", tt.urls, atlassource.ProjectEnv{})
			c.Assert(err, qt.IsNil)

			c.Assert(set.DeclarativeLocalFiles(), qt.IsFalse)
		})
	}
}
