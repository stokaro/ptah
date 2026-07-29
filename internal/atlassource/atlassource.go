// Package atlassource classifies and resolves Atlas desired-state source URLs.
// One typed resolver keeps `--to` and `--from` handling identical across
// `schema apply`, `schema diff`, and `migrate diff`: local schema files,
// migration directories replayed on a dev database, directly connectable
// database URLs, and env:// references into the evaluated atlas.hcl
// environment.
//
// Classification and set rules are deterministic and documented:
//
//   - Every URL of one flag must classify as the same source kind.
//   - An env:// reference must be the flag's only value; it expands to the
//     selected env's configured sources, which are then re-classified under
//     the same rules.
//   - Database-URL and migration-directory sources accept exactly one URL per
//     flag; local schema files accept many.
//   - A local file:// or plain-path directory that contains an atlas.sum file
//     is a migration directory; any other directory keeps the pre-resolver
//     local-file behavior.
//   - Unsupported schemes (atlas://, docker://-as-state, and anything else)
//     fail during classification, before any target database is contacted.
package atlassource

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/atlasprojectpath"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/internal/schemafile"
)

// Kind classifies one desired-state source URL. The values read naturally in
// error messages ("%q is a %s").
type Kind string

const (
	// KindLocalFile is a local schema file (.hcl, .yaml, .yml, or .sql) given
	// as a plain path or file:// URL.
	KindLocalFile Kind = "local schema file"
	// KindDatabase is a directly connectable database URL whose live schema is
	// introspected as the desired state.
	KindDatabase Kind = "database URL"
	// KindMigrationDir is a local migration directory (marked by atlas.sum)
	// that is replayed on a dev database to produce the desired state.
	KindMigrationDir Kind = "migration directory"
	// KindEnv is an env://<attribute> reference into the evaluated atlas.hcl
	// environment. ClassifySet expands it, so resolved sets never carry it.
	KindEnv Kind = "env reference"
)

// Source is one classified desired-state URL.
type Source struct {
	// Raw is the URL as given (or as expanded from atlas.hcl).
	Raw string
	// Kind is the classified source kind.
	Kind Kind
	// Path is the resolved local filesystem path for local-file and
	// migration-directory sources, when it could be resolved.
	Path string
	// EnvAttr is the referenced attribute for env:// sources.
	EnvAttr string
}

// ProjectEnv carries the evaluated atlas.hcl environment used to expand env://
// desired-state references.
type ProjectEnv struct {
	// Loaded reports whether an atlas.hcl environment was evaluated.
	Loaded bool
	// Config is the evaluated project configuration.
	Config projectconfig.Config
	// BaseDir is the atlas.hcl directory; relative env paths resolve against
	// it.
	BaseDir string
}

// Set is the classified value of one desired-state flag (--from or --to).
// All sources of a set share one concrete kind; env:// references are already
// expanded.
type Set struct {
	// Flag is the CLI flag the URLs came from, used in error messages.
	Flag string
	// Kind is the shared source kind of the set.
	Kind Kind
	// Sources are the classified URLs.
	Sources []Source
}

// Classify determines the source kind of one desired-state URL. env://
// references are returned unexpanded; use ClassifySet to expand them against
// an evaluated atlas.hcl environment.
func Classify(rawURL string) (Source, error) {
	trimmed := strings.TrimSpace(rawURL)
	// Query parameters are cut before scheme detection so malformed values
	// keep the schema-file loader's error messages.
	base, _, hasQuery := strings.Cut(trimmed, "?")
	scheme, rest, hasScheme := strings.Cut(base, "://")
	if !hasScheme {
		return classifyLocal(trimmed)
	}
	switch scheme = strings.ToLower(scheme); {
	case scheme == "file":
		return classifyLocal(trimmed)
	case scheme == "env":
		if hasQuery {
			return Source{}, errors.New("env:// desired-state references do not accept query parameters")
		}
		attr := strings.TrimSpace(rest)
		if attr == "" {
			return Source{}, errors.New("env:// desired-state reference is missing the env attribute (for example env://src)")
		}
		return Source{Raw: trimmed, Kind: KindEnv, EnvAttr: attr}, nil
	case platform.NormalizeDialect(scheme) != "":
		return Source{Raw: trimmed, Kind: KindDatabase}, nil
	case scheme == "docker":
		return Source{}, errors.New("docker:// URLs provision Atlas dev databases and cannot be used as a desired-state source; pass a directly connectable database URL")
	case scheme == "atlas":
		return Source{}, errors.New("atlas:// registry URLs are not supported: Ptah has no Atlas Cloud registry; use a local schema file, a migration directory, a database URL, or an env:// reference")
	default:
		return Source{}, fmt.Errorf("unsupported desired-state URL scheme %q: supported sources are local schema files, migration directories, database URLs, and env:// references", scheme)
	}
}

// ClassifySet classifies all URLs of one flag, expands env:// references
// through env, and enforces the one-kind-per-flag and multiplicity rules.
func ClassifySet(flag string, rawURLs []string, env ProjectEnv) (Set, error) {
	set := Set{Flag: flag, Kind: KindLocalFile}
	for _, rawURL := range rawURLs {
		source, err := Classify(rawURL)
		if err != nil {
			return Set{}, fmt.Errorf("%s %q: %w", flag, rawURL, err)
		}
		if source.Kind != KindEnv {
			set.Sources = append(set.Sources, source)
			continue
		}
		if len(rawURLs) > 1 {
			return Set{}, fmt.Errorf("%s %q: an env:// desired-state reference must be the only %s value", flag, rawURL, flag)
		}
		expanded, err := expandEnv(source, env)
		if err != nil {
			return Set{}, fmt.Errorf("%s %q: %w", flag, rawURL, err)
		}
		set.Sources = append(set.Sources, expanded...)
	}
	if err := set.validate(); err != nil {
		return Set{}, err
	}
	return set, nil
}

func (s *Set) validate() error {
	for i, source := range s.Sources {
		if i == 0 {
			s.Kind = source.Kind
			continue
		}
		if source.Kind != s.Kind {
			first := s.Sources[0]
			return fmt.Errorf("%s mixes desired-state source kinds: %q is a %s, but %q is a %s; use one source kind per flag",
				s.Flag, first.Raw, first.Kind, source.Raw, source.Kind)
		}
	}
	if (s.Kind == KindDatabase || s.Kind == KindMigrationDir) && len(s.Sources) > 1 {
		return fmt.Errorf("%s accepts one %s desired-state source, got %d", s.Flag, s.Kind, len(s.Sources))
	}
	return nil
}

// EnsureDevDatabase verifies that sources requiring a dev-database replay have
// one configured. The returned error is deterministic and reported before any
// target database is contacted.
func (s Set) EnsureDevDatabase(devURL string) error {
	if s.Kind != KindMigrationDir || strings.TrimSpace(devURL) != "" {
		return nil
	}
	return fmt.Errorf("%s %q is a migration directory; --dev-url is required to replay it on a dev database", s.Flag, s.Sources[0].Raw)
}

// EnsureDevIsolation rejects a database desired-state source that identifies
// the same database as devURL. Destructive dev workflows must snapshot the
// desired state before resetting the dev database, and an aliased source would
// still destroy the user's desired state after that snapshot.
func (s Set) EnsureDevIsolation(devURL string) error {
	if s.Kind != KindDatabase || len(s.Sources) == 0 || strings.TrimSpace(devURL) == "" {
		return nil
	}
	same, err := atlasurl.SameDatabase(s.Sources[0].Raw, devURL)
	if err != nil {
		return fmt.Errorf("compare %s database identity with --dev-url: %w", s.Flag, err)
	}
	if same {
		return fmt.Errorf("%s database must differ from --dev-url because the dev database is reset during planning", s.Flag)
	}
	return nil
}

// ImpliedDialect returns the dialect implied by a database-URL set. Local-file
// and migration-directory sets imply no dialect.
func (s Set) ImpliedDialect() string {
	if s.Kind != KindDatabase || len(s.Sources) == 0 {
		return ""
	}
	scheme, _, _ := strings.Cut(s.Sources[0].Raw, "://")
	return platform.NormalizeDialect(scheme)
}

// PinDialect determines the SQL dialect shared by the dev database and all
// database desired-state sources. Precedence is deterministic: --dev-url pins
// first, then each set in argument order. Every later database source must
// match the pinned dialect. The returned flag names the pinning source for
// error messages; the dialect is empty when nothing pins one.
func PinDialect(devURL string, sets ...Set) (dialect string, pinnedBy string, err error) {
	dialect, err = atlasurl.DialectFromURL(devURL)
	if err != nil {
		return "", "", err
	}
	pinnedBy = "--dev-url"
	for _, set := range sets {
		implied := set.ImpliedDialect()
		if implied == "" {
			continue
		}
		if dialect == "" {
			dialect, pinnedBy = implied, set.Flag
			continue
		}
		if implied != dialect {
			return "", "", fmt.Errorf("%s database dialect %q does not match %s dialect %q", set.Flag, implied, pinnedBy, dialect)
		}
	}
	return dialect, pinnedBy, nil
}

// classifyLocal keeps the pre-resolver local-file behavior byte-identical:
// path extraction (and its errors) delegate to the schema-file loader, and
// only an existing directory that contains an atlas.sum file becomes a
// migration directory.
func classifyLocal(rawURL string) (Source, error) {
	path, err := schemafile.LocalFilePath(rawURL)
	if err != nil {
		return Source{}, err
	}
	resolved, err := pathguard.ResolveCLIPath(path)
	if err != nil {
		// The schema-file loader reports unresolvable paths itself; classify
		// as a local file so its error message is preserved.
		return Source{Raw: rawURL, Kind: KindLocalFile, Path: path}, nil
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return Source{Raw: rawURL, Kind: KindLocalFile, Path: resolved}, nil
	}
	if _, err := os.Stat(filepath.Join(resolved, migratesum.AtlasFileName)); err == nil {
		return Source{Raw: rawURL, Kind: KindMigrationDir, Path: resolved}, nil
	}
	return Source{Raw: rawURL, Kind: KindLocalFile, Path: resolved}, nil
}

func expandEnv(source Source, env ProjectEnv) ([]Source, error) {
	if !env.Loaded {
		return nil, errors.New("env:// desired-state references require an evaluated atlas.hcl project configuration; pass --config and --env to select one")
	}
	switch source.EnvAttr {
	case "src", "schema.src":
		return expandEnvSchemaSources(env)
	case "url":
		return expandEnvDatabaseURL(source.EnvAttr, env.Config.DatabaseURL)
	case "dev":
		return expandEnvDatabaseURL(source.EnvAttr, env.Config.DevURL)
	case "migration", "migration.dir":
		return expandEnvMigrationDir(env)
	default:
		return nil, fmt.Errorf("unsupported env:// attribute %q: supported attributes are src, schema.src, url, dev, migration, and migration.dir", source.EnvAttr)
	}
}

func expandEnvSchemaSources(env ProjectEnv) ([]Source, error) {
	if len(env.Config.SchemaSources) == 0 {
		return nil, errors.New("the selected atlas.hcl env does not define schema sources (env.src or env.schema.src)")
	}
	sources := make([]Source, 0, len(env.Config.SchemaSources))
	for _, value := range env.Config.SchemaSources {
		source, err := classifyEnvValue(value, env.BaseDir)
		if err != nil {
			return nil, fmt.Errorf("atlas.hcl schema source %q: %w", value, err)
		}
		sources = append(sources, source)
	}
	return sources, nil
}

// classifyEnvValue classifies one env-provided source value. Relative local
// paths resolve against the atlas.hcl directory; non-file URLs classify under
// the standard rules.
func classifyEnvValue(value, baseDir string) (Source, error) {
	trimmed := strings.TrimSpace(value)
	base, _, _ := strings.Cut(trimmed, "?")
	if strings.Contains(base, "://") && !strings.HasPrefix(base, "file://") {
		source, err := Classify(trimmed)
		if err != nil {
			return Source{}, err
		}
		if source.Kind == KindEnv {
			return Source{}, errors.New("nested env:// references are not supported")
		}
		return source, nil
	}
	resolved, err := atlasprojectpath.SchemaFileURL(trimmed, baseDir)
	if err != nil {
		return Source{}, err
	}
	return Classify(resolved)
}

func expandEnvDatabaseURL(attr, value string) ([]Source, error) {
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("the selected atlas.hcl env does not define %s", attr)
	}
	// The configured value may embed credentials, so wrapping errors must not
	// echo it.
	source, err := Classify(value)
	if err != nil {
		return nil, fmt.Errorf("atlas.hcl env %s: %w", attr, err)
	}
	if source.Kind == KindEnv {
		return nil, errors.New("nested env:// references are not supported")
	}
	if source.Kind != KindDatabase {
		return nil, fmt.Errorf("env://%s must resolve to a database URL, got a %s", attr, source.Kind)
	}
	return []Source{source}, nil
}

func expandEnvMigrationDir(env ProjectEnv) ([]Source, error) {
	value := strings.TrimSpace(env.Config.Migration.Dir)
	if value == "" {
		return nil, errors.New("the selected atlas.hcl env does not define migration.dir")
	}
	path, err := atlasprojectpath.LocalDir(value, env.BaseDir)
	if err != nil {
		return nil, fmt.Errorf("atlas.hcl migration.dir: %w", err)
	}
	return []Source{{Raw: value, Kind: KindMigrationDir, Path: path}}, nil
}
