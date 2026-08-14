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
//   - Database-URL, migration-directory, and external-schema sources accept
//     exactly one URL per flag; local schema files accept many.
//   - An env whose desired state is a declared atlas.hcl data.external_schema
//     source expands to an external-schema program source, gated on the
//     PTAH_ALLOW_EXTERNAL_SCHEMA environment variable.
//   - A local file:// or plain-path directory that contains an atlas.sum file
//     is a migration directory; any other directory keeps the pre-resolver
//     local-file behavior.
//   - Unsupported schemes (atlas://, docker://-as-state, and anything else)
//     fail during classification, before any target database is contacted.
package atlassource

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemasource"
	"go.5x5.cz/ptah/internal/atlasprojectpath"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemafile"
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
	// KindExternalSchema is an atlas.hcl data.external_schema program whose
	// standard output is the desired state. It only ever appears through env
	// expansion of a selected env whose desired-state source is a declared
	// external schema data source.
	KindExternalSchema Kind = "external schema program"
)

// AllowExternalSchemaEnvVar gates executing an atlas.hcl
// data.external_schema program resolved through env expansion. The
// Atlas-identical `ptah-compat` flag surface cannot grow a new flag, so the
// opt-in is this environment variable — the same variable that drives the
// native --allow-external-schema flag through Ptah's env twin machinery.
const AllowExternalSchemaEnvVar = "PTAH_ALLOW_EXTERNAL_SCHEMA"

// Source is one classified desired-state URL.
type Source struct {
	// Raw is the URL as given (or as expanded from atlas.hcl).
	Raw string
	// Kind is the classified source kind.
	Kind Kind
	// Dialect is the normalized database dialect for database URL sources and
	// empty for every other source kind.
	Dialect string
	// Path is the resolved local filesystem path for local-file and
	// migration-directory sources, when it could be resolved.
	Path string
	// EnvAttr is the referenced attribute for env:// sources.
	EnvAttr string
	// Command is the resolved external schema program for external-schema
	// sources; zero for every other kind. Its dialect hint is filled during
	// resolution.
	Command schemasource.Command
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
	// migrationSnapshot is the checksum-verified directory image captured by
	// PrepareMigrationSource. Keeping it on the immutable classified value lets
	// a command validate before unrelated database or lock work and then replay
	// those exact bytes instead of reopening the pathname.
	migrationSnapshot fs.FS
}

// PrepareMigrationSource captures and validates a migration-directory source
// before a caller starts unrelated database or lock work. The returned Set
// retains the stable snapshot for Resolve. Nil validators and non-migration
// sources are no-ops, preserving the full compatibility policy's behavior.
func (s Set) PrepareMigrationSource(validate func(fs.FS) error) (Set, error) {
	if s.Kind != KindMigrationDir || validate == nil {
		return s, nil
	}
	snapshot, err := CaptureVerifiedMigrationDir(s.Sources[0].Path)
	if err != nil {
		return Set{}, err
	}
	if err := validate(snapshot); err != nil {
		return Set{}, err
	}
	s.migrationSnapshot = snapshot
	return s, nil
}

// ValidateLocalSchemaSources applies validate to every local schema source in
// the already-classified set. It is deliberately separate from Resolve so a
// caller can enforce a source policy before opening an unrelated database or
// acquiring a lock. Nil and non-local sets are no-ops.
func (s Set) ValidateLocalSchemaSources(validate func(string) error) error {
	if s.Kind != KindLocalFile || validate == nil {
		return nil
	}
	for _, source := range s.Sources {
		if err := validate(source.Path); err != nil {
			return err
		}
	}
	return nil
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
		// SQLite drive paths use the opaque form on Windows so the drive's
		// colon cannot be parsed as a URL host port.
		opaqueScheme, _, hasOpaqueScheme := strings.Cut(base, ":")
		if hasOpaqueScheme && platform.NormalizeDialect(opaqueScheme) == platform.SQLite {
			return Source{Raw: trimmed, Kind: KindDatabase, Dialect: platform.SQLite}, nil
		}
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
		return Source{Raw: trimmed, Kind: KindDatabase, Dialect: platform.NormalizeDialect(scheme)}, nil
	case scheme == "docker":
		return Source{}, errors.New("docker:// URLs provision Atlas dev databases and cannot be used as a desired-state source; pass a directly connectable database URL")
	case scheme == "atlas":
		return Source{}, errors.New("atlas:// registry URLs are not supported; use oci:// with a native Ptah command, or use a local schema file, a migration directory, a database URL, or an env:// reference")
	case scheme == "ptah-external-schema":
		// The marker is minted internally when an atlas.hcl env src selects a
		// data "external_schema" source; spelled directly it must never reach
		// classification, so reject it here rather than relying on the
		// generic unknown-scheme default below staying strict.
		return Source{}, errors.New("ptah-external-schema:// is a reserved internal marker scheme; reference data.external_schema.<name>.url from an atlas.hcl env src instead")
	default:
		return Source{}, &UnsupportedSchemeError{Scheme: scheme}
	}
}

// UnsupportedSchemeError reports a desired-state URL whose scheme names no
// source kind this build resolves. It is the generic default of [Classify]:
// docker://, atlas:// and the reserved internal marker are refused by named
// branches above it and never produce this error.
//
// The type exists so a caller can recognize that verdict without matching the
// message, and it carries the scheme so a caller that words the refusal
// differently does not have to re-parse the URL. The Atlas-compatible surface
// is the caller that needs both: the pinned community binary answers an
// unknown scheme on `schema inspect --url` from its client layer rather than
// from a desired-state resolver, so cmd/atlas re-words exactly this verdict
// and leaves every other classification failure alone. Its Error text is the
// message this branch has always produced, so native Ptah is unchanged.
type UnsupportedSchemeError struct {
	// Scheme is the lowercased scheme that named no source kind.
	Scheme string
}

func (e *UnsupportedSchemeError) Error() string {
	return fmt.Sprintf(
		"unsupported desired-state URL scheme %q: supported sources are local schema files,"+
			" migration directories, database URLs, and env:// references",
		e.Scheme,
	)
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
	if s.Kind != KindLocalFile && len(s.Sources) > 1 {
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
	same, err := atlasurl.MayAddressSameDatabase(s.Sources[0].Raw, devURL)
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
	return s.Sources[0].Dialect
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

// EnvAttrs are the env:// attributes an env reference may name, in the order
// they are listed to the user. It is the single source of truth for both the
// expansion switch below and the native binary's refusal, so a surface that
// grows an attribute cannot leave the other advertising the old list.
var EnvAttrs = []string{"src", "schema.src", "url", "dev", "migration", "migration.dir"}

// ValidateEnvAttr reports whether attr names a supported env:// attribute,
// returning the shared diagnostic when it does not.
func ValidateEnvAttr(attr string) error {
	if slices.Contains(EnvAttrs, attr) {
		return nil
	}
	return fmt.Errorf(
		"unsupported env:// attribute %q: supported attributes are %s",
		attr,
		joinEnvAttrs(),
	)
}

// joinEnvAttrs renders the attribute list as prose: "a, b, and c".
func joinEnvAttrs() string {
	if len(EnvAttrs) < 2 {
		return strings.Join(EnvAttrs, "")
	}
	return strings.Join(EnvAttrs[:len(EnvAttrs)-1], ", ") + ", and " + EnvAttrs[len(EnvAttrs)-1]
}

func expandEnv(source Source, env ProjectEnv) ([]Source, error) {
	if !env.Loaded {
		return nil, errors.New("env:// desired-state references require an evaluated atlas.hcl project configuration; pass --config and --env to select one")
	}
	if err := ValidateEnvAttr(source.EnvAttr); err != nil {
		return nil, err
	}
	// Env expansion is the subsystem that recognizes the external-schema opt-in,
	// so the value is resolved on every expansion -- including the ones that
	// reference `url` or `migration`, and the ones whose selected env declares no
	// external schema program at all. Resolving it only inside
	// [expandEnvExternalSchema] would let PTAH_ALLOW_EXTERNAL_SCHEMA=tru pass in
	// silence on every configuration that does not use the feature, which is
	// exactly the pipeline that exports it and never sees it work.
	// See stokaro/ptah#1334.
	allowExternal, err := externalSchemaAllowed()
	if err != nil {
		return nil, err
	}
	switch source.EnvAttr {
	case "src", "schema.src":
		if !allowExternal && envSelectsExternalSchema(env) {
			return nil, errExternalSchemaDisabled()
		}
		return expandEnvSchemaSources(source, env)
	case "url":
		return expandEnvDatabaseURL(source.EnvAttr, env.Config.DatabaseURL)
	case "dev":
		return expandEnvDatabaseURL(source.EnvAttr, env.Config.DevURL)
	case "migration", "migration.dir":
		return expandEnvMigrationDir(env)
	default:
		// Unreachable while EnvAttrs and this switch agree; ValidateEnvAttr has
		// already rejected everything else. Kept fail-closed so an attribute
		// added to EnvAttrs alone cannot silently expand to nothing.
		return nil, ValidateEnvAttr(source.EnvAttr)
	}
}

func expandEnvSchemaSources(source Source, env ProjectEnv) ([]Source, error) {
	if len(env.Config.SchemaSources) == 0 {
		if envSelectsExternalSchema(env) {
			return expandEnvExternalSchema(source, env)
		}
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

// expandEnvExternalSchema turns the selected env's external schema program
// into a desired-state source. The program is repository-controlled code
// reached through an auto-discovered config file, so expansion is gated on an
// explicit environment opt-in; the gate fails during classification, before
// any database is contacted and before the program could run.
func expandEnvExternalSchema(source Source, env ProjectEnv) ([]Source, error) {
	external := env.Config.ExternalSchema
	return []Source{{
		Raw:  source.Raw,
		Kind: KindExternalSchema,
		Command: schemasource.Command{
			Args:   slices.Clone(external.Program),
			Format: external.Format,
			Dir:    external.WorkingDir,
			Env:    slices.Clone(external.Env),
		},
	}}, nil
}

// envSelectsExternalSchema reports whether the selected env's desired state is
// the declared external schema program rather than ordinary schema sources.
//
// The gate above and the expansion below both ask this, and they have to agree:
// a gate testing a different condition would either refuse an env that runs no
// program or let one run unguarded.
func envSelectsExternalSchema(env ProjectEnv) bool {
	return len(env.Config.SchemaSources) == 0 && len(env.Config.ExternalSchema.Program) > 0
}

// errExternalSchemaDisabled is the refusal an env expansion returns when the
// selected env's desired state is a repository-controlled program and the
// opt-in was not given. It names the way back, because a refusal that does not
// is a dead end.
func errExternalSchemaDisabled() error {
	return fmt.Errorf(
		"atlas.hcl data.external_schema executes a repository-controlled program and is disabled by default; set %s=1 to allow it",
		AllowExternalSchemaEnvVar,
	)
}

// allowExternalSchema is the declaration of the variable, made once, in the
// package that owns it. See [go.5x5.cz/ptah/internal/envbool].
//
// The same name reaches the native surface as the --allow-external-schema
// flag's environment twin, which cmd/internal/cmdflags already parses under the
// same grammar and the same error, so one name means one thing on both
// binaries.
// It is [go.5x5.cz/ptah/internal/envbool.Gated]: evaluating
// `data "external_schema"` runs a repository-controlled program, which the
// pinned community binary reaches only behind its own opt-in flag that the
// strict command tree does not register.
var allowExternalSchema = envbool.New(AllowExternalSchemaEnvVar, false, envbool.Gated)

// externalSchemaAllowed reports whether executing an atlas.hcl
// data.external_schema program is allowed.
//
// Unset denies and a valid false spelling denies too; an empty or unparsable
// value is a configuration error. Denying on a typo is the safe direction, but
// silence is not: an operator who exported the opt-in and is refused anyway has
// to be told why (stokaro/ptah#1334).
func externalSchemaAllowed() (bool, error) {
	return allowExternalSchema.Resolve()
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
