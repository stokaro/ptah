// Package atlascompatpolicy owns the process-wide policy that selects the
// complete ptah-compat surface or the Atlas Community Edition compatibility
// subset.
package atlascompatpolicy

import (
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/migration/migrator"
)

// StrictCompatEnvVar enables the Atlas CE-only compatibility policy for the
// ptah-compat process. It intentionally has no CLI flag: strict mode must not
// change the Atlas command surface it is measuring.
const StrictCompatEnvVar = "PTAH_ATLAS_STRICT_COMPAT"

// Policy is the immutable compatibility choice resolved by the ptah-compat
// process before it constructs the command tree.
type Policy struct {
	strictCE bool
}

var strictCompat = envbool.New(StrictCompatEnvVar, false)

// Full returns the default policy. It retains every Atlas Pro-like and
// best-effort capability Ptah exposes on the compatibility surface.
func Full() Policy {
	return Policy{}
}

// StrictCE returns the opt-in Atlas Community Edition compatibility policy.
func StrictCE() Policy {
	return Policy{strictCE: true}
}

// IsStrictCE reports whether the policy limits compatibility-only extensions
// to the pinned Atlas CE surface.
func (p Policy) IsStrictCE() bool {
	return p.strictCE
}

// IgnoreUnknownHCLNames reports whether Atlas-authored HCL names outside
// Ptah's schema model may be ignored. Full compatibility keeps the existing
// best-effort reader; strict CE mode refuses unknown authored content instead
// of silently dropping it from the parity oracle.
func (p Policy) IgnoreUnknownHCLNames() bool {
	return !p.strictCE
}

// ValidateDesiredSchema refuses desired-schema object kinds that Atlas CE
// cannot represent when strict mode is selected. Full compatibility remains a
// no-op so every Pro-like and best-effort capability stays reachable by
// default on the same compatibility surface.
func (p Policy) ValidateDesiredSchema(database *goschema.Database) error {
	return p.validateSchemaObjects(database, "desired")
}

// ValidateInspectedSchema refuses object kinds the pinned Community Edition
// inspector does not describe. It closes live-database and migration-replay
// paths that do not pass through desired-schema loading, while full mode keeps
// Ptah's richer inspection output unchanged.
func (p Policy) ValidateInspectedSchema(database *goschema.Database) error {
	return p.validateSchemaObjects(database, "inspected")
}

func (p Policy) validateSchemaObjects(database *goschema.Database, source string) error {
	if !p.strictCE || database == nil {
		return nil
	}
	for _, object := range strictCEUnsupportedDesiredObjects(database) {
		if object.present {
			return fmt.Errorf(
				"Atlas Community Edition strict compatibility does not support %s schema %s",
				source,
				object.name,
			)
		}
	}
	return nil
}

// ValidateSchemaCleanObject refuses live object kinds that Atlas Community
// Edition does not model for schema cleanup. The refusal happens after catalog
// inspection but before confirmation or SQL execution: strict mode must not
// turn an OSS-parity run into a destructive Pro-like cleanup. Full mode keeps
// Ptah's complete cleanup coverage unchanged.
func (p Policy) ValidateSchemaCleanObject(kind, name string) error {
	if !p.strictCE {
		return nil
	}
	switch kind {
	case "table", "foreign_key", "enum":
		return nil
	default:
		return fmt.Errorf(
			"Atlas Community Edition strict compatibility does not support cleaning live schema %s %q",
			kind,
			name,
		)
	}
}

// ValidateSchemaCleanSnapshot refuses Pro-only objects that a cleanup plan
// intentionally does not list as separate changes. Named cleanup objects are
// checked by [Policy.ValidateSchemaCleanObject]; this check closes collateral
// deletion and omission paths for dependent catalog state such as triggers and
// row-level security policies.
func (p Policy) ValidateSchemaCleanSnapshot(database *goschema.Database) error {
	if !p.strictCE || database == nil {
		return nil
	}
	for _, object := range strictCEUnsupportedCleanupSnapshotObjects(database) {
		if object.present {
			return fmt.Errorf(
				"Atlas Community Edition strict compatibility does not support cleaning live schema %s",
				object.name,
			)
		}
	}
	return nil
}

// ValidateSchemaInspectFormat refuses Pro-only schema-inspect template
// helpers in strict CE mode. The default policy remains a no-op so the same
// ptah-compat binary retains the complete helper set unless strict mode is
// explicitly selected.
func (p Policy) ValidateSchemaInspectFormat(format string) error {
	if !p.strictCE {
		return nil
	}
	functions, err := atlasreport.SchemaInspectTemplateFunctions(format)
	if err != nil {
		return err
	}
	for _, name := range []string{"hcl", "split", "write"} {
		if slices.Contains(functions, name) {
			return fmt.Errorf("Atlas Community Edition strict compatibility does not support schema inspect template function %q", name)
		}
	}
	return nil
}

// ValidateProjectConfig rejects atlas.hcl constructs that the compatibility
// parser accepted without applying. Full mode reports those no-ops as before;
// strict mode cannot use a silently ignored construct as CE-parity evidence.
func (p Policy) ValidateProjectConfig(config projectconfig.Config) error {
	if !p.strictCE {
		return nil
	}
	if len(config.IgnoredConstructs) > 0 {
		ignored := config.IgnoredConstructs[0]
		return fmt.Errorf(
			"Atlas Community Edition strict compatibility refuses ignored atlas.hcl %s %q at %s:%d",
			ignored.Kind,
			ignored.Name,
			ignored.Filename,
			ignored.Line,
		)
	}
	for _, rawURL := range append(
		[]string{config.DatabaseURL, config.DevURL},
		config.SchemaSources...,
	) {
		if err := p.ValidateURL(rawURL); err != nil {
			return err
		}
		if err := p.ValidateLocalSchemaSource(rawURL); err != nil {
			return err
		}
	}
	return nil
}

// ValidateSchemaApplyConfig refuses an authored lint policy that strict mode
// cannot enforce on schema apply. The pinned CE binary parses the policy but
// skips the apply-time lint pass; copying that behavior would silently discard
// a safety contract the operator wrote. Migrate lint still accepts and enforces
// the same analyzer configuration under strict mode.
func (p Policy) ValidateSchemaApplyConfig(config projectconfig.Config) error {
	if !p.strictCE ||
		(len(config.Lint.RuleConfigs) == 0 && len(config.Lint.DisabledRules) == 0) {
		return nil
	}
	return fmt.Errorf(
		"Atlas Community Edition strict compatibility cannot enforce atlas.hcl lint policy during schema apply",
	)
}

// ValidateLocalSchemaSource refuses Ptah-owned local schema formats that the
// pinned Community Edition does not recognize. SQL and HCL remain valid; full
// compatibility keeps YAML reachable on the same ptah-compat surface.
func (p Policy) ValidateLocalSchemaSource(source string) error {
	if !p.strictCE {
		return nil
	}
	cleaned := strings.TrimSpace(source)
	if localPath, ok := strings.CutPrefix(cleaned, "file://"); ok {
		cleaned = localPath
		if end := strings.IndexAny(cleaned, "?#"); end >= 0 {
			cleaned = cleaned[:end]
		}
	} else if parsed, err := url.Parse(cleaned); err == nil && parsed.Scheme != "" {
		if parsed.Scheme != "file" {
			return nil
		}
		cleaned = parsed.Path
	}
	switch strings.ToLower(filepath.Ext(cleaned)) {
	case ".yaml", ".yml":
		return fmt.Errorf(
			"Atlas Community Edition strict compatibility does not support YAML schema source %q",
			filepath.Base(cleaned),
		)
	default:
		return nil
	}
}

// ValidateURL refuses database dialects absent from the pinned CE binary while
// leaving non-database source URLs and malformed values to their owning
// command's existing validation. Full mode never narrows Ptah's dialect set.
func (p Policy) ValidateURL(rawURL string) error {
	if !p.strictCE {
		return nil
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil
	}
	dialect := platform.NormalizeDialect(parsed.Scheme)
	if parsed.Scheme == "docker" {
		dialect, err = atlasurl.DialectFromURL(rawURL)
		if err != nil {
			return nil
		}
	}
	return p.ValidateDialect(dialect)
}

// ValidateDialect applies the pinned CE database-family inventory. Empty and
// unknown dialects remain the owning command's validation responsibility.
func (p Policy) ValidateDialect(dialect string) error {
	if !p.strictCE {
		return nil
	}
	normalized := platform.NormalizeDialect(dialect)
	switch normalized {
	case "", platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite:
		return nil
	default:
		return fmt.Errorf(
			"Atlas Community Edition strict compatibility does not support database dialect %q",
			normalized,
		)
	}
}

// ValidateMigrationSource refuses authored migration formats and directives
// whose execution semantics are outside Atlas CE. It validates the captured
// source before conversion or database work: strict mode must neither execute
// Ptah's richer meaning nor copy CE's behavior of treating the authored
// metadata as ordinary SQL or an inert comment.
func (p Policy) ValidateMigrationSource(fsys fs.FS) error {
	if !p.strictCE {
		return nil
	}
	return fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || strings.ToLower(path.Ext(name)) != ".sql" {
			return nil
		}
		contents, err := fs.ReadFile(fsys, name)
		if err != nil {
			return fmt.Errorf("read migration %s for Atlas CE strict compatibility: %w", name, err)
		}
		source := string(contents)
		if firstNonemptyLine(source) == "-- atlas:txtar" {
			return fmt.Errorf("Atlas Community Edition strict compatibility does not support Atlas txtar migration %s", name)
		}
		checks, err := migrator.ParseChecks(source, "")
		if err != nil {
			return fmt.Errorf("inspect migration %s for Atlas CE strict compatibility: %w", name, err)
		}
		if len(checks) > 0 {
			return fmt.Errorf("Atlas Community Edition strict compatibility does not support Ptah pre-migration checks in %s", name)
		}
		if len(migrator.ParseFileDirectives(source)) > 0 {
			return fmt.Errorf("Atlas Community Edition strict compatibility does not support Ptah migration directives in %s", name)
		}
		if migrator.LooksAtlasTemplateSQL(source) {
			return fmt.Errorf("Atlas Community Edition strict compatibility does not support SQL template migration %s", name)
		}
		return nil
	})
}

func firstNonemptyLine(source string) string {
	for line := range strings.SplitSeq(source, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return ""
}

type strictCEDesiredObject struct {
	name    string
	present bool
}

func strictCEUnsupportedDesiredObjects(database *goschema.Database) []strictCEDesiredObject {
	return []strictCEDesiredObject{
		{name: "extensions", present: len(database.Extensions) > 0},
		{name: "functions", present: len(database.Functions) > 0},
		{name: "standalone sequences", present: len(database.Sequences) > 0},
		{name: "domains", present: len(database.Domains) > 0},
		{name: "composite types", present: len(database.CompositeTypes) > 0},
		{name: "range types", present: len(database.Ranges) > 0},
		{name: "views", present: len(database.Views) > 0},
		{name: "materialized views", present: len(database.MaterializedViews) > 0},
		{name: "triggers", present: len(database.Triggers) > 0},
		{name: "row-level security policies", present: len(database.RLSPolicies) > 0},
		{name: "row-level security settings", present: len(database.RLSEnabledTables) > 0},
		{name: "roles", present: len(database.Roles) > 0},
		{name: "grants", present: len(database.Grants) > 0},
		{name: "managed data", present: len(database.ManagedData) > 0},
		{name: "table partitioning", present: hasTablePartitioning(database)},
		{name: "platform overrides", present: hasPlatformOverrides(database)},
	}
}

func strictCEUnsupportedCleanupSnapshotObjects(database *goschema.Database) []strictCEDesiredObject {
	return []strictCEDesiredObject{
		{name: "extensions", present: len(database.Extensions) > 0},
		{name: "triggers", present: len(database.Triggers) > 0},
		{name: "row-level security policies", present: len(database.RLSPolicies) > 0},
		{name: "row-level security settings", present: len(database.RLSEnabledTables) > 0},
		{name: "roles", present: len(database.Roles) > 0},
		{name: "grants", present: len(database.Grants) > 0},
		{name: "managed data", present: len(database.ManagedData) > 0},
		{name: "table partitioning", present: hasTablePartitioning(database)},
	}
}

func hasTablePartitioning(database *goschema.Database) bool {
	return slices.ContainsFunc(database.Tables, func(table goschema.Table) bool {
		return table.Partition != nil
	})
}

func hasPlatformOverrides(database *goschema.Database) bool {
	return slices.ContainsFunc(database.Tables, func(table goschema.Table) bool {
		return len(table.Overrides) > 0
	}) || slices.ContainsFunc(database.Fields, func(field goschema.Field) bool {
		return len(field.Overrides) > 0
	})
}

// Resolve reads the strict-mode selector once at the ptah-compat process
// boundary. When strict mode is selected, it also refuses extension toggles
// that would otherwise silently restore behavior outside the CE surface.
func Resolve() (Policy, error) {
	strict, err := strictCompat.Resolve()
	if err != nil {
		return Policy{}, err
	}
	if !strict {
		return Full(), nil
	}
	if err := validateStrictEnvironment(); err != nil {
		return Policy{}, err
	}
	return StrictCE(), nil
}

// gatedBooleanEnvVars are direct opt-in compatibility extensions. Flag-bound
// booleans are discovered from the full command tree by
// [ValidateStrictCompatFlagEnvironment]. False spellings do not enable an
// extension and remain valid in strict mode; true spellings are refused.
// Malformed values retain envbool's one repository-wide error shape.
var gatedBooleanEnvVars = []string{
	"PTAH_ALLOW_EXTERNAL_SCHEMA",
	"PTAH_ALLOW_RESERVED_ROLE_NAMES",
	"PTAH_ATLAS_APPLY_WITHOUT_DEV_URL",
	"PTAH_ATLAS_INSPECT_ALL_BLOCKS",
	"PTAH_ATLAS_LINT_ALL_VERSIONS",
	"PTAH_ATLAS_LINT_WITHOUT_DEV_URL",
	"PTAH_HCL_MERGE_REDECLARATIONS",
	"PTAH_HCL_SCHEMA_SCOPED_ENUMS",
	"PTAH_POSTGRES_INSPECT_ALL_ROLES",
	"PTAH_SKIP_CHECKS",
}

// gatedPresenceEnvVars select Ptah-only adapter behavior outside the generic
// flag binding. Their presence changes the process contract, so strict mode
// rejects them even when the raw value is empty.
var gatedPresenceEnvVars = []string{
	"PTAH_LOG_FORMAT",
}

func validateStrictEnvironment() error {
	for _, name := range gatedBooleanEnvVars {
		value, present := os.LookupEnv(name)
		if !present {
			continue
		}
		enabled, err := envbool.Parse(name, value)
		if err != nil {
			return err
		}
		if enabled {
			return strictEnvironmentError(name)
		}
	}
	for _, name := range gatedPresenceEnvVars {
		if _, present := os.LookupEnv(name); present {
			return strictEnvironmentError(name)
		}
	}
	return nil
}

// ValidateFlagEnvironment rejects a Ptah flag environment twin that strict CE
// command construction deliberately omits. False boolean values are harmless:
// they select the same disabled state as an absent extension. Arbitrary
// PTAH-prefixed values that are not product bindings remain available to
// atlas.hcl getenv expressions.
func (p Policy) ValidateFlagEnvironment(name, value, valueType string) error {
	if !p.strictCE {
		return nil
	}
	if valueType == "bool" {
		enabled, err := envbool.Parse(name, value)
		if err != nil {
			return err
		}
		if !enabled {
			return nil
		}
	}
	return strictEnvironmentError(name)
}

func strictEnvironmentError(name string) error {
	return fmt.Errorf("%s does not allow %s", StrictCompatEnvVar, name)
}
