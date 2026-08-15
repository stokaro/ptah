// Package atlascompatpolicy owns the process-wide policy that selects the
// complete ptah-compat surface or the Atlas Community Edition compatibility
// subset.
package atlascompatpolicy

import (
	"fmt"
	"io/fs"
	"iter"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/ptahdirective"
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

// It is [envbool.Selector]: it is the variable that chooses the policy, so it
// is parsed by the same rule as every other declared boolean and never refused
// by the policy it selects.
var strictCompat = envbool.New(StrictCompatEnvVar, false, envbool.Selector)

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
	if !p.strictCE || database == nil {
		return nil
	}
	// plpgsql is installed by PostgreSQL itself in pg_catalog. The live reader
	// reports every database extension, but that system extension is not user
	// schema content and the pinned Community Edition inspector does not expose
	// it. Keep authored extensions strict through ValidateDesiredSchema while
	// avoiding a false refusal on every ordinary PostgreSQL database.
	inspected := schemaWithoutInspectedPostgresBaselines(database)
	return p.validateSchemaObjects(&inspected, "inspected")
}

// PrepareInspectedSchema removes PostgreSQL server baselines that the pinned
// Community Edition inspector does not expose, then validates the exact state
// that downstream inspection renders. Full mode returns the original snapshot
// unchanged so its richer round-trip remains available by default.
func (p Policy) PrepareInspectedSchema(database *dbschematypes.DBSchema) (*dbschematypes.DBSchema, error) {
	if !p.strictCE || database == nil {
		return database, nil
	}
	inspected := dbSchemaWithoutInspectedPostgresBaselines(database)
	if err := p.validateSchemaObjects(dbschematogo.ConvertDBSchemaToGoSchema(inspected), "inspected"); err != nil {
		return nil, err
	}
	return inspected, nil
}

func dbSchemaWithoutInspectedPostgresBaselines(database *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	inspected := *database
	inspected.Extensions = slices.DeleteFunc(
		slices.Clone(database.Extensions),
		func(extension dbschematypes.DBExtension) bool {
			return strings.EqualFold(strings.TrimSpace(extension.Name), "plpgsql")
		},
	)
	inspected.Grants = slices.DeleteFunc(
		slices.Clone(database.Grants),
		func(grant dbschematypes.DBGrant) bool {
			return strings.EqualFold(strings.TrimSpace(grant.Role), "PUBLIC") &&
				strings.EqualFold(strings.TrimSpace(grant.Privilege), "USAGE") &&
				strings.EqualFold(strings.TrimSpace(grant.ObjectType), "SCHEMA") &&
				strings.EqualFold(strings.TrimSpace(grant.ObjectName), "public") &&
				!grant.WithOption
		},
	)
	return &inspected
}

func schemaWithoutInspectedPostgresBaselines(database *goschema.Database) goschema.Database {
	inspected := *database
	inspected.Extensions = slices.DeleteFunc(
		slices.Clone(database.Extensions),
		func(extension goschema.Extension) bool {
			return strings.EqualFold(strings.TrimSpace(extension.Name), "plpgsql")
		},
	)
	// PostgreSQL creates public with PUBLIC USAGE on current releases. The
	// ordinary reader preserves that baseline because full inspection must be
	// able to round-trip the server state. It is not authored Pro-only content,
	// however, and refusing it would make strict apply and diff reject every
	// ordinary PostgreSQL database. An explicit GRANT of the same privilege is
	// semantically indistinguishable and does not expand the server baseline.
	inspected.Grants = slices.DeleteFunc(
		slices.Clone(database.Grants),
		isPostgresPublicUsageBaseline,
	)
	return inspected
}

func isPostgresPublicUsageBaseline(grant goschema.Grant) bool {
	return strings.EqualFold(strings.TrimSpace(grant.Role), "PUBLIC") &&
		strings.EqualFold(strings.TrimSpace(grant.OnSchema), "public") &&
		strings.TrimSpace(grant.OnTable) == "" &&
		strings.TrimSpace(grant.OnSequence) == "" &&
		!grant.WithOption &&
		len(grant.Privileges) == 1 &&
		strings.EqualFold(strings.TrimSpace(grant.Privileges[0]), "USAGE")
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

// ValidateRenderedVirtualTables refuses an inspection whose rendering dropped a
// SQLite virtual table's module declaration.
//
// The rendered HCL and JSON have no virtual-table construct, so a virtual table
// becomes `table "docs" { schema = schema.main }` -- an empty block that names
// an ordinary table which, replayed, is not a full-text index. That is what the
// pinned community binary emits too, and outside strict mode Ptah matches it
// and says so on the diagnostics stream. Strict mode owns the process output
// contract, so it refuses rather than handing a pipeline a lossy document that
// looks complete.
//
// It is format-specific by construction: the caller only asks when the module
// declaration is actually missing from the output, so `--format '{{ sql . }}'`,
// which renders CREATE VIRTUAL TABLE, is never refused.
func (p Policy) ValidateRenderedVirtualTables(names []string) error {
	if !p.strictCE || len(names) == 0 {
		return nil
	}
	return fmt.Errorf(
		"Atlas Community Edition strict compatibility does not support rendering SQLite virtual %s %s"+
			" in a format that cannot carry the module declaration;"+
			" use --format '{{ sql . }}', or exclude the %s from the inspection",
		strictVirtualNoun(len(names)),
		strings.Join(names, ", "),
		strictVirtualNoun(len(names)),
	)
}

func strictVirtualNoun(count int) string {
	if count == 1 {
		return "table"
	}
	return "tables"
}

// LiveSchemaObject describes the live catalog identity relevant to Atlas
// compatibility policy. ImplicitSequence distinguishes a sequence owned by a
// table column from a standalone sequence the Community Edition surface does
// not model.
type LiveSchemaObject struct {
	Kind             string
	Name             string
	ImplicitSequence bool
}

// ValidateSchemaCleanObject refuses live object kinds that Atlas Community
// Edition does not model for schema cleanup. The refusal happens after catalog
// inspection but before confirmation or SQL execution: strict mode must not
// turn an OSS-parity run into a destructive Pro-like cleanup. Full mode keeps
// Ptah's complete cleanup coverage unchanged.
func (p Policy) ValidateSchemaCleanObject(object LiveSchemaObject) error {
	if !p.strictCE || strictCEAcceptsLiveSchemaObject(object) {
		return nil
	}
	return fmt.Errorf(
		"Atlas Community Edition strict compatibility does not support cleaning live schema %s %q",
		object.Kind,
		object.Name,
	)
}

func strictCEAcceptsLiveSchemaObject(object LiveSchemaObject) bool {
	switch object.Kind {
	case "table", "foreign_key", "enum":
		return true
	case "sequence":
		return object.ImplicitSequence
	default:
		return false
	}
}

// ValidateLiveSchemaObject refuses live catalog objects that Atlas Community
// Edition does not model during inspection, planning, or comparison. The
// ordinary schema reader cannot represent every catalog kind, so callers apply
// this check to the supplemental read-only inventory before publishing output
// or acting on an incomplete state. Full mode remains a no-op and keeps Ptah's
// best-effort behavior.
func (p Policy) ValidateLiveSchemaObject(object LiveSchemaObject) error {
	if !p.strictCE || strictCEAcceptsLiveSchemaObject(object) {
		return nil
	}
	return fmt.Errorf(
		"Atlas Community Edition strict compatibility does not support inspecting live schema %s %q",
		object.Kind,
		object.Name,
	)
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
	inspected := schemaWithoutInspectedPostgresBaselines(database)
	for _, object := range strictCEUnsupportedCleanupSnapshotObjects(&inspected) {
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
	} else if parsed, err := atlasurl.Parse(cleaned); err == nil && parsed.Scheme != "" {
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
	parsed, err := atlasurl.Parse(strings.TrimSpace(rawURL))
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
	return p.ValidateMigrationSourceForDialect(fsys, "")
}

// MigrationSourceValidator returns a strict stable-snapshot callback bound to
// the target database URL. Full mode returns nil so shared source preparation
// does not capture, checksum, or reorder errors for migration directories that
// the default compatibility surface would otherwise resolve later.
//
// URL parsing is deliberately best-effort here: the owning command retains its
// established URL diagnostic and ordering, while a URL whose dialect is not
// yet available falls back to conservative scanning.
func (p Policy) MigrationSourceValidator(databaseURL string) func(fs.FS) error {
	if !p.strictCE {
		return nil
	}
	dialect, _ := atlasurl.DialectFromURL(databaseURL)
	return func(fsys fs.FS) error {
		return p.ValidateMigrationSourceForDialect(fsys, dialect)
	}
}

// ValidateMigrationSourceForURL applies the target-aware migration policy at
// command boundaries that already hold a captured source. Full mode is a
// no-op; strict mode uses the same callback as stable-snapshot preflight.
func (p Policy) ValidateMigrationSourceForURL(fsys fs.FS, databaseURL string) error {
	validate := p.MigrationSourceValidator(databaseURL)
	if validate == nil {
		return nil
	}
	return validate(fsys)
}

// ValidateMigrationSourceForDialect applies the strict migration-content gate
// with the target engine's lexical rules. An empty dialect uses the
// conservative cross-dialect intersection for commands such as import that do
// not yet have a target, avoiding both false extension markers and silent
// acceptance of an unambiguous marker.
func (p Policy) ValidateMigrationSourceForDialect(fsys fs.FS, dialect string) error {
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
		hasCheck, hasOtherDirective, err := migrationDirectiveKinds(source, dialect)
		if err != nil {
			return fmt.Errorf("inspect migration %s for Atlas CE strict compatibility: %w", name, err)
		}
		if hasCheck {
			return fmt.Errorf("Atlas Community Edition strict compatibility does not support Ptah pre-migration checks in %s", name)
		}
		if hasOtherDirective {
			return fmt.Errorf("Atlas Community Edition strict compatibility does not support Ptah migration directives in %s", name)
		}
		if migrator.LooksAtlasTemplateSQL(source) {
			return fmt.Errorf("Atlas Community Edition strict compatibility does not support SQL template migration %s", name)
		}
		return nil
	})
}

func migrationDirectiveKinds(source, dialect string) (hasCheck, hasOther bool, err error) {
	if strings.TrimSpace(dialect) == "" {
		hasCheck, hasOther = classifyPtahDirectiveBodies(ptahdirective.ConservativeBodies(source))
		return hasCheck, hasOther, nil
	}
	hasCheck, hasOther = classifyPtahDirectiveBodies(
		ptahdirective.Bodies(source, dialectlexer.Options(dialect)),
	)
	if hasCheck {
		if _, err := migrator.ParseChecks(source, dialect); err != nil {
			return false, false, err
		}
	}
	return hasCheck, hasOther, nil
}

func classifyPtahDirectiveBodies(bodies iter.Seq[string]) (hasCheck, hasOther bool) {
	for body := range bodies {
		body = strings.TrimSpace(body)
		after, check := strings.CutPrefix(body, "check")
		if check && (after == "" || after[0] == ' ' || after[0] == '\t') {
			hasCheck = true
			continue
		}
		hasOther = true
	}
	return hasCheck, hasOther
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

// gatedPresenceEnvVars select Ptah-only adapter behavior outside the generic
// flag binding. Their presence changes the process contract, so strict mode
// rejects them even when the raw value is empty.
//
// This list stays hand-written because it names variables that are NOT
// booleans, and [envbool] governs booleans only: there is no registry to derive
// it from. cmd/internal/envboolguard is what keeps it honest -- a `PTAH_*` name
// the tree mentions has to be either a declared boolean or a written-down
// non-boolean, and a name in both classifications fails there.
var gatedPresenceEnvVars = []string{
	"PTAH_LOG_FORMAT",
}

// validateStrictEnvironment applies the strict rule to every boolean `PTAH_*`
// variable the process declares, DERIVED from [envbool.Registered] rather than
// listed here.
//
// The lists this replaced were maintained by hand next to a registry that
// already knew the answer, so the two could drift and the drift was silent: a
// variable declared correctly, and therefore invisible to cmd/internal/envboolguard,
// could still be missing from the strict lists, and a malformed value for it was
// ignored under strict mode instead of refused. Deriving from the registry means
// a variable is validated by the act of declaring it. See [envbool.Class] for
// where each variable states which side it is on.
//
// Every present value is parsed, whatever its class. That is deliberate and it
// is the half a "gated variables only" reading would drop: a malformed value on
// a retained variable would otherwise lie dormant until the run that happens to
// reach the behavior it controls.
func validateStrictEnvironment() error {
	for _, variable := range envbool.Registered() {
		if err := validateStrictBooleanEnvironment(variable); err != nil {
			return err
		}
	}
	for _, name := range gatedPresenceEnvVars {
		if _, present := os.LookupEnv(name); present {
			return strictEnvironmentError(name)
		}
	}
	return nil
}

// validateStrictBooleanEnvironment applies the rule to one declared variable.
func validateStrictBooleanEnvironment(variable envbool.Var) error {
	name := variable.Name()
	value, present := os.LookupEnv(name)
	if !present {
		return nil
	}
	enabled, err := envbool.Parse(name, value)
	if err != nil {
		return err
	}
	if enabled && strictRefusesEnabled(variable.Class()) {
		return strictEnvironmentError(name)
	}
	return nil
}

// strictRefusesEnabled reports whether an enabled value is refused.
//
// The default is refusal, and only [envbool.Retained] and [envbool.Selector]
// opt out of it. A variable whose declaration states nothing therefore fails
// CLOSED: it is refused loudly, on the first run that sets it, naming itself.
// The other default would be the silent one -- a gated variable somebody forgot
// to classify would be honored under strict mode, which is exactly the defect
// this derivation exists to close.
func strictRefusesEnabled(class envbool.Class) bool {
	return class != envbool.Retained && class != envbool.Selector
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
