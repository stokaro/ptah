package lint

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/internal/atlaslint"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/migrator"
)

// errNoSQLMigrationFiles reports a directory holding no *.sql file at all. It
// is a sentinel rather than an inline message because the Atlas-compatible
// profile answers it with an empty analysis instead of a failure, and telling
// the two apart by message text would break the moment the wording changed.
var errNoSQLMigrationFiles = errors.New("no *.sql migration files found")

// loadAnalyzableSources reads the SQL sources to analyze, returning an empty
// set for a directory that holds none on a surface where that is nothing to do
// rather than a failure. Every step below iterates over the returned names, so
// an empty set analyzes to an empty result with no branch of its own.
//
// An empty migration directory analyzes to nothing on the Atlas-compatible
// surface, where the pinned community binary v1.3.0 exits 0 with no output for
// `migrate lint --latest 1` on one.
//
// The native profile keeps the refusal: `ptah migrations lint` names a
// directory to analyze, and answering "no findings" for a directory that holds
// nothing to analyze reports success for work never done. The Atlas surface
// cannot afford that reading, because its verb runs in a repository's CI
// before the first migration exists (stokaro/ptah#1241, adjacent to item 7).
func loadAnalyzableSources(
	fsys fs.FS,
	profile CompatibilityProfile,
) (sqlSources, []string, error) {
	sources, names, err := loadSQLSources(fsys)
	if err == nil {
		return sources, names, nil
	}
	if profile == CompatibilityProfileAtlas && errors.Is(err, errNoSQLMigrationFiles) {
		return make(sqlSources), nil, nil
	}
	return nil, nil, err
}

// CompatibilityProfile selects command-surface-specific lint semantics.
type CompatibilityProfile string

const (
	// CompatibilityProfileNative preserves Ptah rule codes, directives, and
	// safety behavior. It is the zero value and the default for every native
	// API and command.
	CompatibilityProfileNative CompatibilityProfile = ""
	// CompatibilityProfileAtlas enables Atlas directive aliases and file-header
	// suppression semantics without changing native Ptah behavior.
	CompatibilityProfileAtlas CompatibilityProfile = "atlas"
)

// SourceSpan identifies a half-open byte range in [File.SQL].
type SourceSpan struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// SubjectKind identifies the kind of schema object affected by a finding.
type SubjectKind string

const (
	// SubjectTable identifies a database table.
	SubjectTable SubjectKind = "table"
	// SubjectColumn identifies a database column.
	SubjectColumn SubjectKind = "column"
)

// Subject identifies one schema object affected by a finding.
type Subject struct {
	Kind     SubjectKind `json:"kind"`
	Name     string      `json:"name"`
	Parent   string      `json:"parent,omitempty"`
	DataType string      `json:"data_type,omitempty"`
}

// FindingContext ties a finding to one exact statement and its affected
// schema objects. File-level findings have a nil context.
type FindingContext struct {
	StatementIndex int       `json:"statement_index"`
	Subjects       []Subject `json:"subjects,omitempty"`
}

// Finding is one rule hit in one migration file.
type Finding struct {
	Rule     string   `json:"rule"`
	Title    string   `json:"title"`
	Severity Severity `json:"severity"`
	File     string   `json:"file"`
	// Line is the 1-based line of the offending statement's first token;
	// zero for file-level findings (naming, pairing).
	Line    int    `json:"line,omitempty"`
	Message string `json:"message"`
	// Context identifies the exact analyzed statement. Renderers should use it
	// instead of trying to match findings back to statements by line number.
	Context *FindingContext `json:"context,omitempty"`
}

// Statement is one SQL statement of a migration file, in the forms rules
// consume.
type Statement struct {
	// Index is this statement's stable zero-based position in [File.Statements].
	Index int
	// Span is the statement's byte range in [File.SQL].
	Span SourceSpan
	// SQL is the raw statement text as written (comments included).
	SQL string
	// Canonical is the comment-stripped, whitespace-collapsed, uppercased
	// display form. String literals keep their original case.
	Canonical string
	// Words is the token-word sequence the built-in rules scan: comments and
	// whitespace dropped, bare keywords/identifiers uppercased, punctuation
	// as its own entry, string literals and quoted identifiers kept verbatim
	// as single opaque words (so a literal containing "DROP COLUMN" or a
	// column named "type" can never impersonate a keyword).
	Words []string
	// Line is the 1-based line number of the statement's first token.
	Line            int
	sourceWords     []string
	suppressedRules []atlaslint.Target
}

// File is one migration file prepared for linting.
type File struct {
	// Path is the file path as it should appear in findings (reporting
	// prefix included).
	Path string
	// Name is the slash-separated path of the file relative to the linted
	// directory (bare file name for root-level files).
	Name string
	// Source is the migration file exactly as captured from the input
	// filesystem.
	Source string
	// SQL is the executable SQL analyzed by the linter. It differs from Source
	// when an Atlas SQL template is rendered or when a txtar migration.sql
	// section is extracted. Statement spans index SQL; reported line numbers
	// are translated back to Source.
	SQL string
	// Version is the parsed migration version, or zero when the name is not
	// recognized.
	Version int64
	// RevisionVersion is the revision-table token for parsed migration files.
	// Atlas repeatables use string keys such as "3R" and "R".
	RevisionVersion string
	// Repeatable reports whether the migrator classifies this as an imported
	// repeatable migration rather than an ordered version.
	Repeatable bool
	// Selected reports whether this file belongs to the requested version
	// selection. Analysis keeps unselected files so reports can distinguish a
	// changeset from the complete migration directory.
	Selected bool
	// Ignored reports whether the Atlas compatibility profile classified this
	// file as wholly ignored through a file-header nolint directive. Native
	// analysis never sets this field.
	Ignored bool
	// Direction is the migration direction ("up"/"down") exactly as the
	// migrator's name parser classifies this file; empty when the migrator
	// cannot parse the name at all.
	Direction string
	// IsUp reports whether statement rules treat this as an up migration:
	// the migrator parses it as up, or its name carries the .up.sql suffix.
	IsUp bool
	// IsDown reports whether this is the rollback half of a migration: the
	// migrator parses it as down, or its name carries the .down.sql suffix.
	//
	// A down file is executed against a production database exactly like an up
	// file, so a hazard in one is a hazard in the other. Rules stay up-only by
	// default because most of them describe a forward change, and the ones that
	// describe a statement's cost or its executability opt in with
	// [Rule.AppliesToDown].
	IsDown bool
	// HasPair reports whether the matching counterpart file (down for up,
	// up for down) exists in the same directory.
	HasPair bool
	// WellFormedName reports whether the name matches the documented
	// NNNNNNNNNN_description.(up|down).sql convention, checked
	// independently of the migrator's parser as defense in depth (see the
	// strictNameRe comment).
	WellFormedName bool
	// NoTransaction reports whether file-scoped directives opt this migration
	// out of the migrator's transaction wrapper.
	NoTransaction bool
	// Statements holds the parsed statements of up and down migrations. Empty
	// for any other file. A rule that reads them without checking direction
	// gets both, which is why every direction-sensitive rule tests IsUp.
	Statements []Statement
	// Changes holds the semantic schema changes this up migration expresses,
	// recovered from Ptah's dialect-aware SQL parser. Empty for down migrations
	// and for files whose statements express no structural change. One statement
	// can contribute zero, one, or several changes, so len(Changes) is not the
	// statement count. Ordered by statement, then by the order changes appear
	// within each statement.
	Changes []SchemaChange
	// scopeExcluded holds the statement indexes the reviewed-schema filter left
	// out, so a finding about one of them is dropped by the same decision that
	// dropped its change. Without it the two disagreed: a rule that attaches no
	// subject produced a finding for a statement the scope had already removed
	// from the counts (stokaro/ptah#1249).
	scopeExcluded   map[int]bool
	suppressedRules []atlaslint.Target
	// compatibility is the profile this run was started with. A rule reads it
	// when the two command surfaces model the same statement differently; see
	// [renamedNames] for the one construct where they do.
	compatibility CompatibilityProfile
	// baseline is the schema state this file's version starts from, when the
	// caller supplied one. Rules read it for facts the statement cannot carry.
	baseline baselineColumns
}

// VersionSelection selects migration versions while preserving the difference
// between no selector and an explicitly empty changeset.
type VersionSelection struct {
	// Versions lists selected migration versions.
	Versions []int64
	// VersionKeys lists selected revision-table tokens. It is used for
	// Atlas-style repeatable files whose identity is not only numeric.
	VersionKeys []string
	// Restricted reports whether Versions is an explicit selection. When true,
	// empty version selectors select no migrations.
	Restricted bool
}

// Analysis is an immutable migration lint result. Its accessors return deep
// copies, so callers cannot modify the captured files, findings, or source
// snapshot.
type Analysis struct {
	files            []File
	findings         []Finding
	snapshot         fsnapshot.Snapshot
	baselineVersions []int64
	unmetInputs      []UnmetInput
}

// Files returns every prepared migration file in the captured directory.
func (a Analysis) Files() []File {
	return cloneFiles(a.files)
}

// SelectedFiles returns the prepared files selected for linting.
func (a Analysis) SelectedFiles() []File {
	files := make([]File, 0, len(a.files))
	for i := range a.files {
		if a.files[i].Selected {
			files = append(files, cloneFile(a.files[i]))
		}
	}
	return files
}

// Findings returns the ordered lint findings.
func (a Analysis) Findings() []Finding {
	return cloneFindings(a.findings)
}

// BaselineVersions returns the analyzed migration versions whose starting
// schema state would let a second analysis say more than SQL text alone can,
// sorted ascending.
//
// It is empty for a run that already has nothing to gain, so a caller can skip
// the dev-database round trips and the second analysis entirely. Feed the
// versions back through [Options.Baseline]; see [BaselineColumn].
func (a Analysis) BaselineVersions() []int64 {
	return slices.Clone(a.baselineVersions)
}

// UnmetInputs returns every rule that asked for an analyzer input this run did
// not supply, ordered by file and then by rule code.
//
// It is empty for a run that gave every rule what it asked for, which is every
// run whose second pass had a baseline and every run with nothing to resolve.
// A non-empty result means the analysis was thinner than the same directory
// would get with a dev database it could read -- not that anything failed, and
// not that the findings reported are wrong (stokaro/ptah#1632).
func (a Analysis) UnmetInputs() []UnmetInput {
	return slices.Clone(a.unmetInputs)
}

// SnapshotFS returns a read-only filesystem containing the SQL sources,
// integrity files, and lint configuration captured for this analysis. Each call
// returns an independent snapshot view.
func (a Analysis) SnapshotFS() fs.FS {
	return a.snapshot.Clone()
}

func cloneFiles(files []File) []File {
	cloned := make([]File, len(files))
	for i := range files {
		cloned[i] = cloneFile(files[i])
	}
	return cloned
}

func cloneFile(file File) File {
	file.suppressedRules = slices.Clone(file.suppressedRules)
	file.Changes = slices.Clone(file.Changes)
	file.scopeExcluded = maps.Clone(file.scopeExcluded)
	statements := file.Statements
	file.Statements = make([]Statement, len(statements))
	for i := range statements {
		file.Statements[i] = cloneStatement(statements[i])
	}
	return file
}

func cloneStatement(statement Statement) Statement {
	statement.Words = slices.Clone(statement.Words)
	statement.sourceWords = slices.Clone(statement.sourceWords)
	statement.suppressedRules = slices.Clone(statement.suppressedRules)
	return statement
}

func cloneFindings(findings []Finding) []Finding {
	cloned := make([]Finding, len(findings))
	copy(cloned, findings)
	for i := range cloned {
		if cloned[i].Context == nil {
			continue
		}
		context := *cloned[i].Context
		context.Subjects = slices.Clone(context.Subjects)
		cloned[i].Context = &context
	}
	return cloned
}

// ValidateOptions checks a lint policy without reading migration files. Apply
// gates should call it before any early return or bypass that can skip analysis.
func ValidateOptions(opts Options) error {
	_, _, err := validateOptions(opts)
	return err
}

func validateOptions(opts Options) (migrator.MigrationDirFormat, []Rule, error) {
	if err := validateCompatibilityProfile(opts.Compatibility); err != nil {
		return "", nil, err
	}
	rules, err := rulesForOptions(opts)
	if err != nil {
		return "", nil, err
	}
	if err := validateRules(rules); err != nil {
		return "", nil, err
	}
	if err := validateRuleConfigs(opts.RuleConfigs); err != nil {
		return "", nil, err
	}
	if err := validateRuleSelectors(opts.Disabled); err != nil {
		return "", nil, err
	}
	if err := validateConfiguredRuleSelectors(rules, opts); err != nil {
		return "", nil, err
	}
	dirFormat, parseErr := migrator.ParseMigrationDirFormat(string(opts.DirFormat))
	return dirFormat, rules, parseErr
}

// AnalyzeFS captures and analyzes every *.sql file under fsys recursively.
// Input file contents are read exactly once; template rendering, linting,
// replay, and reporting can all consume the returned immutable snapshot.
func AnalyzeFS(fsys fs.FS, opts Options) (Analysis, error) {
	dirFormat, rules, err := validateOptions(opts)
	if err != nil {
		return Analysis{}, err
	}
	snapshot, err := migrationsnapshot.Capture(fsys)
	if err != nil {
		return Analysis{}, err
	}
	sources, names, err := loadAnalyzableSources(snapshot, opts.Compatibility)
	if err != nil {
		return Analysis{}, err
	}
	names, err = filterAtlasTemplateSupportNames(sources, names, dirFormat)
	if err != nil {
		return Analysis{}, err
	}

	present := make(map[string]struct{}, len(names))
	for _, name := range names {
		present[name] = struct{}{}
	}

	// Directions present per version, so pairing matches the migrator, which
	// pairs an up and a down by their shared version prefix regardless of
	// description, not by an identical file-name stem.
	versionDirs := make(map[int64]map[string]bool)
	for _, name := range names {
		if parsed, parseErr := parseKnownMigrationName(path.Base(name), dirFormat); parseErr == nil {
			if versionDirs[parsed.Version] == nil {
				versionDirs[parsed.Version] = make(map[string]bool)
			}
			versionDirs[parsed.Version][parsed.Direction] = true
		}
	}

	mode := modeForDialect(opts.Dialect)
	scope := newSchemaScope(opts.SchemaScope)
	baseline := newBaselineIndex(normalizeBaselineColumns(opts.Baseline))
	files := make([]File, 0, len(names))
	for _, name := range names {
		file, err := prepareFile(
			snapshot,
			sources,
			name,
			present,
			versionDirs,
			opts.PathPrefix,
			mode,
			opts.Dialect,
			opts.AtlasTemplateData,
			dirFormat,
			opts.Selection,
			opts.Compatibility,
			scope,
			baseline,
		)
		if err != nil {
			return Analysis{}, err
		}
		files = append(files, file)
	}

	var findings []Finding
	for i := range files {
		if !files[i].Selected {
			continue
		}
		file := cloneFile(files[i])
		findings = append(findings, scope.keepFindings(file.scopeExcluded, runRules(&file, opts, rules))...)
	}
	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].File != findings[j].File {
			return findings[i].File < findings[j].File
		}
		if findings[i].Line != findings[j].Line {
			return findings[i].Line < findings[j].Line
		}
		return findings[i].Rule < findings[j].Rule
	})

	return Analysis{
		files:            files,
		findings:         cloneFindings(findings),
		snapshot:         snapshot,
		baselineVersions: baselineVersions(files, opts, rules),
		unmetInputs:      unmetInputs(files, opts, rules),
	}, nil
}

// LintFS lints every *.sql file under fsys and returns findings ordered by
// file, line, and rule code. Call [AnalyzeFS] when prepared files or the source
// snapshot are also needed.
func LintFS(fsys fs.FS, opts Options) ([]Finding, error) {
	analysis, err := AnalyzeFS(fsys, opts)
	if err != nil {
		return nil, err
	}
	return analysis.Findings(), nil
}

type sqlSources map[string]string

func loadSQLSources(fsys fs.FS) (sqlSources, []string, error) {
	var names []string
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && strings.EqualFold(path.Ext(p), ".sql") {
			names = append(names, p)
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("list migration files: %w", err)
	}
	if len(names) == 0 {
		return nil, nil, errNoSQLMigrationFiles
	}
	sort.Strings(names)
	sources := make(sqlSources, len(names))
	for _, name := range names {
		raw, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read %s: %w", name, err)
		}
		sources[name] = string(raw)
	}
	return sources, names, nil
}

func validateCompatibilityProfile(profile CompatibilityProfile) error {
	switch profile {
	case CompatibilityProfileNative, CompatibilityProfileAtlas:
		return nil
	default:
		return fmt.Errorf("unsupported lint compatibility profile %q", profile)
	}
}

func filterAtlasTemplateSupportNames(
	sources sqlSources,
	names []string,
	dirFormat migrator.MigrationDirFormat,
) ([]string, error) {
	if !hasAtlasTemplateMigration(sources, names, dirFormat) {
		return names, nil
	}

	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if _, err := parseKnownMigrationName(path.Base(name), dirFormat); err == nil {
			filtered = append(filtered, name)
			continue
		}
		if !isAtlasTemplateSupportFile(sources[name]) {
			filtered = append(filtered, name)
		}
	}
	return filtered, nil
}

func hasAtlasTemplateMigration(
	sources sqlSources,
	names []string,
	dirFormat migrator.MigrationDirFormat,
) bool {
	for _, name := range names {
		parsed, err := parseKnownMigrationName(path.Base(name), dirFormat)
		if err != nil || parsed.Format != migrator.MigrationDirFormatAtlas {
			continue
		}
		if migrator.LooksAtlasTemplateSQL(sources[name]) {
			return true
		}
	}
	return false
}

func isAtlasTemplateSupportFile(sql string) bool {
	return migrator.LooksAtlasTemplateSQL(sql) && strings.Contains(sql, "define ")
}

// prepareFile loads one migration file into the forms rules consume.
func prepareFile(
	snapshot fsnapshot.Snapshot,
	sources sqlSources,
	name string,
	present map[string]struct{},
	versionDirs map[int64]map[string]bool,
	pathPrefix string,
	mode scanMode,
	dialect string,
	atlasTemplateData any,
	dirFormat migrator.MigrationDirFormat,
	selection VersionSelection,
	compatibility CompatibilityProfile,
	scope schemaScope,
	baseline map[int64]baselineColumns,
) (File, error) {
	raw := sources[name]
	base := path.Base(name)
	direction := ""
	var version int64
	revisionVersion := ""
	hasVersion := false
	atlasFormat := false
	repeatable := false
	if parsed, parseErr := parseKnownMigrationName(base, dirFormat); parseErr == nil {
		direction = parsed.Direction
		version = parsed.Version
		revisionVersion = parsed.RevisionVersion()
		hasVersion = true
		atlasFormat = parsed.Format == migrator.MigrationDirFormatAtlas
		repeatable = parsed.Repeatable
	}
	file := File{
		Path:            path.Join(pathPrefix, name),
		Name:            name,
		Source:          raw,
		SQL:             raw,
		Version:         version,
		RevisionVersion: revisionVersion,
		Repeatable:      repeatable,
		Selected:        selectionIncludes(selection, version, revisionVersion, hasVersion),
		Direction:       direction,
		// The migrator executes whatever its parser classifies as up, so
		// lint must follow it; the suffix check keeps hazard scanning for
		// .up.sql files whose version prefix is malformed.
		IsUp:           direction == "up" || strings.HasSuffix(base, ".up.sql"),
		IsDown:         direction == "down" || strings.HasSuffix(base, ".down.sql"),
		WellFormedName: strictNameRe.MatchString(base) || atlasFormat,
		compatibility:  compatibility,
		baseline:       baseline[version],
	}
	if compatibility == CompatibilityProfileAtlas {
		file.suppressedRules, file.Ignored = parseAtlasFileNoLint(raw)
	}
	switch {
	case atlasFormat:
		file.HasPair = true
	case hasVersion:
		// Pair by version, matching the migrator: the counterpart is any
		// file of the same version in the opposite direction.
		counterpart := "down"
		if direction == "down" {
			counterpart = "up"
		}
		file.HasPair = versionDirs[version][counterpart]
	case file.IsUp:
		_, file.HasPair = present[strings.TrimSuffix(name, ".up.sql")+".down.sql"]
	case strings.HasSuffix(name, ".down.sql"):
		_, file.HasPair = present[strings.TrimSuffix(name, ".down.sql")+".up.sql"]
	}

	// Both halves of a migration are executed against the database, so both are
	// parsed into statements. Which rules read them is decided per rule; see
	// [Rule.AppliesToDown]. Schema changes stay up-only: a change set is the
	// forward delta this version applies, and the rollback is not a second one.
	if file.IsUp || file.IsDown {
		sql := raw
		if atlasFormat && migrator.LooksAtlasTemplateSQL(sql) {
			rendered, _, err := migrator.RenderAtlasTemplateSQL(snapshot, name, atlasTemplateData)
			if err != nil {
				return File{}, err
			}
			sql = rendered
		}
		up, err := migrator.ParseMigrationUpForAnalysis(name, sql)
		if err != nil {
			return File{}, err
		}
		file.SQL = up.SQL
		file.NoTransaction = up.TxMode == migrator.MigrationFileTxModeNone
		for index, rawStmt := range splitStatementsWithLines(up.SQL, mode, compatibility) {
			file.Statements = append(file.Statements, Statement{
				Index:           index,
				Span:            SourceSpan{Start: rawStmt.start, End: rawStmt.end},
				SQL:             rawStmt.text,
				Canonical:       canonicalize(rawStmt.text, mode),
				Words:           tokenizeWords(rawStmt.text, mode),
				Line:            rawStmt.line + up.SourceLineOffset,
				sourceWords:     tokenizeSourceWords(rawStmt.text, mode),
				suppressedRules: rawStmt.suppressedRules,
			})
		}
		if file.IsUp {
			file.Changes, file.scopeExcluded = extractSchemaChanges(&file, dialect, scope)
		}
	}
	return file, nil
}

func selectionIncludes(selection VersionSelection, version int64, revisionVersion string, hasVersion bool) bool {
	if !selection.Restricted {
		return true
	}
	if len(selection.VersionKeys) > 0 {
		return revisionVersion != "" && slices.Contains(selection.VersionKeys, revisionVersion)
	}
	return hasVersion && slices.Contains(selection.Versions, version)
}
