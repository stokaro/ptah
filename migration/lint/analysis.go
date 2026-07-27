package lint

import (
	"fmt"
	"io/fs"
	"path"
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/internal/fsnapshot"
	"github.com/stokaro/ptah/internal/migrationsnapshot"
	"github.com/stokaro/ptah/migration/migrator"
)

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
	suppressedRules []string
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
	// only when an Atlas SQL template is rendered.
	SQL string
	// Version is the parsed migration version, or zero when the name is not
	// recognized.
	Version int64
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
	// Statements holds the parsed statements of up migrations. Empty for
	// down migrations (statement rules do not run there).
	Statements []Statement
	// Changes holds the semantic schema changes this up migration expresses,
	// recovered from Ptah's dialect-aware SQL parser. Empty for down migrations
	// and for files whose statements express no structural change. One statement
	// can contribute zero, one, or several changes, so len(Changes) is not the
	// statement count. Ordered by statement, then by the order changes appear
	// within each statement.
	Changes         []SchemaChange
	suppressedRules []string
}

// VersionSelection selects migration versions while preserving the difference
// between no selector and an explicitly empty changeset.
type VersionSelection struct {
	// Versions lists selected migration versions.
	Versions []int64
	// Restricted reports whether Versions is an explicit selection. When true,
	// an empty Versions slice selects no migrations.
	Restricted bool
}

// Analysis is an immutable migration lint result. Its accessors return deep
// copies, so callers cannot modify the captured files, findings, or source
// snapshot.
type Analysis struct {
	files    []File
	findings []Finding
	snapshot fsnapshot.Snapshot
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

// AnalyzeFS captures and analyzes every *.sql file under fsys recursively.
// Input file contents are read exactly once; template rendering, linting,
// replay, and reporting can all consume the returned immutable snapshot.
func AnalyzeFS(fsys fs.FS, opts Options) (Analysis, error) {
	if err := validateCompatibilityProfile(opts.Compatibility); err != nil {
		return Analysis{}, err
	}
	if err := validateRules(rulesForOptions(opts)); err != nil {
		return Analysis{}, err
	}
	dirFormat, err := migrator.ParseMigrationDirFormat(string(opts.DirFormat))
	if err != nil {
		return Analysis{}, err
	}
	snapshot, err := migrationsnapshot.Capture(fsys)
	if err != nil {
		return Analysis{}, err
	}
	sources, names, err := loadSQLSources(snapshot)
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
	versionDirs := map[int64]map[string]bool{}
	for _, name := range names {
		if parsed, parseErr := parseKnownMigrationName(path.Base(name), dirFormat); parseErr == nil {
			if versionDirs[parsed.Version] == nil {
				versionDirs[parsed.Version] = map[string]bool{}
			}
			versionDirs[parsed.Version][parsed.Direction] = true
		}
	}

	mode := modeForDialect(opts.Dialect)
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
		findings = append(findings, runRules(&file, opts)...)
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
		files:    files,
		findings: cloneFindings(findings),
		snapshot: snapshot,
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
		return nil, nil, fmt.Errorf("no *.sql migration files found")
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
) (File, error) {
	raw := sources[name]
	base := path.Base(name)
	direction := ""
	var version int64
	hasVersion := false
	atlasFormat := false
	repeatable := false
	if parsed, parseErr := parseKnownMigrationName(base, dirFormat); parseErr == nil {
		direction = parsed.Direction
		version = parsed.Version
		hasVersion = true
		atlasFormat = parsed.Format == migrator.MigrationDirFormatAtlas
		repeatable = parsed.Repeatable
	}
	file := File{
		Path:       path.Join(pathPrefix, name),
		Name:       name,
		Source:     raw,
		SQL:        raw,
		Version:    version,
		Repeatable: repeatable,
		Selected:   selectionIncludes(selection, version, hasVersion),
		Direction:  direction,
		// The migrator executes whatever its parser classifies as up, so
		// lint must follow it; the suffix check keeps hazard scanning for
		// .up.sql files whose version prefix is malformed.
		IsUp:           direction == "up" || strings.HasSuffix(base, ".up.sql"),
		WellFormedName: strictNameRe.MatchString(base) || atlasFormat,
		NoTransaction:  fileNoTransactionDirective(raw),
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

	// Statement rules apply to up migrations only.
	if file.IsUp {
		sql := raw
		if atlasFormat && migrator.LooksAtlasTemplateSQL(sql) {
			rendered, _, err := migrator.RenderAtlasTemplateSQL(snapshot, name, atlasTemplateData)
			if err != nil {
				return File{}, err
			}
			sql = rendered
		}
		file.SQL = sql
		for index, rawStmt := range splitStatementsWithLines(sql, mode, compatibility) {
			file.Statements = append(file.Statements, Statement{
				Index:           index,
				Span:            SourceSpan{Start: rawStmt.start, End: rawStmt.end},
				SQL:             rawStmt.text,
				Canonical:       canonicalize(rawStmt.text, mode),
				Words:           tokenizeWords(rawStmt.text, mode),
				Line:            rawStmt.line,
				sourceWords:     tokenizeSourceWords(rawStmt.text, mode),
				suppressedRules: rawStmt.suppressedRules,
			})
		}
		file.Changes = extractSchemaChanges(&file, dialect)
	}
	return file, nil
}

func selectionIncludes(selection VersionSelection, version int64, hasVersion bool) bool {
	if !selection.Restricted {
		return true
	}
	return hasVersion && slices.Contains(selection.Versions, version)
}
