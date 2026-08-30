// Package importer converts a migration directory produced by another
// versioned-migration tool (golang-migrate, Goose, Flyway, Liquibase, dbmate)
// into Ptah's native NNNNNNNNNN_description.up.sql / .down.sql layout,
// preserving version order and history so a team can adopt Ptah without
// hand-rewriting its migration files.
//
// The package is a tool-agnostic core (SourceMigration, Parser, and the
// version-normalization and ordering rules) plus one Parser per source tool.
package importer

import (
	"cmp"
	"fmt"
	"io/fs"
	"slices"
	"strings"
)

// SourceMigration is one migration read from a source tool's directory,
// normalized to Ptah's up/down model. Version is the source tool's numeric
// version (integer counter or timestamp); it is preserved so ordering and
// collisions are detectable. DownSQL is empty when the source has no rollback.
// NoTransaction reports a source-level whole-migration directive that applies
// to both directions and must be translated to Ptah's native file directives.
type SourceMigration struct {
	Version       int64
	Name          string
	UpSQL         string
	DownSQL       string
	Repeatable    bool
	NoTransaction bool
}

// Parser reads a specific source tool's migration directory.
type Parser interface {
	// Name is the stable tool identifier used by the --from flag.
	Name() string
	// Detect reports whether fsys looks like this tool's migration directory.
	Detect(fsys fs.FS) bool
	// NamePattern is the file-name shape this tool's migrations take, quoted
	// back to the user when a file is declined for not matching it.
	NamePattern() string
	// Parse reads every migration in fsys, and records which source files it
	// used and which it deliberately turned down. It does not order or validate
	// the migrations — Normalize does, and it does not have to account for
	// files it never saw — AccountForSource does.
	Parse(fsys fs.FS) (*ParseResult, error)
}

// Parsers returns the registered source-tool parsers, in detection-preference
// order.
func Parsers() []Parser {
	return []Parser{
		golangMigrateParser{},
		gooseParser{},
		flywayParser{},
		liquibaseParser{},
		dbmateParser{},
	}
}

// ParserByName returns the parser whose Name matches tool (case-insensitive), or
// an error listing the supported tools.
func ParserByName(tool string) (Parser, error) {
	want := strings.ToLower(strings.TrimSpace(tool))
	for _, parser := range Parsers() {
		if parser.Name() == want {
			return parser, nil
		}
	}
	return nil, fmt.Errorf("unsupported source tool %q (supported: %s)", tool, supportedTools())
}

// DetectParser returns the single parser that recognizes fsys. It errors when no
// parser matches, or when more than one does (ambiguous — the caller should pass
// an explicit --from).
func DetectParser(fsys fs.FS) (Parser, error) {
	var matched []Parser
	for _, parser := range Parsers() {
		if parser.Detect(fsys) {
			matched = append(matched, parser)
		}
	}
	switch len(matched) {
	case 1:
		return matched[0], nil
	case 0:
		// A directory whose migrations all sit one level down is not an
		// undetectable directory -- it is a detectable one read at the wrong
		// depth, and "pass --from" does not help: the chosen parser then fails
		// with "no <tool> migration files found". Name the real cause
		// (stokaro/ptah#2231).
		if nested := detectBelowTopLevel(fsys); nested != nil {
			return nil, fmt.Errorf(
				"no migration files at the top level of the source directory, but %s migration files were found "+
					"below it (%s); %s reads only the top level, so point --source-dir at the directory that holds "+
					"the migrations",
				nested.parser.Name(), strings.Join(nested.examples, ", "), nested.parser.Name())
		}
		return nil, fmt.Errorf("could not detect the source migration tool; pass --from (supported: %s)", supportedTools())
	default:
		names := make([]string, len(matched))
		for i, parser := range matched {
			names[i] = parser.Name()
		}
		return nil, fmt.Errorf("source directory matches multiple tools (%s); pass --from to choose", strings.Join(names, ", "))
	}
}

// SupportedTools lists the source-tool identifiers accepted by --from, in
// detection-preference order. It is the single source of truth for the set of
// tools the importer understands.
func SupportedTools() []string {
	names := make([]string, 0, len(Parsers()))
	for _, parser := range Parsers() {
		names = append(names, parser.Name())
	}
	return names
}

func supportedTools() string {
	return strings.Join(SupportedTools(), ", ")
}

// Options configures an import run.
type Options struct {
	// DryRun reports the planned files without writing anything.
	DryRun bool
	// AllowPartial permits writing ptah.sum for an import that declined at
	// least one source file.
	//
	// Without it such an import is refused, because the alternative is the
	// failure this flag exists to prevent: ptah.sum written over the subset
	// that survived, so the truncated directory validates clean and nothing
	// downstream can establish that SQL was lost (stokaro/ptah#2231).
	AllowPartial bool
}

// Import parses sourceFS, normalizes the result, and emits Ptah migration files
// into outDir. When parser is nil the source tool is auto-detected. An import
// that would decline SQL-carrying source files fails with a
// *[PartialImportError] unless opts.AllowPartial is set (a dry run reports the
// declines instead of failing); a successful result still lists every
// unconverted file in [EmitResult.Declined], and a caller owes the user that
// list. It is the single entry point the CLI uses.
func Import(sourceFS fs.FS, parser Parser, outDir string, opts Options) (*EmitResult, error) {
	if parser == nil {
		detected, err := DetectParser(sourceFS)
		if err != nil {
			return nil, err
		}
		parser = detected
	}
	parsed, err := parser.Parse(sourceFS)
	if err != nil {
		return nil, fmt.Errorf("parse %s source: %w", parser.Name(), err)
	}
	declined, err := AccountForSource(sourceFS, parser, parsed)
	if err != nil {
		return nil, err
	}
	normalized, err := Normalize(parsed.Migrations)
	if err != nil {
		return nil, err
	}
	result, err := Emit(outDir, normalized, declined, opts)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Normalize orders migrations by version and validates them: it fails loudly on
// a duplicate version (two migrations mapping to the same Ptah file name) and on
// a non-positive version, so an ambiguous import never silently drops or
// reorders history. Repeatable migrations (no version) sort after the versioned
// ones, in parse order. The returned slice is a sorted copy; the input is not
// mutated.
func Normalize(migrations []SourceMigration) ([]SourceMigration, error) {
	versioned := make([]SourceMigration, 0, len(migrations))
	repeatable := make([]SourceMigration, 0)
	seen := make(map[int64]string, len(migrations))
	for _, migration := range migrations {
		if migration.Repeatable {
			repeatable = append(repeatable, migration)
			continue
		}
		if migration.Version <= 0 {
			return nil, fmt.Errorf("migration %q has non-positive version %d", migration.Name, migration.Version)
		}
		if existing, ok := seen[migration.Version]; ok {
			return nil, fmt.Errorf("duplicate source version %d (%q and %q)", migration.Version, existing, migration.Name)
		}
		seen[migration.Version] = migration.Name
		versioned = append(versioned, migration)
	}
	slices.SortStableFunc(versioned, func(a, b SourceMigration) int {
		return cmp.Compare(a.Version, b.Version)
	})
	return append(versioned, repeatable...), nil
}
