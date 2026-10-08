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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/internal/liquibaserun"
)

// SourceMigration is one migration read from a source tool's directory,
// normalized to Ptah's up/down model. Version is the source tool's numeric
// version (integer counter or timestamp); it is preserved so ordering and
// collisions are detectable. DownSQL is empty when the source has no rollback.
// UpNoTransaction and DownNoTransaction report a source directive saying that
// direction cannot run inside a transaction, to be translated into Ptah's
// native file directive. They are separate because Ptah reads the directive
// per direction -- CreateMigrationFromSQL parses UpTxMode and DownTxMode
// independently -- and so does dbmate. A source whose directive covers the
// whole file, as Goose's does, sets both.
type SourceMigration struct {
	Version           int64
	Name              string
	UpSQL             string
	DownSQL           string
	Repeatable        bool
	UpNoTransaction   bool
	DownNoTransaction bool
	// Path is the slash-separated path, relative to the source root, of the
	// Liquibase changelog the migration was read from, and Changeset is the
	// changeset's author:id, as [SkippedChangeset] names one. Each migration
	// is one changeset, so a caller that keeps only part of it can say which
	// changeset lost what. Both are empty for the other tools, whose migration
	// is a file of its own.
	Path      string
	Changeset string
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
	// The context is required and reaches every selected rendering call.
	// Cancellation returns no parsed result.
	Parse(ctx context.Context, fsys fs.FS) (*ParseResult, error)
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

// typedChangeRenderer converts declarations to SQL through a selected service.
type typedChangeRenderer interface {
	withRendering(target string, caps capability.Capabilities, service renderer.Service) Parser
}

// WithRendering configures a parser to render typed changes for target through
// service. Only Liquibase describes changes without SQL; other source tools
// refuse this option. Target is the provider's canonical name and may be a
// custom target. The service resolves its support when parsing reaches a typed
// change. This function does not render, connect to a server, or choose defaults.
//
// The parser receives its own capability snapshot. The caller owns the service,
// which must remain safe for concurrent calls. Parsing supplies the invocation's
// context, so configuring a parser does not capture a request lifetime.
// A recorded rendering omission refuses conversion rather than discarding part
// of the source change. The original parser is not modified.
func WithRendering(parser Parser, target string, caps capability.Capabilities, service renderer.Service) (Parser, error) {
	if parser == nil {
		return nil, errors.New("a target dialect needs a source tool: choose or detect the parser first")
	}
	if strings.TrimSpace(target) == "" {
		return nil, errors.New("a rendering target is required")
	}
	if err := schemaext.RequireRuntime(context.Background(), service); err != nil {
		return nil, err
	}
	if err := caps.Validate(); err != nil {
		return nil, fmt.Errorf("invalid capabilities for %s: %w", target, err)
	}
	rendering, ok := parser.(typedChangeRenderer)
	if !ok {
		return nil, fmt.Errorf("a target dialect applies only to a Liquibase source; %s migrations are SQL already", parser.Name())
	}
	return rendering.withRendering(target, caps.Clone(), service), nil
}

// dbmsSelector is a Parser whose source can limit a changeset to some
// databases, so importing that changeset needs the name of the database the
// history ran on.
type dbmsSelector interface {
	withDBMS(shortName string) Parser
}

// WithLiquibaseDBMS returns parser set to import the history a Liquibase
// changelog applied to one database, the one Liquibase called shortName.
//
// shortName is Liquibase's own name for the database, such as "postgresql",
// "mysql", "mariadb", "mssql", "oracle", "sqlite" or "cockroachdb" -- not a Ptah
// dialect name. The two are not the same thing: Liquibase calls a YugabyteDB
// server "yugabytedb" when its extension is installed and "postgresql" when it
// is not, and a Spanner database "cloudspanner" or "postgresql" depending on how
// it connected, so the name has to be the one the history ran under. It matches
// in any case, and a name Liquibase does not know is refused.
//
// A changeset whose `dbms` does not select shortName is left out, and so is a
// sql, sqlFile, insert or createProcedure change whose own `dbms` does not; each
// is named in [ParseResult.Skipped], with [SkippedChangeset.Change] set for a
// change. A changeset or change whose `dbms` selects it imports without the
// attribute. A changeset whose every change is left out is left out too.
// Matching follows Liquibase's rule: a comma-separated list, where `all`
// matches first, then `none` matches nothing, then `!name` excludes a database,
// and a list naming no database without `!` matches every one it does not
// exclude. A name in the list that Liquibase does not know is refused, as
// Liquibase's validation refuses it.
//
// Without it a `dbms` attribute is refused. Only a Liquibase parser takes it;
// any other is refused. The parser passed in is not modified, and the result
// keeps a dialect set by [WithRendering].
func WithLiquibaseDBMS(parser Parser, shortName string) (Parser, error) {
	if parser == nil {
		return nil, errors.New("a Liquibase database name needs a source tool: choose or detect the parser first")
	}
	name := strings.ToLower(strings.TrimSpace(shortName))
	if !liquibaserun.KnownDBMS(name) {
		return nil, fmt.Errorf("%q is not a database name Liquibase knows (known: %s)",
			shortName, strings.Join(liquibaserun.KnownDBMSNames(), ", "))
	}
	selector, ok := parser.(dbmsSelector)
	if !ok {
		return nil, fmt.Errorf("a Liquibase database name applies only to a Liquibase source, not to %s", parser.Name())
	}
	return selector.withDBMS(name), nil
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
// unconverted file in [EmitResult.Declined], and every changeset left out
// because the source tool never runs it in [EmitResult.Skipped], and a caller
// owes the user both lists. It is the single entry point the CLI uses. A missing
// or canceled context is refused before parsing and checked before writing.
func Import(ctx context.Context, sourceFS fs.FS, parser Parser, outDir string, opts Options) (*EmitResult, error) {
	if err := parseContext(ctx); err != nil {
		return nil, err
	}
	if parser == nil {
		detected, err := DetectParser(sourceFS)
		if err != nil {
			return nil, err
		}
		parser = detected
	}
	parsed, err := parser.Parse(ctx, sourceFS)
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
	if err := parseContext(ctx); err != nil {
		return nil, err
	}
	result, err := Emit(outDir, normalized, declined, opts)
	if err != nil {
		return nil, err
	}
	result.Skipped = parsed.Skipped
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

// parseContext rejects missing or canceled invocation contexts before reads and
// before Import starts writing the converted directory.
func parseContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("migration import requires a context")
	}
	return ctx.Err()
}

// parseWithContext gives every source parser the same cancellation boundary.
// A parser that completed after cancellation must not publish its partial work.
func parseWithContext(ctx context.Context, parse func() (*ParseResult, error)) (*ParseResult, error) {
	if err := parseContext(ctx); err != nil {
		return nil, err
	}
	result, err := parse()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
