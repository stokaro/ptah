package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/fileplan"
	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/rolescope"
	"go.5x5.cz/ptah/internal/schemascope"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

// InspectOptions configures Atlas-compatible schema inspection.
type InspectOptions struct {
	DevURL  string
	Schemas []string
	// Include positively selects the top-level resources inspection keeps,
	// with the same Atlas-style selectors [atlasfilter.Scope] applies to
	// schema apply and diff. Empty keeps every inspected resource, and the
	// exclusion-only path is then byte-for-byte unchanged.
	Include     []string
	Exclude     []string
	Format      string
	Diagnostics io.Writer
	// SuppressRoleCoverageNote omits only Ptah's informational note about
	// server roles outside the rendered schema scope. Other diagnostics keep
	// using Diagnostics.
	SuppressRoleCoverageNote bool
	// OmitAtlasRefusedBlocks renders HCL for the Atlas-compatible surface,
	// which leaves out the top-level block types the pinned Atlas community
	// binary refuses as a feature -- unless something else in the document
	// names the object -- and reports every decision on Diagnostics. Only
	// `ptah-compat` sets it, and setting
	// [go.5x5.cz/ptah/internal/atlashclrender.KeepAtlasRefusedBlocksEnvVar]
	// turns it back off there; the native surface renders every construct Ptah
	// models. See
	// [go.5x5.cz/ptah/internal/atlashclrender.RenderInspectedForAtlasCLI].
	OmitAtlasRefusedBlocks bool
	// CompatibilityHCLFraming selects the Atlas-compatible single-document HCL
	// frame without changing which blocks the document contains. Only
	// `ptah-compat` sets it; native inspection keeps Ptah's generated marker and
	// native terminal spacing.
	CompatibilityHCLFraming bool
	// PrepareSchema applies a caller-selected normalization and validation
	// policy to the fully introspected schema before any template renders or
	// file export is published. A returned replacement becomes the exact state
	// every downstream inspection surface sees.
	PrepareSchema func(*dbschematypes.DBSchema) (*dbschematypes.DBSchema, error)
	// ValidateSchema applies a caller-selected policy to the fully introspected
	// schema before any template renders or file export is published.
	ValidateSchema func(*goschema.Database) error
	// ValidateRenderedVirtualTables applies a caller-selected policy to the
	// SQLite virtual tables whose module declaration the chosen rendering
	// dropped. It is asked only when something was actually dropped, so a
	// format that carries the declaration never reaches it.
	ValidateRenderedVirtualTables func(names []string) error
	// ValidateLiveObject applies a caller-selected policy to supplemental
	// catalog objects before any template renders or file export is published.
	// Nil avoids the additional catalog query and preserves full-mode
	// best-effort inspection behavior.
	ValidateLiveObject func(LiveSchemaObject) error
}

// NormalizeInspectFormat returns and validates the executable Atlas schema
// inspect template.
func NormalizeInspectFormat(format string) (string, error) {
	normalized, err := atlasreport.NormalizeSchemaInspectFormat(format)
	if err != nil {
		return "", err
	}
	if err := atlasreport.ValidateSchemaInspectTemplate(normalized); err != nil {
		return "", err
	}
	return normalized, nil
}

// Inspect reads a live schema and renders it with Atlas-compatible
// formatting, applying any split/write file exports the format template
// planned.
func Inspect(ctx context.Context, conn *dbschema.DatabaseConnection, opts InspectOptions) (string, error) {
	if _, err := NormalizeInspectFormat(opts.Format); err != nil {
		return "", err
	}
	if conn == nil {
		return "", errors.New("schema inspect requires database connection")
	}
	if err := atlasurl.ValidateDialectMatch(opts.DevURL, conn.Info().Dialect); err != nil {
		return "", err
	}

	schema, names, err := readInspectSchemaWithNames(ctx, conn, opts.Schemas)
	if err != nil {
		return "", fmt.Errorf("read database schema: %w", err)
	}
	schema, err = prepareInspectSchema(schema, opts.PrepareSchema)
	if err != nil {
		return "", err
	}
	if err := validateInspectSchema(schema, opts.ValidateSchema); err != nil {
		return "", err
	}
	if err := ValidateLiveObjects(conn, names, opts.ValidateLiveObject); err != nil {
		return "", err
	}
	// A description scoped to the roles the inspected schemas use omits roles
	// that exist on the server, and the rendered document is the only thing
	// the operator sees. Reported here rather than in renderInspectSchema
	// because only this path inspects a database the operator named: the
	// file and migration-directory sources render a dev database they were
	// told to materialize, whose other cluster roles are not an answer to
	// anything they asked. See stokaro/ptah#1267.
	if !opts.SuppressRoleCoverageNote {
		rolescope.ReportUndescribed(opts.Diagnostics, schema)
	}
	validatedOpts := opts
	validatedOpts.PrepareSchema = nil
	validatedOpts.ValidateSchema = nil
	return renderInspectSchema(schema, conn.Info(), validatedOpts)
}

func prepareInspectSchema(
	schema *dbschematypes.DBSchema,
	prepare func(*dbschematypes.DBSchema) (*dbschematypes.DBSchema, error),
) (*dbschematypes.DBSchema, error) {
	if prepare == nil {
		return schema, nil
	}
	return prepare(schema)
}

func validateInspectSchema(schema *dbschematypes.DBSchema, validate func(*goschema.Database) error) error {
	if validate == nil {
		return nil
	}
	return validate(dbschematogo.ConvertDBSchemaToGoSchema(schema))
}

// renderInspectSchema is the shared inspect tail for every source kind:
// exclude filtering, report construction, format rendering, and application
// of the planned split/write file exports.
func renderInspectSchema(
	schema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
	opts InspectOptions,
) (string, error) {
	format, err := NormalizeInspectFormat(opts.Format)
	if err != nil {
		return "", err
	}
	if err := validateInspectSchema(schema, opts.ValidateSchema); err != nil {
		return "", err
	}
	schema, excludeReport, err := scopeInspectSchema(schema, info, opts)
	// Inspection is read-only and its documented answer for an empty selection
	// is an empty rendering, so it keeps exit 0 and reports the empty selection
	// on the diagnostics stream instead of failing.
	if emptySelection(err) {
		reportEmptySelection(opts.Diagnostics, err)
		err = nil
	}
	if err != nil {
		return "", err
	}
	// Inspection looks at exactly one state, so its own report is already the
	// across-states answer.
	reportUnmatchedExclude(opts.Diagnostics, atlasfilter.UnmatchedAcrossStates(excludeReport))
	// Reported from the SCOPED schema, which is the document the operator
	// receives. Emitting it before the projection let the note describe a
	// rendering that selection had already changed -- `--include users` can
	// leave a document with no virtual table in it at all, and the note still
	// said the description reports the module's tables as ordinary ones. See
	// stokaro/ptah#1028.
	sqlitevirtual.ReportUnclassified(opts.Diagnostics, schema)
	dbsch := dbschematogo.ConvertDBSchemaToGoSchema(schema)
	output, err := atlasreport.RenderSchemaInspect(format, atlasreport.NewSchemaInspectReport(
		dbsch,
		schema,
		info,
		opts.Diagnostics,
		atlasreport.SchemaInspectReportOptions{
			OmitAtlasRefusedBlocks:  opts.OmitAtlasRefusedBlocks,
			DescribeSchemas:         describesSchemas(info, opts),
			CompatibilityHCLFraming: opts.CompatibilityHCLFraming,
		},
	))
	if err != nil {
		return "", err
	}
	if err := reportOmittedVirtualTables(schema, output, opts); err != nil {
		return "", err
	}
	if err := applyInspectFileExports(output.Files); err != nil {
		return "", err
	}
	return output.Text, nil
}

// reportOmittedVirtualTables answers for a SQLite virtual table the rendering
// could not carry.
//
// Only the SQL rendering has a construct for one. HCL and JSON do not, so a
// virtual table becomes `table "docs" { schema = schema.main }` -- an empty
// block naming an ordinary table that, replayed, is not a full-text index. The
// pinned community binary emits the same lossy block, and Ptah matches it
// rather than changing the document; what it will not do is hand a pipeline
// that captures inspection output a document that looks complete and is not.
// Documentation cannot reach that pipeline. See stokaro/ptah#1028.
//
// Whether the declaration survived is read off the rendered output rather than
// classified from the template, because --format takes an arbitrary Go
// template: one that calls `sql` carries the declarations whatever else it
// does, and one that does not, does not.
//
// The planned FILES count as rendered output, not just the printed text. A
// `split | write` template deliberately leaves Text empty and puts the whole
// document in Files, so reading Text alone reported a loss for an export whose
// file holds the correct CREATE VIRTUAL TABLE -- measured as a note beside an
// `out/main.sql` that contained the statement.
func reportOmittedVirtualTables(
	schema *dbschematypes.DBSchema,
	output atlasreport.SchemaInspectOutput,
	opts InspectOptions,
) error {
	omitted := virtualTablesTheRenderingDropped(schema, output)
	if len(omitted) == 0 {
		return nil
	}

	names := sqlitevirtual.Names(omitted)
	if opts.ValidateRenderedVirtualTables != nil {
		if err := opts.ValidateRenderedVirtualTables(names); err != nil {
			return err
		}
	}
	if opts.Diagnostics == nil {
		return nil
	}
	fmt.Fprintf(
		opts.Diagnostics,
		"note: this format cannot carry a SQLite virtual table's module declaration,"+
			" so %s %s rendered as an empty table block that does not replay;"+
			" use --format '{{ sql . }}' to keep the CREATE VIRTUAL TABLE statement\n",
		strings.Join(names, ", "),
		omittedVerb(len(names)),
	)
	return nil
}

// virtualTablesTheRenderingDropped returns the virtual tables whose own
// declaration is absent from everything the render produced.
//
// It is asked per table, and per table it looks for THAT table's statement --
// the keyword followed, before the module clause, by the table's quoted name.
// Testing for the bare keyword anywhere in the output was wrong in the
// direction that suppresses the answer: a virtual table literally named
// `CREATE VIRTUAL TABLE` renders in HCL as `table "CREATE VIRTUAL TABLE"`,
// whose text contains the keyword while the document still carries no module
// declaration at all.
//
// Both halves of what the render produced count. A `split | write` template
// leaves the printed text empty and puts the whole document in the planned
// files.
func virtualTablesTheRenderingDropped(
	schema *dbschematypes.DBSchema,
	output atlasreport.SchemaInspectOutput,
) []sqlitevirtual.Table {
	virtual := sqlitevirtual.Tables(schema)
	if len(virtual) == 0 {
		return nil
	}

	rendered := []string{output.Text}
	for _, file := range output.Files {
		rendered = append(rendered, file.Data)
	}

	var omitted []sqlitevirtual.Table
	for _, table := range virtual {
		if !declarationRendered(rendered, table) {
			omitted = append(omitted, table)
		}
	}
	return omitted
}

// declarationRendered reports whether one table's module declaration appears in
// any of the rendered documents.
//
// The needle is the complete statement the renderer emits: CREATE VIRTUAL
// TABLE, the exact qualified name, USING, the module, the catalog-recorded
// arguments, and the terminator. Two looser rules were wrong on a table
// literally named `CREATE VIRTUAL TABLE`: searching for the bare keyword
// matched the HCL block `table "CREATE VIRTUAL TABLE"` and suppressed a real
// loss, and looking only for the name joined to USING let a comment such as
// `# "docs" USING fts5` pretend that lossy HCL carried the declaration.
// Checking only the statement prefix was also insufficient: the module
// arguments define the virtual table's columns and behavior.
//
// The module is spelled through the same helper the renderer writes it with, so
// a module whose name needs quoting -- `USING "fts-5"` -- is looked for the way
// it is emitted. Spelling it bare here reported a SQL document that carried the
// declaration as lossy, and strict compatibility refused it.
func declarationRendered(rendered []string, table sqlitevirtual.Table) bool {
	qualifiedName := sqlident.Quote(platform.SQLite, table.Name)
	if table.Schema != "" {
		qualifiedName = sqlident.Quote(platform.SQLite, table.Schema) + "." + qualifiedName
	}
	prefix := "CREATE VIRTUAL TABLE " + qualifiedName +
		" USING " + sqlident.BareOrQuoted(platform.SQLite, table.Module)
	suffixes := []string{";"}
	if arguments := table.Arguments; arguments != "" {
		suffixes[0] = "(" + arguments + ");"
	} else {
		// SQLite creates the same object for a module with no argument list
		// and for an explicitly empty one. The catalog reader deliberately
		// represents both with empty Arguments.
		suffixes = append(suffixes, "();")
	}
	prefixKey := foldSQLiteASCII(prefix)
	for _, document := range rendered {
		for _, suffix := range suffixes {
			if documentContainsSQLiteDeclaration(document, prefix, prefixKey, suffix) {
				return true
			}
		}
	}
	return false
}

// documentContainsSQLiteDeclaration reports whether the exact canonical
// declaration starts at an executable line boundary. Arguments can contain
// newlines, so comparing one split line at a time would reject SQL that carries
// the declaration exactly. Text after the terminator must remain whitespace;
// this also prevents a longer or commented fragment from proving survival.
func documentContainsSQLiteDeclaration(document, prefix, prefixKey, suffix string) bool {
	statementLength := len(prefix) + len(suffix)
	lexr := lexer.NewLexerWithOptions(document, dialectlexer.Options(platform.SQLite))
	tokens := make([]lexer.Token, 0)
	for token := lexr.NextToken(); token.Type != lexer.TokenEOF; token = lexr.NextToken() {
		tokens = append(tokens, token)
	}
	for _, token := range tokens {
		if token.Type != lexer.TokenIdentifier || !strings.EqualFold(token.Value, "CREATE") {
			continue
		}
		lineStart := strings.LastIndexByte(document[:token.Start], '\n') + 1
		if !sqliteTriviaOnly(document, tokens, lineStart, token.Start) ||
			len(document)-token.Start < statementLength {
			continue
		}
		candidate := document[token.Start : token.Start+statementLength]
		if foldSQLiteASCII(candidate[:len(prefix)]) == prefixKey &&
			candidate[len(prefix):] == suffix &&
			sqliteLineRemainderIsTrivia(document, tokens, token.Start+statementLength) {
			return true
		}
	}
	return false
}

// sqliteLineRemainderIsTrivia reports whether an executable declaration owns
// the rest of its line. A trailing SQL comment remains trivia; ordinary text
// does not. Tokenizing the complete document is important because a block
// comment may start before the line under inspection and end inside it.
func sqliteLineRemainderIsTrivia(document string, tokens []lexer.Token, start int) bool {
	end := len(document)
	if offset := strings.IndexByte(document[start:], '\n'); offset >= 0 {
		end = start + offset
	}
	return sqliteTriviaOnly(document, tokens, start, end)
}

// sqliteTriviaOnly reports whether a document range contains only whitespace
// and bytes covered by SQLite comment tokens. The tokens come from the whole
// document so a range beginning in the middle of a multiline comment still
// sees that comment's lexical context. This helper is used only outside the
// declaration candidate: module argument bytes remain exact even when they
// look like SQL comments.
func sqliteTriviaOnly(document string, tokens []lexer.Token, start, end int) bool {
	cursor := start
	for _, token := range tokens {
		if token.Type != lexer.TokenComment || token.End <= start || token.Start >= end {
			continue
		}
		commentStart := max(start, token.Start)
		if commentStart > cursor && strings.TrimSpace(document[cursor:commentStart]) != "" {
			return false
		}
		cursor = max(cursor, min(end, token.End))
	}
	return strings.TrimSpace(document[cursor:end]) == ""
}

// foldSQLiteASCII applies SQLite's identifier comparison to a rendered SQL
// statement prefix. SQLite folds ASCII letters but leaves non-ASCII bytes
// distinct, so Unicode case conversion would let one quoted table declaration
// stand in for a different table whose name differs only by non-ASCII case.
// Module arguments are deliberately outside the prefix: they are module-owned
// data whose bytes remain significant.
func foldSQLiteASCII(value string) string {
	folded := []byte(value)
	for index, letter := range folded {
		if letter >= 'A' && letter <= 'Z' {
			folded[index] = letter + ('a' - 'A')
		}
	}
	return string(folded)
}

func omittedVerb(count int) string {
	if count == 1 {
		return "was"
	}
	return "were"
}

// scopeInspectSchema applies the inspection selection to the introspected
// schema. --schema is honored earlier, when the schema is read, so only
// --include and --exclude reach the projection here: with --include the full
// positive projection runs (include selection, exclude subtraction, then
// cross-scope dependency validation, so inspection never renders a reference
// to an object it dropped), and without it the established exclusion-only path
// is kept unchanged.
func scopeInspectSchema(
	schema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
	opts InspectOptions,
) (*dbschematypes.DBSchema, atlasfilter.ExcludeReport, error) {
	return atlasfilter.ScopeDatabaseReport(schema, atlasfilter.Scope{
		Include:       opts.Include,
		Exclude:       opts.Exclude,
		DefaultSchema: info.Schema,
	})
}

// applyInspectFileExports hands the rendered output plan to the shared
// file-plan writer, which enforces path safety before any file is written.
func applyInspectFileExports(files []atlasreport.SchemaInspectFile) error {
	if len(files) == 0 {
		return nil
	}
	plan := make([]fileplan.File, 0, len(files))
	for _, file := range files {
		plan = append(plan, fileplan.File{Root: file.Dir, Path: file.Path, Data: file.Data})
	}
	return fileplan.Apply(plan)
}

// SplitSchemaNames expands repeated and comma-separated Atlas schema filters.
func SplitSchemaNames(values []string) []string {
	return schemascope.SplitNames(values)
}

// describesSchemas reports whether this run chose the schemas it describes,
// rather than having the connection URL choose for it.
//
// It is the same branch inspectSchemaNames takes -- an explicit `--schema`
// wins, otherwise realm scope decides -- asked a second time because the answer
// is what the SQL format needs: the pinned community binary v1.3.0 renders a
// schema it was told about and stays quiet about the one it merely connected
// to. The measurements are on
// [go.5x5.cz/ptah/internal/atlasreport.SchemaInspectReport].
func describesSchemas(info dbschematypes.DBInfo, opts InspectOptions) bool {
	if len(SplitSchemaNames(opts.Schemas)) > 0 {
		return true
	}
	return schemaselection.Realm(info.Dialect, info.URL, info.Schema)
}
