package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/hclext"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/chpolicyexport"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/defaultprivnote"
	"ptah.run/internal/featurejson"
	"ptah.run/internal/fileplan"
	"ptah.run/internal/hashshard"
	"ptah.run/internal/providerurl"
	"ptah.run/internal/readscope"
	"ptah.run/internal/rolescope"
	"ptah.run/internal/schemascope"
	"ptah.run/internal/schemaselection"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/timescale"
	"ptah.run/internal/unmodelednote"
	"ptah.run/internal/unreadtables"
)

// InspectRuntime supplies conversion, reporting, and schema rendering through
// one selected build.
type InspectRuntime interface {
	schemaext.ValueContextRuntime
	goschematodb.Runtime
	schemacomparison.Runtime
	atlasreport.InspectRuntime
	yamlext.Runtime
	sqlext.Runtime
	hclext.Runtime
	selectext.Runtime
}

// InspectOptions configures Atlas-compatible schema inspection.
type InspectOptions struct {
	// Runtime selects required feature conversion and reporting services.
	Runtime InspectRuntime
	DevURL  string
	Schemas []string
	// Include positively selects the top-level resources inspection keeps,
	// with the same Atlas-style selectors [atlasfilter.Scope] applies to
	// schema apply and diff. Empty keeps every inspected resource, and the
	// exclusion-only path is then byte-for-byte unchanged.
	Include []string
	Exclude []string
	// ExcludeSources names where the run read each Exclude selector, for the
	// diagnostic about one that matched nothing; see
	// [atlasfilter.ExcludeSources]. Nil names every selector as --exclude's.
	ExcludeSources atlasfilter.ExcludeSources
	Format         string
	Diagnostics    io.Writer
	// NativeJSON preserves the scoped desired model for the named json format.
	// It uses the selected runtime codecs for objects, facets, and coverage.
	NativeJSON bool
	// SuppressRoleCoverageNote omits only Ptah's informational note about
	// server roles outside the rendered schema scope. Other diagnostics keep
	// using Diagnostics.
	SuppressRoleCoverageNote bool
	// reportUnmodeled names the objects of a kind Ptah does not model that the
	// scoped schema holds. Only [Inspect] sets it: it reads a database the
	// operator named, where the file and migration-directory sources read a
	// dev database they materialized, whose cluster objects answer nothing
	// they asked.
	reportUnmodeled bool
	// OmitAtlasRefusedBlocks renders HCL for the Atlas-compatible surface,
	// which leaves out the top-level block types the pinned Atlas community
	// binary refuses as a feature -- unless something else in the document
	// names the object -- and reports every decision on Diagnostics. Only
	// `ptah-compat` sets it, and setting
	// [ptah.run/internal/atlashclrender.KeepAtlasRefusedBlocksEnvVar]
	// turns it back off there; the native surface renders every construct Ptah
	// models. See
	// [ptah.run/internal/atlashclrender.RenderInspectedForAtlasCLI].
	OmitAtlasRefusedBlocks bool
	// CompatibilityHCLFraming selects the Atlas-compatible single-document HCL
	// frame without changing which blocks the document contains. Only
	// `ptah-compat` sets it; native inspection keeps Ptah's generated marker and
	// native terminal spacing.
	CompatibilityHCLFraming bool
	// SQLCreationPlan, when set, makes the `sql` helper write the plan that
	// creates the inspected schema on a new database of the same server,
	// planned through this runtime in the statement style and shape `schema
	// diff` plans in, each statement under the comment naming its change. It
	// also names the schema of every object a read of the whole database
	// returns, as that plan names them. Only `ptah-compat` sets it; nil keeps
	// the rendered schema native inspection writes.
	SQLCreationPlan CreationPlanRuntime
	// PrepareSchema applies a caller-selected normalization and validation
	// policy to the fully introspected schema before any template renders or
	// file export is published. A returned replacement becomes the exact state
	// every downstream inspection surface sees.
	PrepareSchema func(context.Context, *catalog.Database, catalog.ServerInfo) (*catalog.Database, error)
	// ValidateSchema applies a caller-selected policy to the fully introspected
	// schema before any template renders or file export is published.
	ValidateSchema func(*schemamodel.Database) error
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
func Inspect(ctx context.Context, conn *dbschema.DatabaseConnection, opts InspectOptions) (InspectResult, error) {
	if err := schemaext.RequireRuntime(ctx, opts.Runtime); err != nil {
		return InspectResult{}, err
	}
	if _, err := NormalizeInspectFormat(opts.Format); err != nil {
		return InspectResult{}, err
	}
	if conn == nil {
		return InspectResult{}, errors.New("schema inspect requires database connection")
	}
	if err := providerurl.ValidateDialectMatch(ctx, opts.DevURL, conn.Info().Dialect); err != nil {
		return InspectResult{}, err
	}

	ctx = atlasfilter.ReadScope(ctx, opts.Runtime.Selections(), opts.Exclude, opts.Include)
	if opts.SQLCreationPlan != nil {
		// The plan names the schema of every object, as `schema diff` does;
		// see [readscope.Qualified]. The HCL and JSON documents are the same
		// either way, since both name each object's schema already.
		ctx = readscope.Qualified(ctx)
	}
	schema, names, err := readInspectSchemaWithNames(ctx, conn, opts.Schemas)
	if err != nil {
		return InspectResult{}, fmt.Errorf("read database schema: %w", err)
	}
	schema, err = prepareInspectSchema(ctx, schema, conn.Info(), opts.PrepareSchema)
	if err != nil {
		return InspectResult{}, err
	}
	if err := validateInspectSchema(ctx, schema, conn.Info().Dialect, opts); err != nil {
		return InspectResult{}, err
	}
	if err := ValidateLiveObjects(conn, names, opts.ValidateLiveObject); err != nil {
		return InspectResult{}, err
	}
	// A description scoped to the roles the inspected schemas use omits roles
	// that exist on the server, and the rendered document is the only thing
	// the operator sees. Reported here rather than in renderInspectSchema
	// because only this path inspects a database the operator named: the
	// file and migration-directory sources render a dev database they were
	// told to materialize, whose other cluster roles are not an answer to
	// anything they asked. See stokaro/ptah#1267.
	if !opts.SuppressRoleCoverageNote {
		rolescope.ReportUndescribed(opts.Diagnostics, conn.Info().Dialect, schema)
	}
	// A table the read found and left out is in no rendered document, as
	// through the built-in engines; the note is the only place it shows.
	unreadtables.Report(opts.Diagnostics, schema)
	validatedOpts := opts
	validatedOpts.reportUnmodeled = true
	validatedOpts.PrepareSchema = nil
	validatedOpts.ValidateSchema = nil
	return renderInspectSchema(ctx, schema, conn.Info(), validatedOpts)
}

func prepareInspectSchema(
	ctx context.Context,
	schema *catalog.Database,
	info catalog.ServerInfo,
	prepare func(context.Context, *catalog.Database, catalog.ServerInfo) (*catalog.Database, error),
) (*catalog.Database, error) {
	if prepare == nil {
		return schema, nil
	}
	return prepare(ctx, schema, info)
}

func validateInspectSchema(ctx context.Context, schema *catalog.Database, dialect string, opts InspectOptions) error {
	if opts.ValidateSchema == nil {
		return nil
	}
	model, err := dbschematogo.ConvertDBSchemaToGoSchema(ctx, schema, dialect, opts.Runtime)
	if err != nil {
		return err
	}
	return opts.ValidateSchema(model)
}

// InspectResult is a rendered inspection and the schema it describes.
//
// The two travel together because a caller that wants a diagram wants the same
// schema the document describes, and re-reading the source to get it would give
// a second answer -- from a database that may have changed, or from a dev
// database materialized a second time.
//
// Schema is the SCOPED schema: --schema, --include and --exclude have already
// narrowed it, exactly as they narrowed Rendered. A diagram drawn from a wider
// schema than the document beside it would show tables the operator asked not
// to see.
type InspectResult struct {
	// Rendered is the document --format asked for.
	Rendered string
	// Schema is the same schema as a model, for a caller rendering something
	// else from it. It is nil only when Rendered is empty and an error is
	// returned.
	Schema *schemamodel.Database
}

// renderInspectSchema is the shared inspect tail for every source kind:
// exclude filtering, report construction, format rendering, and application
// of the planned split/write file exports.
func renderInspectSchema(
	ctx context.Context,
	schema *catalog.Database,
	info catalog.ServerInfo,
	opts InspectOptions,
) (InspectResult, error) {
	if err := schemaext.RequireRuntime(ctx, opts.Runtime); err != nil {
		return InspectResult{}, err
	}
	format, err := NormalizeInspectFormat(opts.Format)
	if err != nil {
		return InspectResult{}, err
	}
	if err := validateInspectSchema(ctx, schema, info.Dialect, opts); err != nil {
		return InspectResult{}, err
	}
	// From the whole read, before selection: a ClickHouse row policy is
	// matched to its table by name, and a table a selector leaves out is not
	// a table the database dropped. A policy whose table is gone has nothing
	// a document can declare it with; the warning names the statement that
	// drops it.
	schema, err = chpolicyexport.Without(ctx, opts.Diagnostics, schema)
	if err != nil {
		return InspectResult{}, err
	}
	schema, excludeReport, err := scopeInspectSchema(ctx, schema, info, opts)
	// Inspection is read-only and its documented answer for an empty selection
	// is an empty rendering, so it keeps exit 0 and reports the empty selection
	// on the diagnostics stream instead of failing.
	if emptySelection(err) {
		reportEmptySelection(opts.Diagnostics, err)
		err = nil
	}
	if err != nil {
		return InspectResult{}, err
	}
	// Inspection looks at exactly one state, so its own report is already the
	// across-states answer.
	reportUnmatchedExclude(opts.Diagnostics, atlasfilter.UnmatchedAcrossStates(excludeReport), opts.ExcludeSources, atlasfilter.StoredSpellings(excludeReport))
	// Reported from the SCOPED schema, which is the document the operator
	// receives. Emitting it before the projection let the note describe a
	// rendering that selection had already changed -- `--include users` can
	// leave a document with no virtual table in it at all, and the note still
	// said the description reports the module's tables as ordinary ones. See
	// stokaro/ptah#1028.
	sqlitevirtual.ReportUnclassified(opts.Diagnostics, schema)
	// From the SCOPED schema for the same reason, and it is the same failure:
	// `--exclude conditions` removes the hypertable from the document, and a
	// note emitted before the projection would still name it as a table the
	// description carries incompletely -- sending the reader to look for a
	// statement that is not there (stokaro/ptah#1026).
	//
	// It is not behind the role-coverage suppression either: that flag exists
	// for the paths inspecting a dev database they materialized themselves, and
	// those have no hypertables in them, so the note is silent there rather
	// than suppressed.
	timescale.ReportUndescribed(opts.Diagnostics, schema)
	// Undescribed default privileges are not in any document this renders, and
	// the list survives selection unchanged, so the note says what the read
	// left out (stokaro/ptah#3737, stokaro/ptah#3770).
	defaultprivnote.ReportUndescribed(opts.Diagnostics, schema)
	// From the scoped schema too, so a table a selector left out is not named
	// (stokaro/ptah#3771).
	hashshard.ReportUndescribed(opts.Diagnostics, schema)
	// An object of a kind Ptah does not model is in no rendered document
	// either, and from the scoped schema an object a selector left out is not
	// named.
	if opts.reportUnmodeled {
		unmodelednote.Report(ctx, opts.Diagnostics, "the database", schema)
	}
	dbsch, err := dbschematogo.ConvertDBSchemaToGoSchema(ctx, schema, info.Dialect, opts.Runtime)
	if err != nil {
		return InspectResult{}, err
	}
	if opts.NativeJSON && strings.TrimSpace(opts.Format) == "json" {
		data, err := featurejson.MarshalIndent(ctx, opts.Runtime.Codecs(), schemaext.Desired, dbsch, "", "  ")
		if err != nil {
			return InspectResult{}, err
		}
		return InspectResult{Rendered: string(data) + "\n", Schema: dbsch}, nil
	}
	report, err := atlasreport.NewSchemaInspectReport(
		ctx,
		dbsch,
		schema,
		info,
		opts.Diagnostics,
		atlasreport.SchemaInspectReportOptions{
			OmitAtlasRefusedBlocks:  opts.OmitAtlasRefusedBlocks,
			DescribeSchemas:         describesSchemas(info, opts),
			CompatibilityHCLFraming: opts.CompatibilityHCLFraming,
			CreationPlan:            creationPlanOf(opts, dbsch, schema, info),
		},
		opts.Runtime,
	)
	if err != nil {
		return InspectResult{}, err
	}
	output, err := atlasreport.RenderSchemaInspect(format, report)
	if err != nil {
		return InspectResult{}, err
	}
	if err := reportOmittedVirtualTables(schema, output, opts); err != nil {
		return InspectResult{}, err
	}
	if err := applyInspectFileExports(output.Files); err != nil {
		return InspectResult{}, err
	}
	return InspectResult{Rendered: output.Text, Schema: dbsch}, nil
}

// creationPlanOf is the creation plan the `sql` helper writes, or nil where
// opts asks for the rendered schema.
func creationPlanOf(
	opts InspectOptions,
	desired *schemamodel.Database,
	read *catalog.Database,
	info catalog.ServerInfo,
) func(context.Context) ([]atlasreport.SchemaChange, error) {
	if opts.SQLCreationPlan == nil {
		return nil
	}
	scope := creationScope{describeSchemas: describesSchemas(info, opts), partial: readIsPartial(opts, info)}
	return inspectCreationPlan(opts.SQLCreationPlan, desired, read, info, scope, opts.Diagnostics)
}

// readIsPartial reports whether the read covers part of the database rather
// than the whole of it: --schema named schemas, --include selected objects, or
// the URL limits the connection to one schema.
//
// A dialect whose schema boundary nobody measured, as YDB served by a
// provider, limits nothing by URL, so a read without --schema there covers the
// whole database. --exclude leaves a read whole: it removes the objects it
// names, an extension installed in an excluded schema among them, and what
// belongs to the database stays in the plan unless a selector names it.
// --include keeps the schemas of the read, so an extension installed in one of
// them is created; a grant on the database, which no schema holds, is not.
func readIsPartial(opts InspectOptions, info catalog.ServerInfo) bool {
	_, urlLimited := schemaselection.URLScope(info.URL)
	return len(schemascope.UniqueNames(opts.Schemas)) > 0 || len(opts.Include) > 0 || urlLimited
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
	schema *catalog.Database,
	output atlasreport.SchemaInspectOutput,
	opts InspectOptions,
) error {
	omitted, err := virtualTablesTheRenderingDropped(schema, output)
	if err != nil || len(omitted) == 0 {
		return err
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
	schema *catalog.Database,
	output atlasreport.SchemaInspectOutput,
) ([]sqlitevirtual.Table, error) {
	virtual, err := sqlitevirtual.Tables(schema)
	if err != nil || len(virtual) == 0 {
		return nil, err
	}

	rendered := []string{output.Text}
	for _, file := range output.Files {
		rendered = append(rendered, file.Data)
	}

	var omitted []sqlitevirtual.Table
	for _, table := range virtual {
		if !sqlitevirtual.DeclarationRendered(rendered, table) {
			omitted = append(omitted, table)
		}
	}
	return omitted, nil
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
	ctx context.Context,
	schema *catalog.Database,
	info catalog.ServerInfo,
	opts InspectOptions,
) (*catalog.Database, atlasfilter.ExcludeReport, error) {
	scope := atlasfilter.Scope{
		Include:               opts.Include,
		Exclude:               opts.Exclude,
		DefaultSchema:         info.Schema,
		RealmRelativePatterns: ConnectionIsRealmScoped(info),
		Dialect:               info.Dialect,
	}
	if opts.Runtime != nil {
		scope.Selections = opts.Runtime.Selections()
	}
	var err error
	scope.Bindings, err = scopeBindings(ctx, opts.Runtime, info.Dialect, scope, databaseSide(schema))
	if err != nil {
		return nil, atlasfilter.ExcludeReport{}, err
	}
	return atlasfilter.ScopeDatabaseReport(schema, scope)
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

// describesSchemas reports whether this run chose the schemas it describes,
// rather than having the connection URL choose for it.
//
// It is the same branch inspectSchemaNames takes -- an explicit `--schema`
// wins, otherwise realm scope decides -- asked a second time because the answer
// is what the SQL format needs: the pinned community binary v1.3.0 renders a
// schema it was told about and stays quiet about the one it merely connected
// to. The measurements are on
// [ptah.run/internal/atlasreport.SchemaInspectReport].
func describesSchemas(info catalog.ServerInfo, opts InspectOptions) bool {
	if len(schemascope.UniqueNames(opts.Schemas)) > 0 {
		return true
	}
	return ConnectionIsRealmScoped(info)
}

// ConnectionIsRealmScoped reports whether a run over this connection describes
// the whole realm rather than the one schema its URL named.
//
// It is the question [ptah.run/internal/atlasfilter.Scope]'s
// RealmRelativePatterns answers, asked from the connection every surface
// already holds. Spelled once, by [atlasfilter.ConnectionNaming], so that
// `schema inspect`, `schema apply`, `schema clean` and `migrate diff` cannot
// end up counting the same exclude pattern different ways.
func ConnectionIsRealmScoped(info catalog.ServerInfo) bool {
	return atlasfilter.ConnectionNaming(info).RealmRelative
}
