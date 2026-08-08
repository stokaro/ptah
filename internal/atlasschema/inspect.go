package atlasschema

import (
	"errors"
	"fmt"
	"io"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/fileplan"
	"go.5x5.cz/ptah/internal/rolescope"
	"go.5x5.cz/ptah/internal/schemascope"
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
func Inspect(conn *dbschema.DatabaseConnection, opts InspectOptions) (string, error) {
	if _, err := NormalizeInspectFormat(opts.Format); err != nil {
		return "", err
	}
	if conn == nil {
		return "", errors.New("schema inspect requires database connection")
	}
	if err := atlasurl.ValidateDialectMatch(opts.DevURL, conn.Info().Dialect); err != nil {
		return "", err
	}

	schema, err := dbschema.ReadSchemaWithSchemas(conn, SplitSchemaNames(opts.Schemas))
	if err != nil {
		return "", fmt.Errorf("read database schema: %w", err)
	}
	// A description scoped to the roles the inspected schemas use omits roles
	// that exist on the server, and the rendered document is the only thing
	// the operator sees. Reported here rather than in renderInspectSchema
	// because only this path inspects a database the operator named: the
	// file and migration-directory sources render a dev database they were
	// told to materialize, whose other cluster roles are not an answer to
	// anything they asked. See stokaro/ptah#1267.
	rolescope.ReportUndescribed(opts.Diagnostics, schema)
	return renderInspectSchema(schema, conn.Info(), opts)
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
	schema, err = scopeInspectSchema(schema, info, opts)
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
	dbsch := dbschematogo.ConvertDBSchemaToGoSchema(schema)
	output, err := atlasreport.RenderSchemaInspect(format, atlasreport.NewSchemaInspectReport(
		dbsch,
		schema,
		info,
		opts.Diagnostics,
		opts.OmitAtlasRefusedBlocks,
	))
	if err != nil {
		return "", err
	}
	if err := applyInspectFileExports(output.Files); err != nil {
		return "", err
	}
	return output.Text, nil
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
) (*dbschematypes.DBSchema, error) {
	return atlasfilter.ScopeDatabase(schema, atlasfilter.Scope{
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
