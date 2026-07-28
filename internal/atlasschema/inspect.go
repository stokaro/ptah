package atlasschema

import (
	"errors"
	"fmt"
	"io"

	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasfilter"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
	"github.com/stokaro/ptah/internal/fileplan"
	"github.com/stokaro/ptah/internal/schemascope"
)

// InspectOptions configures Atlas-compatible schema inspection.
type InspectOptions struct {
	DevURL      string
	Schemas     []string
	Exclude     []string
	Format      string
	Diagnostics io.Writer
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
	schema, err = atlasfilter.ExcludeDatabase(schema, opts.Exclude)
	if err != nil {
		return "", err
	}
	dbsch := dbschematogo.ConvertDBSchemaToGoSchema(schema)
	output, err := atlasreport.RenderSchemaInspect(format, atlasreport.NewSchemaInspectReport(
		dbsch,
		schema,
		info,
		opts.Diagnostics,
	))
	if err != nil {
		return "", err
	}
	if err := applyInspectFileExports(output.Files); err != nil {
		return "", err
	}
	return output.Text, nil
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
