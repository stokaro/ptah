package atlasschema

import (
	"errors"
	"fmt"
	"io"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasfilter"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
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

// Inspect reads a live schema and renders it with Atlas-compatible formatting.
func Inspect(conn *dbschema.DatabaseConnection, opts InspectOptions) (string, error) {
	format, err := NormalizeInspectFormat(opts.Format)
	if err != nil {
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
	schema, err = atlasfilter.ExcludeDatabase(schema, opts.Exclude)
	if err != nil {
		return "", err
	}
	dbsch := dbschematogo.ConvertDBSchemaToGoSchema(schema)
	rendered, err := atlasreport.RenderSchemaInspectFormat(format, atlasreport.NewSchemaInspectReport(
		dbsch,
		schema,
		conn.Info(),
		opts.Diagnostics,
	))
	if err != nil {
		return "", err
	}
	return rendered, nil
}

// SplitSchemaNames expands repeated and comma-separated Atlas schema filters.
func SplitSchemaNames(values []string) []string {
	return schemascope.SplitNames(values)
}
