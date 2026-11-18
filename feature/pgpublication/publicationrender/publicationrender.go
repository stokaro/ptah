// Package publicationrender validates and renders the publication operation
// payload of package pgpublication for PostgreSQL. It reads captured target
// facts only and never opens a database connection.
//
// Every name is quoted, so a name the server stores in mixed case or with a
// space names the same publication, table, schema and role. A table is
// written with ONLY: the publication holds the tables it names, and no
// inheritance child joins it unnamed. A partitioned table publishes its
// partitions through itself either way.
package publicationrender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/sqlident"
)

// Handlers returns an independent statement handler for the payload, for a
// composition that joins it with other owners' handlers.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&pgpublication.Operation{}, ast.StatementExtension, validateOperation, renderOperation),
	}
}

// Registry returns the statement handler for the payload. Each call returns
// an independent registry.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

func validateOperation(ctx renderer.ExtensionContext, value *pgpublication.Operation) error {
	if platform.NormalizeDialect(ctx.Target) != platform.Postgres {
		return renderer.UnsupportedExtension(ctx.Target, pgpublication.OperationKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return RefuseUnsupported(ctx.Target, ctx.Capabilities, value)
}

// RefuseUnsupported returns the refusal of an operation that writes a clause
// the target lacks: TABLES IN SCHEMA without [capability.PublicationSchemas],
// and publish_generated_columns without
// [capability.PublicationGeneratedColumns]. It returns nil for any other
// operation. A server without the setting publishes no generated column, so
// leaving the clause out would not be the publication the operation names.
func RefuseUnsupported(target string, caps capability.Capabilities, value *pgpublication.Operation) error {
	normalized := platform.NormalizeDialect(target)
	switch {
	case len(value.Schemas) != 0 && !caps.Has(capability.PublicationSchemas):
		return &ptaherr.CapabilityError{Dialect: normalized, Feature: "TABLES IN SCHEMA", Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("publication %q publishes every table of schema %q, which requires target capability %s, unavailable on this %s target",
				value.Name, value.Schemas[0], capability.PublicationSchemas, normalized)}
	case value.GeneratedColumns != "" && !caps.Has(capability.PublicationGeneratedColumns):
		return &ptaherr.CapabilityError{Dialect: normalized, Feature: "publish_generated_columns", Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("publication %q declares publish_generated_columns = %s, which requires target capability %s, unavailable on this %s target",
				value.Name, value.GeneratedColumns, capability.PublicationGeneratedColumns, normalized)}
	default:
		return nil
	}
}

// renderOperation writes one statement about one publication. Each spelling is
// PostgreSQL's own, the one pg_dump writes for the same object.
func renderOperation(_ renderer.ExtensionContext, value *pgpublication.Operation) ([]string, error) {
	name := quote(value.Name)
	switch value.Action {
	case pgpublication.Drop:
		if value.IfExists {
			return []string{"DROP PUBLICATION IF EXISTS " + name + ";"}, nil
		}
		return []string{"DROP PUBLICATION " + name + ";"}, nil
	case pgpublication.AddMembers:
		return []string{"ALTER PUBLICATION " + name + " ADD " + members(value) + ";"}, nil
	case pgpublication.DropMembers:
		return []string{"ALTER PUBLICATION " + name + " DROP " + members(value) + ";"}, nil
	case pgpublication.SetMembers:
		return []string{"ALTER PUBLICATION " + name + " SET " + members(value) + ";"}, nil
	case pgpublication.SetOptions:
		return []string{"ALTER PUBLICATION " + name + " SET (" + strings.Join(setOptions(value), ", ") + ");"}, nil
	case pgpublication.SetOwner:
		return []string{"ALTER PUBLICATION " + name + " OWNER TO " + quote(value.Owner) + ";"}, nil
	default:
		return []string{CreateStatement(value)}, nil
	}
}

// CreateStatement writes CREATE PUBLICATION for a creation: FOR ALL TABLES,
// the tables and schemas it publishes, or no FOR clause for a publication
// that publishes nothing yet, then the options it names.
func CreateStatement(value *pgpublication.Operation) string {
	statement := "CREATE PUBLICATION " + quote(value.Name)
	switch {
	case value.AllTables:
		statement += " FOR ALL TABLES"
	case len(value.Tables) != 0 || len(value.Schemas) != 0:
		statement += " FOR " + members(value)
	}
	if written := createOptions(value); len(written) != 0 {
		statement += " WITH (" + strings.Join(written, ", ") + ")"
	}
	return statement + ";"
}

// members writes the tables and the schemas of an operation as one list:
// `TABLE ONLY a, ONLY b, TABLES IN SCHEMA s`.
func members(value *pgpublication.Operation) string {
	var parts []string
	for index, member := range value.Tables {
		table := "ONLY " + sqlident.Qualified(platform.Postgres, member.Schema, member.Table)
		if index == 0 {
			table = "TABLE " + table
		}
		parts = append(parts, table)
	}
	for index, schema := range value.Schemas {
		written := quote(schema)
		if index == 0 {
			written = "TABLES IN SCHEMA " + written
		}
		parts = append(parts, written)
	}
	return strings.Join(parts, ", ")
}

// createOptions are the options a creation writes: only the ones it names, so
// the server applies its defaults to the rest.
func createOptions(value *pgpublication.Operation) []string {
	written := publishOption(value)
	if value.ViaPartitionRoot {
		written = append(written, viaPartitionRootOption(value))
	}
	return append(written, generatedColumnsOption(value)...)
}

// setOptions are the options a change writes: the publish set and
// publish_via_partition_root always, and publish_generated_columns where the
// operation names it.
func setOptions(value *pgpublication.Operation) []string {
	written := append(publishOption(value), viaPartitionRootOption(value))
	return append(written, generatedColumnsOption(value)...)
}

func publishOption(value *pgpublication.Operation) []string {
	if value.Publish == nil {
		return nil
	}
	return []string{"publish = " + sqlident.StringLiteral(strings.Join(value.Publish, ", "))}
}

func viaPartitionRootOption(value *pgpublication.Operation) string {
	return fmt.Sprintf("publish_via_partition_root = %t", value.ViaPartitionRoot)
}

func generatedColumnsOption(value *pgpublication.Operation) []string {
	if value.GeneratedColumns == "" {
		return nil
	}
	return []string{"publish_generated_columns = " + value.GeneratedColumns}
}

func quote(name string) string { return sqlident.Quote(platform.Postgres, name) }
