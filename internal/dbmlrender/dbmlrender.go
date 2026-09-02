// Package dbmlrender writes a Ptah schema as DBML.
//
// It is a format adapter and nothing else: it reads [schemamodel.Database] and
// produces text. It plans nothing, converts nothing to SQL, and is not a second
// place where schema semantics are decided (stokaro/ptah#2065).
//
// # What canonical means here
//
// The same schema renders to the same bytes: LF endings, one trailing newline,
// and an order that comes from the schema rather than from a map. Enums and
// tables are sorted by their identity; columns keep the order they were
// declared in, because that order is part of what the schema says and
// re-sorting it would be the renderer editing the schema. Indexes and
// references are sorted by name, since neither carries a meaningful order.
//
// # What DBML cannot say
//
// DBML describes tables, columns, enums, indexes and references. A Ptah schema
// can hold views, functions, triggers, sequences, domains, policies and more,
// and none of them has a DBML spelling. Those are not dropped quietly:
// [Result.Omitted] names every family that had members and no representation,
// so a caller can report the loss rather than discover it later
// (stokaro/ptah#2065 asks for exactly that, and a format that reported nothing
// would make a DBML export look like a complete description of the database).
package dbmlrender

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
)

// Options selects what is rendered.
type Options struct {
	// IncludeTables and ExcludeTables narrow the table set by name. Empty
	// includes everything.
	IncludeTables []string
	ExcludeTables []string
}

// Result is the rendered document and what it could not carry.
type Result struct {
	// DBML is the document: LF endings, one trailing newline, empty when the
	// schema has nothing DBML can express.
	DBML string
	// Omitted names each object family that had members and no DBML spelling,
	// sorted, as "views (2)".
	Omitted []string
}

// Render writes the schema as DBML.
func Render(db *schemamodel.Database, opts Options) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("schema database is nil")
	}
	b := &builder{db: db, opts: opts}
	if metadata := b.selectedExportMetadata(); len(metadata) > 0 {
		return Result{}, exportMetadataError(metadata)
	}
	return Result{DBML: b.render(), Omitted: omittedFamilies(db)}, nil
}

func exportMetadataError(metadata []schemamodel.ExportMetadata) error {
	declarations := make([]string, 0, len(metadata))
	for _, item := range metadata {
		declarations = append(declarations, fmt.Sprintf(
			"%s %q %s=%q",
			item.Kind,
			item.Name,
			item.Attribute,
			item.Value,
		))
	}
	return fmt.Errorf(
		"DBML cannot represent API export metadata without loss: %s; use YAML, HCL, or Go annotations",
		strings.Join(declarations, "; "),
	)
}

type builder struct {
	db   *schemamodel.Database
	opts Options
}

func (b *builder) selectedExportMetadata() []schemamodel.ExportMetadata {
	selected := &schemamodel.Database{Tables: b.selected()}
	for _, table := range selected.Tables {
		selected.Fields = append(selected.Fields, b.fieldsOf(table)...)
	}
	return schemamodel.ExportMetadataIn(selected)
}

func (b *builder) render() string {
	blocks := make([]string, 0, 8)
	blocks = append(blocks, b.enums()...)
	blocks = append(blocks, b.tables()...)
	blocks = append(blocks, b.references()...)
	if len(blocks) == 0 {
		return ""
	}
	return strings.Join(blocks, "\n\n") + "\n"
}
