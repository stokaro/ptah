// Package schemadoc renders a declared schema as one self-contained HTML
// document.
//
// Self-contained is the constraint everything else follows from: no stylesheet,
// font, script or image is fetched when the page is opened. That makes the
// document shareable by copying, attachable to a review, readable on a machine
// with no network, and it means looking at a schema never sends the schema
// anywhere (stokaro/ptah#1862).
//
// The diagram is drawn here rather than delegated for the same reason. Mermaid
// would be a megabyte of JavaScript inside the binary, and Graphviz would be an
// external program a document quietly depends on -- a document that is only
// complete when a tool happens to be installed is not one.
package schemadoc

import (
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaexport"
	"go.5x5.cz/ptah/internal/tableref"
)

// document is everything the page renders, resolved once so the templates and
// the diagram read the same schema rather than each walking it their own way.
type document struct {
	Title     string
	Source    string
	Tables    []tableDoc
	Enums     []enumDoc
	Relations []relation
}

// tableDoc is one table as the page presents it.
type tableDoc struct {
	Name    string
	Comment string
	Columns []columnDoc
	Indexes []indexDoc
}

// columnDoc is one column, with the facts a reader looks for first.
type columnDoc struct {
	Name     string
	Type     string
	Nullable bool
	Key      string
	Default  string
	Foreign  string
	Comment  string
}

type indexDoc struct {
	Name    string
	Columns string
	Unique  bool
}

type enumDoc struct {
	Name   string
	Values []string
}

// relation is one foreign key, table to table. The diagram draws these; the
// column list names them per column.
type relation struct {
	From string
	To   string
}

// build resolves the document model from a declared schema.
func build(db *schemamodel.Database, opts Options) document {
	selected := schemaexport.SelectTables(db, schemaexport.Options{
		IncludeTables: opts.IncludeTables,
		ExcludeTables: opts.ExcludeTables,
	})
	included := make(map[string]bool, len(selected))
	byStruct := make(map[string]schemamodel.Table, len(selected))
	for _, table := range selected {
		included[table.Name] = true
		byStruct[table.StructName] = table
	}

	fields := schemamodel.ProcessEmbeddedFields(db.EmbeddedFields, db.Fields)
	columns := make(map[string][]columnDoc, len(selected))
	var relations []relation
	for _, field := range fields {
		table, known := byStruct[field.StructName]
		if !known {
			continue
		}
		columns[table.Name] = append(columns[table.Name], columnOf(field))
		if target, ok := foreignTarget(field.Foreign); ok && included[target] {
			relations = append(relations, relation{From: table.Name, To: target})
		}
	}

	doc := document{Title: opts.Title, Source: opts.Source, Relations: dedupeRelations(relations)}
	for _, table := range selected {
		doc.Tables = append(doc.Tables, tableDoc{
			Name:    table.Name,
			Comment: table.Comment,
			Columns: columns[table.Name],
			Indexes: indexesOf(db, table),
		})
	}
	sort.Slice(doc.Tables, func(i, j int) bool { return doc.Tables[i].Name < doc.Tables[j].Name })
	doc.Enums = enumsOf(db)
	return doc
}

func columnOf(field schemamodel.Field) columnDoc {
	return columnDoc{
		Name:     field.Name,
		Type:     field.Type,
		Nullable: field.Nullable,
		Key:      keyOf(field),
		Default:  defaultOf(field),
		Foreign:  field.Foreign,
		Comment:  field.Comment,
	}
}

// keyOf names the strongest key role a column carries, because a column that is
// both primary and unique is described by the first alone.
func keyOf(field schemamodel.Field) string {
	switch {
	case field.Primary:
		return "primary"
	case field.Unique:
		return "unique"
	default:
		return ""
	}
}

// defaultOf reads a default that was set to an empty string as a default,
// because DefaultSet is what separates "defaults to empty" from "has none".
func defaultOf(field schemamodel.Field) string {
	if field.DefaultExpr != "" {
		return field.DefaultExpr
	}
	if field.Default != "" || field.DefaultSet {
		return field.Default
	}
	return ""
}

func indexesOf(db *schemamodel.Database, table schemamodel.Table) []indexDoc {
	var indexes []indexDoc
	for _, index := range db.Indexes {
		if index.StructName != table.StructName {
			continue
		}
		indexes = append(indexes, indexDoc{
			Name:    index.Name,
			Columns: strings.Join(index.Fields, ", "),
			Unique:  index.Unique,
		})
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	return indexes
}

func enumsOf(db *schemamodel.Database) []enumDoc {
	var enums []enumDoc
	for _, enum := range db.Enums {
		enums = append(enums, enumDoc{Name: enum.Name, Values: enum.Values})
	}
	sort.Slice(enums, func(i, j int) bool { return enums[i].Name < enums[j].Name })
	return enums
}

// foreignTarget reads the table a foreign key points at, from the "table(col)"
// spelling the annotations use.
func foreignTarget(foreign string) (string, bool) {
	if foreign == "" {
		return "", false
	}
	table, _, qualified := strings.Cut(foreign, "(")
	if !qualified {
		return strings.TrimSpace(foreign), true
	}
	ref, ok := tableref.Parse(strings.TrimSpace(table))
	if !ok {
		return "", false
	}
	return ref.Name, true
}

// dedupeRelations collapses several foreign keys between the same two tables
// into one edge, because the diagram draws a dependency rather than a count.
func dedupeRelations(relations []relation) []relation {
	seen := make(map[relation]bool, len(relations))
	var unique []relation
	for _, r := range relations {
		if seen[r] {
			continue
		}
		seen[r] = true
		unique = append(unique, r)
	}
	sort.Slice(unique, func(i, j int) bool {
		if unique[i].From != unique[j].From {
			return unique[i].From < unique[j].From
		}
		return unique[i].To < unique[j].To
	})
	return unique
}
