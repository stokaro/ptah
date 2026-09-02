package dbmlparse

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
)

// enum reads `Enum name { value ... }`.
func (p *parser) enum() error {
	if err := p.advance(); err != nil {
		return err
	}
	schema, name, err := p.qualifiedName()
	if err != nil {
		return err
	}
	if err := p.expectPunct("{"); err != nil {
		return err
	}
	enum := schemamodel.Enum{Name: name, Schema: schema}
	for !p.isPunct("}") {
		if p.tok.kind == tokenEOF {
			return p.errorf("unterminated enum %q", name)
		}
		value, err := p.name()
		if err != nil {
			return err
		}
		// A value may carry a note, which describes it rather than defines it.
		if p.isPunct("[") {
			settings, err := p.settings()
			if err != nil {
				return err
			}
			for _, entry := range settings {
				if err := rejectExportMetadataSetting("enum value", entry); err != nil {
					return p.wrapAt(err)
				}
			}
		}
		enum.Values = append(enum.Values, value)
	}
	p.db.Enums = append(p.db.Enums, enum)
	return p.advance()
}

// table reads `Table name { column ... Indexes { ... } Note: '...' }`.
func (p *parser) table() error {
	if err := p.advance(); err != nil {
		return err
	}
	schema, name, err := p.qualifiedName()
	if err != nil {
		return err
	}
	// `as alias` names the table for the diagram. It is read so the document
	// parses, and it names nothing in the database.
	if p.isWord("as") {
		if err := p.advance(); err != nil {
			return err
		}
		if _, err := p.name(); err != nil {
			return err
		}
	}
	if p.isPunct("[") {
		settings, err := p.settings()
		if err != nil {
			return err
		}
		for _, entry := range settings {
			if err := rejectExportMetadataSetting("table", entry); err != nil {
				return p.wrapAt(err)
			}
		}
	}
	if err := p.expectPunct("{"); err != nil {
		return err
	}

	structName := structNameFor(schema, name)
	table := schemamodel.Table{StructName: structName, Name: name, Schema: schema}
	p.tableStruct[structName] = name

	for !p.isPunct("}") {
		if p.tok.kind == tokenEOF {
			return p.errorf("unterminated table %q", name)
		}
		switch {
		case p.isWord("indexes"):
			if err := p.indexes(structName); err != nil {
				return err
			}
		case p.isWord("note"):
			note, err := p.noteValue()
			if err != nil {
				return err
			}
			table.Comment = note
		default:
			if err := p.column(structName, schema); err != nil {
				return err
			}
		}
	}
	p.db.Tables = append(p.db.Tables, table)
	return p.advance()
}

// noteValue reads `Note: '...'` or `Note { '...' }`.
func (p *parser) noteValue() (string, error) {
	if err := p.advance(); err != nil {
		return "", err
	}
	switch {
	case p.isPunct(":"):
		if err := p.advance(); err != nil {
			return "", err
		}
	case p.isPunct("{"):
		if err := p.advance(); err != nil {
			return "", err
		}
		value, err := p.stringValue()
		if err != nil {
			return "", err
		}
		return value, p.expectPunct("}")
	default:
		return "", p.errorf("expected %q or %q after Note, found %s", ":", "{", p.tok.describe())
	}
	return p.stringValue()
}

// stringValue reads one string literal.
func (p *parser) stringValue() (string, error) {
	if p.tok.kind != tokenString {
		return "", p.errorf("expected a quoted string, found %s", p.tok.describe())
	}
	value := p.tok.text
	return value, p.advance()
}

// column reads one column line: a name, a type, and optional settings.
func (p *parser) column(structName, schema string) error {
	name, err := p.name()
	if err != nil {
		return err
	}
	columnType, err := p.columnType()
	if err != nil {
		return err
	}
	field := schemamodel.Field{
		StructName: structName,
		FieldName:  name,
		Name:       name,
		Type:       columnType,
		// DBML columns are nullable unless the document says otherwise, which
		// is the opposite of Ptah's zero value, so it is set here rather than
		// left to the struct.
		Nullable: true,
	}
	if p.isPunct("[") {
		settings, err := p.settings()
		if err != nil {
			return err
		}
		if err := applyColumnSettings(&field, settings, schema); err != nil {
			return p.wrapAt(err)
		}
	}
	p.db.Fields = append(p.db.Fields, field)
	return nil
}

// wrapAt attaches the current position to an error raised while interpreting a
// construct that has already been read.
func (p *parser) wrapAt(err error) error {
	return fmt.Errorf("%s: %w", p.tok.position(p.file), err)
}

// columnType reads a type name, with its optional parenthesized arguments and
// array suffix kept verbatim.
func (p *parser) columnType() (string, error) {
	base, err := p.name()
	if err != nil {
		return "", err
	}
	var out strings.Builder
	out.WriteString(base)
	if p.isPunct("(") {
		out.WriteString("(")
		if err := p.advance(); err != nil {
			return "", err
		}
		for !p.isPunct(")") {
			if p.tok.kind == tokenEOF {
				return "", p.errorf("unterminated type arguments for %q", base)
			}
			out.WriteString(p.tok.text)
			if err := p.advance(); err != nil {
				return "", err
			}
		}
		out.WriteString(")")
		if err := p.advance(); err != nil {
			return "", err
		}
	}
	for p.isPunct("[") && p.peekIsArraySuffix() {
		out.WriteString("[]")
		if err := p.advance(); err != nil {
			return "", err
		}
		if err := p.expectPunct("]"); err != nil {
			return "", err
		}
	}
	return out.String(), nil
}

// peekIsArraySuffix distinguishes `type[]` from `type [settings]` by looking at
// what follows the bracket.
func (p *parser) peekIsArraySuffix() bool {
	saved := *p.lex
	next, err := p.lex.next()
	*p.lex = saved
	return err == nil && next.kind == tokenPunct && next.text == "]"
}

// structNameFor derives the identity a table is recorded under. Ptah keys
// fields and indexes by struct name, and a DBML document has no structs, so the
// qualified table name stands in for one.
func structNameFor(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}
