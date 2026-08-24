package dbmlparse

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// indexes reads the `Indexes { ... }` block inside a table.
func (p *parser) indexes(structName string) error {
	if err := p.advance(); err != nil {
		return err
	}
	if err := p.expectPunct("{"); err != nil {
		return err
	}
	for !p.isPunct("}") {
		if p.tok.kind == tokenEOF {
			return p.errorf("unterminated Indexes block")
		}
		index, err := p.index(structName)
		if err != nil {
			return err
		}
		p.db.Indexes = append(p.db.Indexes, index)
	}
	return p.advance()
}

// index reads one entry: a single column, or a parenthesized list, plus
// settings.
func (p *parser) index(structName string) (goschema.Index, error) {
	index := goschema.Index{StructName: structName}
	switch {
	case p.isPunct("("):
		if err := p.advance(); err != nil {
			return index, err
		}
		for !p.isPunct(")") {
			if p.tok.kind == tokenEOF {
				return index, p.errorf("unterminated index column list")
			}
			column, err := p.name()
			if err != nil {
				return index, err
			}
			index.Fields = append(index.Fields, column)
			if p.isPunct(",") {
				if err := p.advance(); err != nil {
					return index, err
				}
			}
		}
		if err := p.advance(); err != nil {
			return index, err
		}
	default:
		column, err := p.name()
		if err != nil {
			return index, err
		}
		index.Fields = []string{column}
	}

	if !p.isPunct("[") {
		return index, nil
	}
	settings, err := p.settings()
	if err != nil {
		return index, err
	}
	if err := applyIndexSettings(&index, settings); err != nil {
		return index, p.wrapAt(err)
	}
	return index, nil
}

// applyIndexSettings maps an index's bracketed list.
func applyIndexSettings(index *goschema.Index, settings []setting) error {
	for _, entry := range settings {
		switch entry.key {
		case "unique":
			index.Unique = true
		case "pk", "primary key":
			// A primary-key index is the table's primary key rather than an
			// index of its own, and the columns already carry `pk`. Recording
			// it twice would make the table declare two.
			return fmt.Errorf("declare a primary key on its columns rather than in Indexes")
		case "name":
			index.Name = entry.value
		case "type":
			index.Type = strings.ToUpper(entry.value)
		case "note":
			index.Comment = entry.value
		default:
			return fmt.Errorf("unsupported index setting %q", entry.key)
		}
	}
	return nil
}

// ref reads a top-level `Ref name: a.b > c.d [settings]`.
func (p *parser) ref() error {
	if err := p.advance(); err != nil {
		return err
	}
	name := ""
	if p.tok.kind == tokenWord || p.tok.kind == tokenQuoted {
		read, err := p.name()
		if err != nil {
			return err
		}
		name = read
	}
	// `Ref: a.b > c.d` names nothing; `Ref x: a.b > c.d` names the constraint.
	if p.isPunct(":") {
		if err := p.advance(); err != nil {
			return err
		}
	}
	if p.isPunct("{") {
		return p.refBlock(name)
	}
	return p.refBody(name)
}

// refBlock reads the braced form, which holds one relationship per line.
func (p *parser) refBlock(name string) error {
	if err := p.advance(); err != nil {
		return err
	}
	for !p.isPunct("}") {
		if p.tok.kind == tokenEOF {
			return p.errorf("unterminated Ref block")
		}
		if err := p.refBody(name); err != nil {
			return err
		}
	}
	return p.advance()
}

// refBody reads one `a.b > c.d [settings]` relationship.
func (p *parser) refBody(name string) error {
	leftTable, leftColumn, err := p.refEndpoint()
	if err != nil {
		return err
	}
	operator, err := p.refOperator()
	if err != nil {
		return err
	}
	rightTable, rightColumn, err := p.refEndpoint()
	if err != nil {
		return err
	}

	settings := []setting(nil)
	if p.isPunct("[") {
		settings, err = p.settings()
		if err != nil {
			return err
		}
	}

	// The many-to-many operator has no foreign key behind it: a database
	// expresses it with a join table, and inventing one here would put a table
	// in the schema that the document never declared.
	if operator == "<>" {
		return p.errorf(
			"a many-to-many relationship has no foreign key; declare the join table and two references to it")
	}

	// `<` points from the one side to the many side, so the foreign key lives
	// on the right-hand table. `>` and `-` put it on the left.
	if operator == "<" {
		leftTable, rightTable = rightTable, leftTable
		leftColumn, rightColumn = rightColumn, leftColumn
	}
	return p.recordReference(name, leftTable, leftColumn, rightTable, rightColumn, settings)
}

// refEndpoint reads `table.column` or `schema.table.column`.
func (p *parser) refEndpoint() (table, column string, err error) {
	parts := make([]string, 0, 3)
	for {
		part, err := p.name()
		if err != nil {
			return "", "", err
		}
		parts = append(parts, part)
		if !p.isPunct(".") {
			break
		}
		if err := p.advance(); err != nil {
			return "", "", err
		}
	}
	if len(parts) < 2 {
		return "", "", p.errorf("a reference endpoint needs a table and a column")
	}
	column = parts[len(parts)-1]
	table = strings.Join(parts[:len(parts)-1], ".")
	return table, column, nil
}

// refOperator reads the relationship operator.
func (p *parser) refOperator() (string, error) {
	if p.tok.kind != tokenPunct {
		return "", p.errorf("expected a relationship operator, found %s", p.tok.describe())
	}
	operator := p.tok.text
	if err := p.advance(); err != nil {
		return "", err
	}
	// `<>` arrives as two tokens, since the lexer reads punctuation one rune at
	// a time.
	if operator == "<" && p.isPunct(">") {
		if err := p.advance(); err != nil {
			return "", err
		}
		return "<>", nil
	}
	switch operator {
	case ">", "<", "-":
		return operator, nil
	default:
		return "", p.errorf("unknown relationship operator %q", operator)
	}
}

// recordReference attaches the foreign key to the column that carries it.
func (p *parser) recordReference(
	name, fromTable, fromColumn, toTable, toColumn string,
	settings []setting,
) error {
	structName := fromTable
	found := false
	for i := range p.db.Fields {
		field := &p.db.Fields[i]
		if field.StructName != structName || field.Name != fromColumn {
			continue
		}
		field.Foreign = toTable + "(" + toColumn + ")"
		field.ForeignKeyName = name
		if err := applyRefSettings(field, settings); err != nil {
			return p.wrapAt(err)
		}
		found = true
		break
	}
	if !found {
		return p.errorf("reference names %s.%s, which no table declares", fromTable, fromColumn)
	}
	return nil
}

// applyRefSettings maps a relationship's bracketed list.
func applyRefSettings(field *goschema.Field, settings []setting) error {
	for _, entry := range settings {
		switch entry.key {
		case "delete":
			field.OnDelete = strings.ToUpper(entry.value)
		case "update":
			field.OnUpdate = strings.ToUpper(entry.value)
		case "name":
			field.ForeignKeyName = entry.value
		case "note":
			// A relationship's note describes the diagram edge; the column
			// keeps its own.
		default:
			return fmt.Errorf("unsupported reference setting %q", entry.key)
		}
	}
	return nil
}
