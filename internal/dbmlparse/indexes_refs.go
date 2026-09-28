package dbmlparse

import (
	"fmt"
	"strings"

	"ptah.run/core/schemamodel"
)

// indexes reads the `Indexes { ... }` block inside a table.
//
// An entry with `pk` is the table's primary key rather than an index: DBML
// writes a composite key only this way. A table gets one.
func (p *parser) indexes(table *schemamodel.Table) error {
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
		index, primary, err := p.index(table.StructName)
		if err != nil {
			return err
		}
		if !primary {
			p.db.Indexes = append(p.db.Indexes, index)
			continue
		}
		if len(table.PrimaryKey) > 0 {
			return p.errorf("table %q declares a second primary key in Indexes", table.Name)
		}
		table.PrimaryKey = index.Fields
		table.PrimaryKeyName = index.Name
	}
	return p.advance()
}

// index reads one entry: a single column, or a parenthesized list, plus
// settings. primary reports an entry marked `pk`, whose columns and name are
// the table's primary key.
func (p *parser) index(structName string) (index schemamodel.Index, primary bool, err error) {
	index = schemamodel.Index{StructName: structName}
	switch {
	case p.isPunct("("):
		if err := p.advance(); err != nil {
			return index, false, err
		}
		columns, err := p.columnListBody()
		if err != nil {
			return index, false, err
		}
		index.Fields = columns
	default:
		column, err := p.name()
		if err != nil {
			return index, false, err
		}
		index.Fields = []string{column}
	}

	if !p.isPunct("[") {
		return index, false, nil
	}
	settings, err := p.settings()
	if err != nil {
		return index, false, err
	}
	primary, err = applyIndexSettings(&index, settings)
	if err != nil {
		return index, false, p.wrapAt(err)
	}
	return index, primary, nil
}

// columnListBody reads the columns of a parenthesized list whose opening
// parenthesis was just consumed, through the closing one.
func (p *parser) columnListBody() ([]string, error) {
	columns := make([]string, 0, 2)
	for !p.isPunct(")") {
		if p.tok.kind == tokenEOF {
			return nil, p.errorf("unterminated column list")
		}
		column, err := p.name()
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
		if p.isPunct(",") {
			if err := p.advance(); err != nil {
				return nil, err
			}
		}
	}
	if len(columns) == 0 {
		return nil, p.errorf("a column list names no column")
	}
	return columns, p.advance()
}

// applyIndexSettings maps an index's bracketed list, and reports an entry
// marked `pk`.
//
// A primary-key entry takes a name and nothing else: `unique` says what `pk`
// already does, and `type` and `note` describe an index the key is not.
func applyIndexSettings(index *schemamodel.Index, settings []setting) (primary bool, err error) {
	for _, entry := range settings {
		switch entry.key {
		case "unique":
			index.Unique = true
		case "pk", "primary key":
			primary = true
		case "name":
			index.Name = entry.value
		case "type":
			index.Type = strings.ToUpper(entry.value)
		case "note":
			index.Comment = entry.value
		default:
			return false, fmt.Errorf("unsupported index setting %q", entry.key)
		}
	}
	if primary && (index.Unique || index.Type != "" || index.Comment != "") {
		return false, fmt.Errorf("a primary key entry in Indexes takes only a name")
	}
	return primary, nil
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

// refBody reads one `a.b > c.d [settings]` relationship. Either side may name
// several columns, as `a.(x, y)`, for a composite key.
func (p *parser) refBody(name string) error {
	leftTable, leftColumns, err := p.refEndpoint()
	if err != nil {
		return err
	}
	operator, err := p.refOperator()
	if err != nil {
		return err
	}
	rightTable, rightColumns, err := p.refEndpoint()
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
	if len(leftColumns) != len(rightColumns) {
		return p.errorf("a reference pairs %d columns with %d", len(leftColumns), len(rightColumns))
	}

	// `<` points from the one side to the many side, so the foreign key lives
	// on the right-hand table. `>` and `-` put it on the left.
	if operator == "<" {
		leftTable, rightTable = rightTable, leftTable
		leftColumns, rightColumns = rightColumns, leftColumns
	}
	return p.recordReference(name, leftTable, leftColumns, rightTable, rightColumns, settings)
}

// refEndpoint reads `table.column`, `schema.table.column`, or either with a
// parenthesized column list in place of the column.
func (p *parser) refEndpoint() (table string, columns []string, err error) {
	parts := make([]string, 0, 3)
	for {
		part, err := p.name()
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, part)
		if !p.isPunct(".") {
			break
		}
		if err := p.advance(); err != nil {
			return "", nil, err
		}
		if p.isPunct("(") {
			if err := p.advance(); err != nil {
				return "", nil, err
			}
			columns, err := p.columnListBody()
			if err != nil {
				return "", nil, err
			}
			return strings.Join(parts, "."), columns, nil
		}
	}
	if len(parts) < 2 {
		return "", nil, p.errorf("a reference endpoint needs a table and a column")
	}
	return strings.Join(parts[:len(parts)-1], "."), []string{parts[len(parts)-1]}, nil
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

// recordReference records one foreign key.
//
// A key over one column is carried by that column, which is how every other
// source declares one. A composite key, and a second key over a column that
// already carries one, is a FOREIGN KEY constraint of the table: a column holds
// one key, and writing the second over it would drop the first.
func (p *parser) recordReference(
	name, fromTable string, fromColumns []string, toTable string, toColumns []string,
	settings []setting,
) error {
	for _, column := range fromColumns {
		if p.field(fromTable, column) == nil {
			return p.errorf("reference names %s.%s, which no table declares", fromTable, column)
		}
	}
	if len(fromColumns) == 1 {
		field := p.field(fromTable, fromColumns[0])
		if field.Foreign == "" {
			field.Foreign = toTable + "(" + toColumns[0] + ")"
			field.ForeignKeyName = name
			if err := applyRefSettings(field, settings); err != nil {
				return p.wrapAt(err)
			}
			return nil
		}
	}
	key := schemamodel.Field{ForeignKeyName: name}
	if err := applyRefSettings(&key, settings); err != nil {
		return p.wrapAt(err)
	}
	p.db.Constraints = append(p.db.Constraints, schemamodel.Constraint{
		StructName:     fromTable,
		Name:           key.ForeignKeyName,
		Type:           "FOREIGN KEY",
		Table:          fromTable,
		Columns:        fromColumns,
		ForeignTable:   toTable,
		ForeignColumn:  toColumns[0],
		ForeignColumns: toColumns,
		OnDelete:       key.OnDelete,
		OnUpdate:       key.OnUpdate,
	})
	return nil
}

// field is the declared column of a table, or nil.
func (p *parser) field(structName, column string) *schemamodel.Field {
	for i := range p.db.Fields {
		if p.db.Fields[i].StructName == structName && p.db.Fields[i].Name == column {
			return &p.db.Fields[i]
		}
	}
	return nil
}

// applyRefSettings maps a relationship's bracketed list.
func applyRefSettings(field *schemamodel.Field, settings []setting) error {
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
