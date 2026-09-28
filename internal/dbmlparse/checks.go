package dbmlparse

import (
	"fmt"

	"ptah.run/core/schemamodel"
)

// checks reads the `Checks { ... }` block inside a table: one backtick
// expression per entry, optionally named.
//
// A named entry is a CHECK constraint, which keeps its name. An unnamed one is
// one of the table's own checks, which the server names.
func (p *parser) checks(table *schemamodel.Table) error {
	if err := p.advance(); err != nil {
		return err
	}
	if err := p.expectPunct("{"); err != nil {
		return err
	}
	for !p.isPunct("}") {
		if p.tok.kind == tokenEOF {
			return p.errorf("unterminated Checks block")
		}
		if p.tok.kind != tokenExpr {
			return p.errorf("expected a check expression in backticks, found %s", p.tok.describe())
		}
		expression := p.tok.text
		if err := p.advance(); err != nil {
			return err
		}
		name := ""
		if p.isPunct("[") {
			settings, err := p.settings()
			if err != nil {
				return err
			}
			if name, err = checkName(settings); err != nil {
				return p.wrapAt(err)
			}
		}
		if name == "" {
			table.Checks = append(table.Checks, expression)
			continue
		}
		p.db.Constraints = append(p.db.Constraints, schemamodel.Constraint{
			StructName:      table.StructName,
			Name:            name,
			Type:            "CHECK",
			Table:           table.QualifiedName(),
			CheckExpression: expression,
		})
	}
	return p.advance()
}

// checkName reads a check entry's bracketed list, which may name it.
func checkName(settings []setting) (string, error) {
	name := ""
	for _, entry := range settings {
		switch entry.key {
		case "name":
			name = entry.value
		default:
			return "", fmt.Errorf("unsupported check setting %q", entry.key)
		}
	}
	return name, nil
}
