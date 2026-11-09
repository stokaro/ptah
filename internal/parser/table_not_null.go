package parser

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/tableref"
)

// tableNotNullClause keeps the column reference until the table body is read.
type tableNotNullClause struct {
	name     string
	column   string
	position int
}

func (p *Parser) takesTableNotNull() bool {
	return !p.target.known || p.target.grammar.Constraints.TableNotNull
}

func (p *Parser) readTableNotNull(name string) error {
	position := p.current.Start
	if err := p.expectWords("NOT", "NULL"); err != nil {
		return err
	}
	p.skipWhitespace()
	column, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected column name after NOT NULL: %w", err)
	}
	p.tableNotNull = append(p.tableNotNull, tableNotNullClause{name: name, column: column, position: position})
	p.definitions++
	return nil
}

func (p *Parser) applyTableNotNull(table *ast.CreateTableNode) error {
	for _, clause := range p.tableNotNull {
		key := tableNotNullColumnKey(clause.column)
		var column *ast.ColumnNode
		for _, candidate := range table.Columns {
			if tableNotNullColumnKey(candidate.Name) == key {
				column = candidate
				break
			}
		}
		if column == nil {
			return ptaherr.At(clause.position, fmt.Errorf("NOT NULL names column %s, which table %s does not declare", clause.column, table.Name))
		}
		if column.NotNullConstraintName != "" && clause.name != "" &&
			tableNotNullColumnKey(column.NotNullConstraintName) != tableNotNullColumnKey(clause.name) {
			return ptaherr.At(clause.position, fmt.Errorf("column %s already has a named NOT NULL constraint", clause.column))
		}
		column.SetNotNull()
		if clause.name != "" {
			column.SetNotNullConstraintName(clause.name)
		}
	}
	return nil
}

// tableNotNullColumnKey follows PostgreSQL's fold of bare identifiers while
// preserving the case and escaped quotes of quoted identifiers.
func tableNotNullColumnKey(written string) string {
	part, ok := tableref.Parse(written)
	if !ok || part.Qualified {
		return written
	}
	if strings.HasPrefix(written, `"`) {
		return part.Name
	}
	return identifier.ComparisonASCIIInsensitive.IdentityKey(part.Name)
}
