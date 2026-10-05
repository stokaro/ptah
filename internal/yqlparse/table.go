package yqlparse

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
)

func (p *parser) table() *ast.CreateTableNode {
	guard := p.word("IF")
	if guard {
		p.pos++
		p.wantWord("NOT")
		p.wantWord("EXISTS")
	}
	table := ast.NewCreateTable(p.identifier())
	table.IfNotExists = guard
	if strings.HasPrefix(decodedName(table.Name), "/") {
		p.failf("desired YQL schema paths must be database-relative")
	}
	p.want("(")
	var key []string
	familyColumns := make(map[string][]string)
	for !p.done() && p.peek().Value != ")" {
		switch {
		case p.word("PRIMARY"):
			p.pos++
			p.wantWord("KEY")
			if key != nil {
				p.failf("a table cannot declare its primary key twice")
			}
			key = p.names()
		case p.word("INDEX"):
			p.pos++
			table.AddIndex(p.index(table.Name))
		case p.word("FAMILY"):
			p.pos++
			table.YDBColumnFamilies = append(table.YDBColumnFamilies, p.family())
		case p.anyWord([]string{"CONSTRAINT", "UNIQUE", "CHECK", "FOREIGN"}):
			p.failf("this table element is not supported in a desired YQL schema")
		default:
			table.AddColumn(p.column(familyColumns))
		}
		if !p.accept(",") {
			break
		}
	}
	p.want(")")
	p.primaryKey(table, key)
	p.bindFamilies(table, familyColumns)
	p.tableSettings(table)
	return table
}

func (p *parser) column(families map[string][]string) *ast.ColumnNode {
	column := ast.NewColumn(p.identifier(), p.identifier())
	if p.peek().Value == "(" {
		column.Type += p.typeParameters()
	}
	seen := make(map[string]bool)
	for !p.done() && p.peek().Value != "," && p.peek().Value != ")" {
		clause := strings.ToUpper(p.peek().Value)
		if seen[clause] {
			p.failf("column clause %s is declared twice", clause)
			break
		}
		seen[clause] = true
		switch {
		case p.word("NOT"):
			p.pos++
			p.wantWord("NULL")
			column.Nullable = false
		case p.word("FAMILY"):
			p.pos++
			family := decodedName(p.identifier())
			if family != "default" {
				families[family] = append(families[family], decodedName(column.Name))
			}
		case p.word("DEFAULT"):
			p.pos++
			literal := p.expression("NOT", "FAMILY")
			value, err := defaultValue(literal, column.Type)
			if err != nil {
				p.failf("%v", err)
				break
			}
			column.SetDefault(value)
		default:
			p.failf("unsupported YQL column clause")
		}
	}
	return column
}

func (p *parser) typeParameters() string {
	start := p.peek().Start
	p.want("(")
	for !p.done() && p.peek().Value != ")" {
		token := p.peek()
		if token.Type != lexer.TokenIdentifier && token.Value != "," {
			p.failf("unsupported type parameter")
			break
		}
		p.pos++
	}
	end := p.peek().End
	p.want(")")
	return strings.ReplaceAll(p.text[start:end], " ", "")
}

func (p *parser) primaryKey(table *ast.CreateTableNode, names []string) {
	if len(names) == 0 {
		p.failf("a YDB table needs a primary key")
		return
	}
	columns := make(map[string]*ast.ColumnNode)
	for _, column := range table.Columns {
		name := decodedName(column.Name)
		if columns[name] != nil {
			p.failf("column %q is declared twice", name)
		}
		columns[name] = column
	}
	seen := make(map[string]bool)
	for _, raw := range names {
		name := decodedName(raw)
		column := columns[name]
		switch {
		case seen[name]:
			p.failf("primary key column %q is declared twice", name)
		case column == nil:
			p.failf("primary key names unknown column %q", name)
		case column.Nullable:
			p.failf("nullable primary key column %q cannot be represented by a desired schema; declare NOT NULL", name)
		}
		seen[name] = true
	}
	table.AddConstraint(&ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: names})
}
