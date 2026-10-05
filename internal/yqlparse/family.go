package yqlparse

import (
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbfamily"
)

func (p *parser) family() ast.YDBColumnFamilySpec {
	name := decodedName(p.identifier())
	values := map[string]string{"name": name}
	raw := make(map[string]string)
	if p.pos+1 < len(p.tokens) && p.peek().Value == "(" && p.tokens[p.pos+1].Value == ")" {
		p.pos += 2
	} else {
		raw = p.options()
	}
	for _, key := range slices.Sorted(maps.Keys(raw)) {
		switch key {
		case "data", "compression", "cache_mode":
			values[key] = scalar(raw[key])
		default:
			p.failf("unsupported column-family setting %q", key)
		}
	}
	spec, err := ydbfamily.ParseDeclaration(values)
	if err != nil {
		p.failf("%v", err)
	}
	if spec.Name != name {
		p.failf("column-family name %q cannot be represented exactly", name)
	}
	return spec
}

func (p *parser) bindFamilies(table *ast.CreateTableNode, columns map[string][]string) {
	declared := make(map[string]bool)
	for i := range table.YDBColumnFamilies {
		family := &table.YDBColumnFamilies[i]
		if declared[family.Name] {
			p.failf("column family %q is declared twice", family.Name)
		}
		declared[family.Name] = true
		family.Columns = columns[family.Name]
	}
	for _, name := range slices.Sorted(maps.Keys(columns)) {
		if !declared[name] {
			p.failf("column family %q is not declared", name)
		}
	}
}
