package yqlparse

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbexternal"
)

func (p *parser) external(replace bool) ast.Node {
	if p.word("TABLE") {
		p.pos++
		return p.externalTable(replace)
	}
	p.wantWord("DATA")
	p.wantWord("SOURCE")
	name := p.externalPath()
	values := p.externalSettings()
	source := &ast.CreateExternalDataSourceNode{
		Name: name, Replace: replace,
		SourceType: p.requiredExternalSetting(values, "SOURCE_TYPE"),
		Location:   takeExternalSetting(values, "LOCATION"),
		AuthMethod: p.requiredExternalSetting(values, "AUTH_METHOD"),
	}
	source.Options = values
	return source
}

func (p *parser) externalTable(replace bool) *ast.CreateExternalTableNode {
	table := &ast.CreateExternalTableNode{Name: p.externalPath(), Replace: replace}
	p.want("(")
	var columns []ydbexternal.Column
	for !p.done() && p.peek().Value != ")" {
		if p.anyWord([]string{"PRIMARY", "FOREIGN"}) && p.pos+1 < len(p.tokens) && p.tokens[p.pos+1].MatchIdentifierValue("KEY") {
			p.failf("external tables cannot declare keys")
			break
		}
		column := ast.ExternalColumn{Name: decodedName(p.identifier()), Type: p.identifier()}
		if p.peek().Value == "(" {
			column.Type += p.typeParameters()
		}
		if p.word("NOT") {
			p.pos++
			p.wantWord("NULL")
			column.NotNull = true
		}
		table.Columns = append(table.Columns, column)
		columns = append(columns, ydbexternal.Column{Name: column.Name, Type: column.Type, NotNull: column.NotNull})
		if !p.accept(",") {
			break
		}
	}
	p.want(")")
	if err := ydbexternal.CheckColumns(columns); err != nil {
		p.failf("%v", err)
	}
	values := p.externalSettings()
	table.DataSource = p.requiredExternalSetting(values, "DATA_SOURCE")
	table.Location = p.requiredExternalSetting(values, "LOCATION")
	table.Options = values
	return table
}

// externalPath converts the YQL path to the reference the external-object AST
// uses. A literal dot belongs to a path segment, not a schema separator.
func (p *parser) externalPath() string {
	path := decodedName(p.path())
	if slash := strings.LastIndex(path, "/"); slash >= 0 {
		if slash == len(path)-1 {
			p.failf("an external object path needs a name after its directory")
		}
		return tableref.Canonical(path[:slash], path[slash+1:])
	}
	return tableref.Canonical("", path)
}

func (p *parser) externalSettings() map[string]string {
	p.wantWord("WITH")
	raw := p.options()
	values := make(map[string]string, len(raw))
	for _, key := range slices.Sorted(maps.Keys(raw)) {
		value, ok := stringLiteral(raw[key], "String")
		if !ok || strings.HasSuffix(strings.ToLower(raw[key]), "s") {
			p.failf("external object setting %q requires a string literal without a type suffix", key)
		}
		values[strings.ToUpper(key)] = value
	}
	// CheckOptions also trims annotation/YAML values. The quoted YQL values
	// already have their boundaries, so retain their contents without trimming.
	if _, err := ydbexternal.CheckOptions(values); err != nil {
		p.failf("%v", err)
	}
	return values
}

func (p *parser) requiredExternalSetting(values map[string]string, name string) string {
	value := takeExternalSetting(values, name)
	if strings.TrimSpace(value) == "" {
		p.failf("external object setting %s is required", name)
	}
	return value
}

func takeExternalSetting(values map[string]string, name string) string {
	value := values[name]
	delete(values, name)
	return value
}
