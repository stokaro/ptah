package yqlparse

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbexternal"
)

// external reads `CREATE [OR REPLACE] EXTERNAL DATA SOURCE` or `EXTERNAL
// TABLE` as the owner's creation of a declared object. The path is read by
// [ydbexternal.ParsePath], so a dot is part of a name and a path written
// absolute is refused. OR REPLACE, which operation carries, says nothing a
// declaration keeps: whether a plan replaces the object is the plan's
// decision.
func (p *parser) external(operation ydbast.ExternalOperation) ast.Node {
	if p.word("TABLE") {
		p.pos++
		return p.externalTable(operation)
	}
	p.wantWord("DATA")
	p.wantWord("SOURCE")
	schema, name := p.externalPath(ydbexternal.SourceKind)
	values := p.externalSettings()
	source := ydbexternal.DataSource{
		SourceType: p.requiredExternalSetting(values, "SOURCE_TYPE"),
		Location:   takeExternalSetting(values, "LOCATION"),
		AuthMethod: p.requiredExternalSetting(values, "AUTH_METHOD"),
	}
	if len(values) > 0 {
		source.Options = values
	}
	return &ast.ExtensionStatement{Payload: &ydbast.ExternalDataSource{Operation: operation, Schema: schema, Name: name, Spec: source}}
}

func (p *parser) externalTable(operation ydbast.ExternalOperation) ast.Node {
	schema, name := p.externalPath(ydbexternal.TableKind)
	p.want("(")
	var table ydbexternal.Table
	for !p.done() && p.peek().Value != ")" {
		if p.anyWord([]string{"PRIMARY", "FOREIGN"}) && p.pos+1 < len(p.tokens) && p.tokens[p.pos+1].MatchIdentifierValue("KEY") {
			p.failf("external tables cannot declare keys")
			break
		}
		column := ydbexternal.Column{Name: decodedName(p.identifier()), Type: p.identifier()}
		if p.peek().Value == "(" {
			column.Type += p.typeParameters()
		}
		if p.word("NOT") {
			p.pos++
			p.wantWord("NULL")
			column.NotNull = true
		}
		table.Columns = append(table.Columns, column)
		if !p.accept(",") {
			break
		}
	}
	p.want(")")
	if err := ydbexternal.CheckColumns(table.Columns); err != nil {
		p.failf("%v", err)
	}
	values := p.externalSettings()
	table.DataSource = p.requiredExternalSetting(values, "DATA_SOURCE")
	table.Location = p.requiredExternalSetting(values, "LOCATION")
	if len(values) > 0 {
		table.Options = values
	}
	return &ast.ExtensionStatement{Payload: &ydbast.ExternalTable{Operation: operation, Schema: schema, Name: name, Spec: table}}
}

// externalPath reads the path of an external object of kind relative to the
// database root.
func (p *parser) externalPath(kind schemaext.Kind) (schema, name string) {
	ref, err := ydbexternal.ParsePath(kind, decodedName(p.path()))
	if err != nil {
		p.failf("%v", err)
	}
	return ref.Schema.Source, ref.Name.Source
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
