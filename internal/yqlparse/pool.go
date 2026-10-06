package yqlparse

import (
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbpool"
)

func (p *parser) resourcePool() ast.Node {
	p.wantWord("POOL")
	if p.word("CLASSIFIER") {
		p.pos++
		return p.poolClassifier()
	}
	name := decodedName(p.identifier())
	p.wantWord("WITH")
	return p.poolSettings(name)
}

// defaultPool reads the declaration spelling emitted by the renderer for the
// server-owned pool. Unlike user and membership changes, this spelling is a
// declaration by itself because the server owns the default pool.
func (p *parser) defaultPool() *ast.CreateResourcePoolNode {
	p.wantWord("RESOURCE")
	p.wantWord("POOL")
	name := decodedName(p.identifier())
	if name != ydbpool.DefaultPool {
		p.failf("only the server-owned default pool uses ALTER RESOURCE POOL in a desired schema")
	}
	p.wantWord("SET")
	return p.poolSettings(name)
}

func (p *parser) poolSettings(name string) *ast.CreateResourcePoolNode {
	raw := p.options()
	values := map[string]string{ydbpool.AttributeName: name}
	for _, key := range slices.Sorted(maps.Keys(raw)) {
		value := raw[key]
		if key == ydbpool.AttributeName || !slices.Contains(ydbpool.PoolAttributes(), key) {
			p.failf("unsupported resource pool setting %q", key)
		}
		// YQL spells an unset limit as the string "-1"; the shared model uses
		// an absent setting. An unquoted negative number is invalid YQL.
		if decoded, ok := stringLiteral(value, "String"); ok && decoded == "-1" {
			continue
		}
		values[key] = scalar(value)
	}
	parsed, spec, err := ydbpool.ParsePool(values)
	if err != nil {
		p.failf("%v", err)
	}
	if parsed != name {
		p.failf("resource pool name %q cannot be represented exactly", name)
	}
	return ast.NewCreateResourcePool(name, spec)
}

func (p *parser) poolClassifier() *ast.CreateResourcePoolClassifierNode {
	name := decodedName(p.identifier())
	if !p.word("WITH") {
		p.failf("a resource pool classifier needs WITH settings")
	}
	values := p.declarationSettings(classifierSetting)
	values[ydbpool.AttributeName] = name
	parsed, spec, err := ydbpool.ParseClassifier(values)
	if err != nil {
		p.failf("%v", err)
	}
	if parsed != name {
		p.failf("classifier name %q cannot be represented exactly", name)
	}
	return ast.NewCreateResourcePoolClassifier(name, spec)
}

func classifierSetting(key string) string {
	if key == ydbpool.AttributeName || !slices.Contains(ydbpool.ClassifierAttributes(), key) {
		return "unsupported"
	}
	return ""
}
