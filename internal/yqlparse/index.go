package yqlparse

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

func (p *parser) index(table string) *ast.IndexNode {
	index := &ast.IndexNode{Name: p.identifier(), Table: table, Type: "sync"}
	p.indexKind(index)
	p.wantWord("ON")
	index.Columns = p.names()
	if p.word("COVER") {
		p.pos++
		index.IncludeColumns = p.names()
	}

	if p.word("WITH") {
		p.pos++
		p.indexSettings(index, p.options())
	}
	return index
}

func (p *parser) indexSettings(index *ast.IndexNode, raw map[string]string) {
	values := make(map[string]string)
	kind, kindErr := ydbindex.KindOf(index.Type)
	if kindErr != nil {
		p.failf("%v", kindErr)
		return
	}
	var allowed []string
	switch {
	case kind == ydbindex.Vector:
		allowed = slices.Concat(ydbpartition.Attributes(), ydbindex.VectorAttributes())
	case kind.IsFullText():
		allowed = ydbindex.FullTextAttributes()
	case kind.IsLocal():
		allowed = ydbindex.LocalAttributes()
	default:
		allowed = ydbpartition.Attributes()
	}
	for _, name := range slices.Sorted(maps.Keys(raw)) {
		value := raw[name]
		if !slices.Contains(allowed, name) {
			p.failf("unsupported index setting %q", name)
		}
		values[name] = scalar(value)
	}
	partitioning, err := ydbindex.ParseDeclaration(values)
	if err != nil {
		p.failf("%v", err)
	}
	if index.Facets, err = ydbindex.WithPartitioning(index.Facets, partitioning); err != nil {
		p.failf("%v", err)
	}
	vector, err := ydbindex.DeclareVector(values, index.Type, index.Operator)
	if err != nil {
		p.failf("%v", err)
	}
	if vector != nil {
		if index.Facets, err = index.Facets.With(vector); err != nil {
			p.failf("%v", err)
		}
	}
	values["type"] = strings.ToLower(index.Type)
	if index.StorageParams, err = ydbindex.ParseOptionsDeclaration(values); err != nil {
		p.failf("%v", err)
	}
}

func (p *parser) indexKind(index *ast.IndexNode) {
	local := p.word("LOCAL")
	if local {
		p.pos++
	} else {
		p.wantWord("GLOBAL")
	}
	if p.word("UNIQUE") {
		p.pos++
		index.Unique = true
	}
	mode := p.word("ASYNC") || p.word("SYNC")
	async := p.word("ASYNC")
	if p.word("ASYNC") {
		p.pos++
		index.Type = "async"
	} else if p.word("SYNC") {
		p.pos++
	}
	using := p.word("USING")
	if using {
		if async || index.Unique || local && mode {
			p.failf("USING cannot be combined with ASYNC, UNIQUE or a local index mode")
		}
		p.pos++
		index.Type = decodedName(p.identifier())
	} else if local {
		p.failf("LOCAL indexes require USING and an index method")
	}
	kind, err := ydbindex.KindOf(index.Type)
	if err != nil {
		p.failf("%v", err)
	}
	if using && (kind == ydbindex.Sync || kind == ydbindex.Async) {
		p.failf("USING requires a YDB vector, full-text or local index method")
	}
	if kind.IsLocal() != local {
		p.failf("index method %q does not match its GLOBAL or LOCAL scope", index.Type)
	}
	if index.Unique && index.Type != "sync" {
		p.failf("UNIQUE requires a synchronous global index")
	}
}
