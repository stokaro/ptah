package yqlparse

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbpartition"
)

func (p *parser) tableSettings(table *ast.CreateTableNode) {
	var hash []string
	if p.word("PARTITION") {
		p.pos++
		p.wantWord("BY")
		p.wantWord("HASH")
		for _, name := range p.names() {
			hash = append(hash, decodedName(name))
		}
	}
	values := p.tableOptions()
	var err error
	if strings.EqualFold(values["store"], "COLUMN") {
		columnValues := map[string]string{"store": "column"}
		for _, name := range slices.Sorted(maps.Keys(values)) {
			value := values[name]
			switch name {
			case "store", "ttl":
			case ydbpartition.AttributeMinPartitions:
				columnValues["column_shards"] = value
			default:
				p.failf("unsupported column-table setting %q", name)
			}
		}
		table.YDBColumnTable, err = ydbcolumn.Parse(columnValues)
		if err == nil {
			table.YDBColumnTable.HashColumns = hash
			err = ydbcolumn.Validate(table.YDBColumnTable)
		}
	} else {
		if len(hash) > 0 {
			p.failf("PARTITION BY HASH requires STORE = COLUMN")
		}
		if store, ok := values["store"]; ok && !strings.EqualFold(store, "ROW") {
			p.failf("STORE must be ROW or COLUMN")
		}
		table.YDBPartitioning, err = ydbpartition.ParseTableDeclaration(values)
	}
	if err != nil {
		p.failf("%v", err)
	}
	if ttl, ok := values["ttl"]; ok {
		p.applyTTL(table, ttl)
	}
}

func (p *parser) tableOptions() map[string]string {
	values := make(map[string]string)
	if p.word("WITH") {
		p.pos++
		raw := p.optionsUsing(p.tableOptionValue)
		for _, name := range slices.Sorted(maps.Keys(raw)) {
			value := raw[name]
			if name != "store" && name != "ttl" && !slices.Contains(ydbpartition.TableAttributes(), name) {
				p.failf("unsupported table setting %q", name)
			}
			if name == "partition_at_keys" {
				points, err := splitPoints(value)
				if err != nil {
					p.failf("%v", err)
				}
				values[name] = ydbpartition.FormatSplitPoints(points)
				continue
			}
			values[name] = scalar(value)
		}
	}
	return values
}
