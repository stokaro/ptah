package yqlparse

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbchangefeed"
)

func (p *parser) alterDeclaration() ast.Node {
	p.wantWord("ALTER")
	switch {
	case p.word("USER"):
		p.pos++
		return p.alterUser()
	case p.word("GROUP"):
		p.pos++
		return p.alterGroup()
	case p.word("TABLE"):
		p.pos++
		return p.changefeed()
	case p.word("TOPIC"):
		p.pos++
		path := decodedName(p.path())
		directory, name := "", path
		if slash := strings.LastIndexByte(path, '/'); slash >= 0 {
			directory, name = path[:slash], path[slash+1:]
		}
		p.wantWord("ADD")
		p.wantWord("CONSUMER")
		return ast.NewAddTopicConsumer(tableref.Canonical(directory, name), p.consumer())
	default:
		return p.defaultPool()
	}
}

func (p *parser) changefeed() *ast.AlterTableNode {
	table := p.path()
	p.wantWord("ADD")
	p.wantWord("CHANGEFEED")
	name := decodedName(p.identifier())
	values := p.declarationSettings(changefeedSetting)
	if value, ok := values[ydbchangefeed.AttributeTopicAutoPartitioning]; ok {
		switch strings.ToUpper(value) {
		case "ENABLED":
			values[ydbchangefeed.AttributeTopicAutoPartitioning] = "true"
		case "DISABLED":
			values[ydbchangefeed.AttributeTopicAutoPartitioning] = "false"
		default:
			p.failf("topic_auto_partitioning requires ENABLED or DISABLED")
		}
	}
	values[ydbchangefeed.AttributeName] = name
	spec, err := ydbchangefeed.ParseDeclaration(values)
	if err != nil {
		p.failf("%v", err)
	}
	if spec.Name != name {
		p.failf("changefeed name %q cannot be represented exactly", name)
	}
	return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{&ast.AddChangefeedOperation{Changefeed: spec}}}
}

func changefeedSetting(key string) string {
	switch key {
	case ydbchangefeed.AttributeMode, ydbchangefeed.AttributeFormat,
		ydbchangefeed.AttributeVirtualTimestamps, ydbchangefeed.AttributeInitialScan,
		ydbchangefeed.AttributeUserSIDs, ydbchangefeed.AttributeSchemaChanges,
		ydbchangefeed.AttributeTopicMinActivePartitions, ydbchangefeed.AttributeTopicAutoPartitioning:
		return ""
	case ydbchangefeed.AttributeResolvedTimestamps, ydbchangefeed.AttributeRetentionPeriod:
		return "Interval"
	default:
		return "unsupported"
	}
}
