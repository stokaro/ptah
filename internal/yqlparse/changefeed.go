package yqlparse

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/internal/ydbchangefeed"
)

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
	return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: &ydbast.AddChangefeed{Changefeed: spec}}}}
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
