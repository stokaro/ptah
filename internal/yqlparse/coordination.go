package yqlparse

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/internal/tableref"
)

func (p *parser) coordination() *ast.ExtensionStatement {
	p.wantWord("NODE")
	name := p.path()
	spec, err := ydbcoordination.ParseDeclaration(p.declarationSettings(coordinationSetting))
	if err != nil {
		p.failf("%v", err)
	}
	ref, ok := tableref.Parse(name)
	if !ok {
		p.failf("invalid coordination node path %q", name)
	}
	nodePath := ref.Name
	if ref.Schema != "" {
		nodePath = ref.Schema + "/" + ref.Name
	}
	ref.Schema, ref.Name = "", nodePath
	if slash := strings.LastIndex(nodePath, "/"); slash >= 0 {
		ref.Schema, ref.Name = nodePath[:slash], nodePath[slash+1:]
	}
	return &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: ref.Schema, Name: ref.Name, Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{Spec: spec}}}}
}

func coordinationSetting(key string) string {
	if !slices.Contains(ydbcoordination.Settings(), key) {
		return "unsupported"
	}
	if key == ydbcoordination.SettingSelfCheckPeriod || key == ydbcoordination.SettingSessionGracePeriod {
		return "Interval"
	}
	return ""
}
