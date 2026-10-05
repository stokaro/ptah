package yqlparse

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbcoordination"
)

func (p *parser) coordination() *ast.CreateCoordinationNodeNode {
	p.wantWord("NODE")
	name := p.path()
	spec, err := ydbcoordination.ParseDeclaration(p.declarationSettings(coordinationSetting))
	if err != nil {
		p.failf("%v", err)
	}
	return &ast.CreateCoordinationNodeNode{Name: name, Spec: spec}
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
