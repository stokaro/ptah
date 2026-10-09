package safety

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

func classifyExtensionNode(node ast.Node) (Severity, string) {
	var payload ast.ExtensionPayload
	switch typed := node.(type) {
	case *ast.ExtensionStatement:
		if typed != nil {
			payload = typed.Payload
		}
	case *ast.ExtensionAlterOperation:
		if typed != nil {
			payload = typed.Payload
		}
	}
	return classifyExtension(payload)
}

// Missing or invalid effect metadata must never inherit the additive default
// used by common operations. Destructive requires the strongest existing
// approval; the reason reports uncertainty rather than inventing data loss.
func classifyExtension(payload ast.ExtensionPayload) (Severity, string) {
	unknown := "extension effects are unknown; manual review is required"
	prepared, err := ast.CloneExtensionPayload(payload)
	if err != nil {
		return Destructive, unknown
	}
	source, ok := prepared.(schemaext.EffectSource)
	if !ok {
		return Destructive, unknown
	}
	return classifyFeatureEffect(source.Effect())
}

func classifyFeatureEffect(effect schemaext.Effect) (Severity, string) {
	unknown := "extension effects are unknown; manual review is required"
	if strings.TrimSpace(effect.Reason) == "" {
		return Destructive, unknown
	}
	switch effect.Impact {
	case schemaext.Additive:
		return Safe, effect.Reason
	case schemaext.Behavioral:
		return Warning, effect.Reason
	case schemaext.Destructive:
		return Destructive, effect.Reason
	default:
		return Destructive, unknown
	}
}

// hasExtensionEffect keeps the owner's risk on every statement emitted for an
// extension-bearing unit. SQL keyword rules cannot interpret these operations;
// splitting their output must not turn unknown effects into an additive change.
func hasExtensionEffect(node ast.Node) bool {
	switch typed := node.(type) {
	case *ast.ExtensionStatement, *ast.ExtensionAlterOperation:
		return true
	case *ast.StatementList:
		return typed != nil && slices.ContainsFunc(typed.Statements, hasExtensionEffect)
	case *ast.AlterTableNode:
		if typed == nil {
			return false
		}
		for _, operation := range typed.Operations {
			if _, ok := operation.(*ast.ExtensionAlterOperation); ok {
				return true
			}
		}
	}
	return false
}
