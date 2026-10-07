package safety

import (
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
	effect := source.Effect()
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
