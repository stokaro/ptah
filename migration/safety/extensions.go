package safety

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func classifyExtensionNode(node ast.Node) extensionVerdict {
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

// extensionVerdict is the safety reading of one owned operation: the higher of
// its lifecycle and access severities, and the owner's access assessment when
// the payload declares one. A zero access means the payload makes no claim.
type extensionVerdict struct {
	severity Severity
	reason   string
	access   schemaext.AccessEffect
}

const unknownExtensionEffect = "extension effects are unknown; manual review is required"

// Missing or invalid effect metadata must never inherit the additive default
// used by common operations. Destructive requires the strongest existing
// approval; the reason reports uncertainty rather than inventing data loss.
// The access assessment is read separately from the lifecycle effect, and the
// statement takes the higher of the two severities.
func classifyExtension(payload ast.ExtensionPayload) extensionVerdict {
	verdict := extensionVerdict{severity: Destructive, reason: unknownExtensionEffect}
	prepared, err := ast.CloneExtensionPayload(payload)
	if err != nil {
		if _, declares := payload.(schemaext.AccessEffectSource); declares {
			verdict.access = unknownAccess
		}
		return verdict
	}
	if source, ok := prepared.(schemaext.EffectSource); ok {
		verdict.severity, verdict.reason = classifyFeatureEffect(source.Effect())
	}
	if source, ok := prepared.(schemaext.AccessEffectSource); ok {
		verdict.access = readAccess(source.AccessEffect())
		if severity := accessSeverity(verdict.access.Access); severityRank(severity) > severityRank(verdict.severity) {
			verdict.severity, verdict.reason = severity, accessReason(verdict.access)
		}
	}
	return verdict
}

func classifyFeatureEffect(effect schemaext.Effect) (Severity, string) {
	if strings.TrimSpace(effect.Reason) == "" {
		return Destructive, unknownExtensionEffect
	}
	switch effect.Impact {
	case schemaext.Additive:
		return Safe, effect.Reason
	case schemaext.Behavioral:
		return Warning, effect.Reason
	case schemaext.Destructive:
		return Destructive, effect.Reason
	default:
		return Destructive, unknownExtensionEffect
	}
}

// unknownAccess replaces an assessment the classifier cannot trust. It is never
// read as unchanged: an access effect nobody established needs manual review.
var unknownAccess = schemaext.AccessEffect{
	Access: schemaext.AccessUnknown,
	Reason: "the access effect is not established; manual review is required",
}

func readAccess(effect schemaext.AccessEffect) schemaext.AccessEffect {
	if effect.Validate() != nil {
		return unknownAccess
	}
	return effect
}

// accessSeverity maps an owner's access assessment onto the shared scale.
// Widening access removes a protection, the reason DISABLE ROW LEVEL SECURITY
// is destructive, and an unknown effect takes the same strongest review that
// unknown extension effects take. Narrowing access can deny a deployed reader
// or writer what it relies on, which is a behavioral change.
func accessSeverity(access schemaext.Access) Severity {
	switch access {
	case schemaext.AccessUnchanged:
		return Safe
	case schemaext.AccessNarrows:
		return Warning
	default:
		return Destructive
	}
}

func accessReason(effect schemaext.AccessEffect) string {
	switch effect.Access {
	case schemaext.AccessWidens:
		return "can widen access: " + effect.Reason
	case schemaext.AccessNarrows:
		return "can narrow access: " + effect.Reason
	case schemaext.AccessUnchanged:
		return "leaves access unchanged: " + effect.Reason
	default:
		return "access effect is unknown: " + effect.Reason
	}
}

// accessRank orders assessments for a statement that carries several owned
// operations. A widening anywhere means the statement can widen access; an
// unknown effect outranks a narrowing because it may widen it too.
func accessRank(access schemaext.Access) int {
	switch access {
	case schemaext.AccessUnchanged:
		return 1
	case schemaext.AccessNarrows:
		return 2
	case schemaext.AccessUnknown:
		return 3
	case schemaext.AccessWidens:
		return 4
	default:
		return 0
	}
}

// combineAccess keeps the strongest access assessment a statement carries.
func combineAccess(target *StatementAssessment, access schemaext.Access, reason string) {
	if accessRank(access) > accessRank(target.Access) {
		target.Access, target.AccessReason = access, reason
	}
}

func applyExtensionVerdict(target *StatementAssessment, verdict extensionVerdict) {
	if severityRank(verdict.severity) > severityRank(target.Severity) {
		target.Severity, target.Reason = verdict.severity, verdict.reason
	}
	combineAccess(target, verdict.access.Access, verdict.access.Reason)
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

// extensionSubject reads an owner's structured identity after snapshotting the
// payload. Its name stays a literal component rather than being split on dots.
func extensionSubject(node ast.Node) string {
	envelope, ok := node.(*ast.ExtensionStatement)
	if !ok || envelope == nil {
		return ""
	}
	payload, err := ast.CloneExtensionPayload(envelope.Payload)
	if err != nil {
		return ""
	}
	source, ok := payload.(interface{ Subject() objectidentity.ID })
	if !ok {
		return ""
	}
	subject := source.Subject()
	return strings.TrimPrefix(subject.String(), string(subject.Kind)+" ")
}
