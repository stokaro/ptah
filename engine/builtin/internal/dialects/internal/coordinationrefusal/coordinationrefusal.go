// Package coordinationrefusal holds the refusal every dialect renderer but
// YDB's makes for a YDB coordination node.
//
// A coordination node is YDB's own object, and no other engine has one. The
// renderer's central check refuses the three coordination node statements on
// a target without [capability.CoordinationNodes] before any dialect sees
// them; this is the answer a dialect's own dispatcher gives when it is handed
// one directly, so it refuses in the same words rather than emitting a
// statement or a comment.
package coordinationrefusal

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
)

// Node refuses node, one of the three coordination node statements, on
// dialect, with a [ptaherr.CapabilityError] naming
// [capability.CoordinationNodes]. Any other node, and a nil one, is not
// refused here.
func Node(dialect string, node ast.Node) error {
	var subject string
	switch typed := node.(type) {
	case *ast.CreateCoordinationNodeNode:
		if typed == nil {
			return nil
		}
		subject = "coordination node " + typed.Name
	case *ast.AlterCoordinationNodeNode:
		if typed == nil {
			return nil
		}
		subject = "changing coordination node " + typed.Name
	case *ast.DropCoordinationNodeNode:
		if typed == nil {
			return nil
		}
		subject = "dropping coordination node " + typed.Name
	default:
		return nil
	}
	normalized := platform.NormalizeDialect(dialect)
	return &ptaherr.CapabilityError{
		Dialect: normalized,
		Feature: string(capability.CoordinationNodes),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target: "+
			"a coordination node is a YDB object", subject, capability.CoordinationNodes, normalized),
	}
}
