package ydb

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbcoordination"
)

// renderCoordinationNode writes Ptah's own statement for a coordination node.
// YQL has none (see [ydbcoordination]), and Ptah's YDB connection runs the
// statement through the coordination service, so the plan carries the change
// as text like every other step.
//
// The configuration a CREATE gives a node is refused here when the node would
// not run with it as written. An ALTER is checked where the node's other
// settings are known: the connection merges it with the node's configuration
// before it sends it.
func (r *Renderer) renderCoordinationNode(verb ydbcoordination.Verb, name string, spec ast.CoordinationNodeSpec) error {
	if !r.caps.Has(capability.CoordinationNodes) {
		return refuseKey(capability.CoordinationNodes, "coordination node "+name)
	}
	ref, ok := tableref.Parse(name)
	if !ok {
		return fmt.Errorf("%w: %s: coordination node name %q is not a name", ptaherr.ErrInvalidSchemaDiff,
			DialectName, name)
	}
	if err := ydbcoordination.RefuseName(ref.Schema, ref.Name); err != nil {
		return refuseFact("coordination node "+name, err.Error())
	}
	if verb == ydbcoordination.Create {
		if err := ydbcoordination.Validate(spec); err != nil {
			return refuseFact("coordination node "+name, err.Error())
		}
	}
	nodePath := ref.Name
	if schema := strings.TrimRight(ref.Schema, "/"); schema != "" {
		nodePath = schema + "/" + ref.Name
	}
	text, err := ydbcoordination.Statement{Verb: verb, Path: nodePath, Spec: spec}.Text()
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ptaherr.ErrInvalidSchemaDiff, DialectName, err)
	}
	r.w.WriteLinef("%s;", text)
	return nil
}
