// Package featureplan defines contextual batches of feature changes and their
// owner-contributed operations. It does not select providers, render SQL, or
// execute a plan. Every contribution must join the host's complete plan graph.
package featureplan

import (
	"context"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

// Table captures a parent once for every feature that needs its context. A
// missing declaration or observation means uncaptured state, not known absence.
// Subject names the parent with the request's identifier semantics.
type Table struct {
	// Rebuild means the host has selected a parent replacement and owns
	// restoring attached objects. Services still validate every child change.
	Rebuild bool
	Subject objectidentity.ID
	Desired schemacapture.TableDeclaration
	Current schemacapture.TableObservation
}

// Clone returns independent common table definitions. Feature collections keep
// immutable ownership; the selected runtime validates their registered models.
func (t Table) Clone() Table {
	t.Desired = t.Desired.Clone()
	t.Current = t.Current.Clone()
	return t
}

// Request plans accepted changes against captured state. Capabilities and
// identifiers describe the selected target, not a live connection. Owners must
// refuse missing required facts instead of inspecting a database or source file.
// Tables contains all supplied parent context, including unchanged children.
type Request struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Changes      []schemaext.ChangeRecord
	Tables       []Table
}

// Operation carries one typed feature operation and its grammatical placement.
// An alter-table operation names a captured table in Parent. A standalone
// statement has a zero Parent. Notes are preserved as comments before the
// operation. Payload remains typed data; it cannot render or perform I/O.
type Operation struct {
	Role    ast.ExtensionRole
	Parent  objectidentity.ID
	Payload ast.ExtensionPayload
	Notes   []string
}

// ChangePlan accounts for one input change, preserving its subject and kind.
// Steps names the operations that implement it. An empty list is an explicit
// no-op and still requires a nonempty Strategy. Several changes may share a
// step, but every contributed step must be accounted for by a change.
type ChangePlan struct {
	Subject  objectidentity.ID
	Kind     schemaext.Kind
	Strategy string
	Steps    []plangraph.StepID
}

// Result is an owner's complete reply, with one ChangePlan per input in input
// order. Dependencies may name steps supplied by other owners. The caller must
// schedule the complete graph before using any payload. A reply alone is not an
// executable plan; missing external dependencies remain a graph error.
type Result struct {
	Contributions []plangraph.Contribution[Operation]
	Changes       []ChangePlan
}

// Service plans a complete batch without mutating its inputs or performing I/O.
// It is safe for concurrent calls and honors cancellation. Errors discard all
// results, including any operations returned alongside the error.
type Service interface {
	PlanFeatures(context.Context, Request) (Result, error)
}

// Runtime combines selected planning with its local model codecs. Composition
// remains with the caller; a missing service never selects a built-in fallback.
type Runtime interface {
	schemaext.ModelRuntime
	Service
}
