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
	// Action names a host operation that requires assessment even when no
	// attached feature changed. Its zero value supplies context only.
	Action  ParentAction
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
	// CommonSteps describes host operations available for dependency references
	// and explicit rewrites. Providers cannot inspect the host's mutable graph.
	CommonSteps []CommonStep
	// ParentKinds is the model vocabulary assigned to this service. The
	// selected runtime derives it from registration; callers cannot narrow it.
	ParentKinds []schemaext.Kind
	// DatabasePath is the absolute path of the database the plan runs
	// against, such as /local, and empty when the target has none or the
	// host does not know it, as for a declaration rendered without a
	// database. An owner reads a path its operation writes absolute against
	// it, to name the object the path holds in an effect.
	DatabasePath string
}

// ParentAction identifies a common table operation. It does not grant
// permission to perform that operation or imply that attached state is known.
type ParentAction string

const (
	// AlterTable changes surviving common objects whose dependencies may be
	// owned by a feature. The captured table remains present on both sides.
	AlterTable ParentAction = "alter-table"
	// DropTable removes the captured table and its attached state.
	DropTable ParentAction = "drop-table"
	// RebuildTable replaces a table and restores its effective declaration.
	RebuildTable ParentAction = "rebuild-table"
)

// ParentPlan accounts for one model's state through a parent operation, even
// for an empty namespace. Strategy explains preservation or accepted loss;
// Steps names any contributed operations required by that strategy. A receipt
// with no steps relies on the host's operation and is not proof of reversibility.
type ParentPlan struct {
	Subject  objectidentity.ID
	Kind     schemaext.Kind
	Action   ParentAction
	Strategy string
	Steps    []plangraph.StepID
}

// Operation carries one typed feature operation and its grammatical placement.
// An alter-table operation names a captured table in Parent. A standalone
// statement has a zero Parent. Notes are preserved as comments before the
// operation. Payload remains typed data; it cannot render or perform I/O.
// Phase selects the host's window for the operation.
type Operation struct {
	Role    ast.ExtensionRole
	Parent  objectidentity.ID
	Payload ast.ExtensionPayload
	Notes   []string
	Phase   Phase
}

// Phase selects the host window a feature operation joins. A host defines the
// windows it has and refuses a phase it has none for, so an operation is never
// placed where its owner did not ask. An owner orders an operation further
// through dependencies on the common steps whose effects it can identify; the
// phase is the window those dependencies move within.
type Phase string

const (
	// PhaseDefault is a host's ordinary window for feature operations, which
	// every host accepts. Where it lies, and whether it orders creations
	// against drops, is the host's.
	PhaseDefault Phase = ""
	// PhaseDependent is for an operation on an object that depends on objects
	// of every common family -- relations, columns, routines and roles -- and
	// that no common object reads, such as a row-security policy. A host that
	// accepts it places the operation after its creations and changes of those
	// families and before its removals, in one window that creations and drops
	// share, and the owner orders its own steps there. The migration planners
	// of the PostgreSQL family and of SQL Server accept it; every other host
	// refuses it, a whole-schema render included.
	PhaseDependent Phase = "dependent"
)

// Valid reports whether p is a phase this contract defines.
func (p Phase) Valid() bool { return p == PhaseDefault || p == PhaseDependent }

// ChangePlan accounts for one input change, preserving its subject and kind.
// Steps names the operations that implement it. An empty list is an explicit
// no-op and still requires a nonempty Strategy. Several changes may share a
// step, but every contributed step must be accounted for by a change or parent.
type ChangePlan struct {
	Subject  objectidentity.ID
	Kind     schemaext.Kind
	Strategy string
	Steps    []plangraph.StepID
}

// Result is an owner's complete reply. A successful reply has one ChangePlan per
// input in input order. Dependencies may name steps supplied by other owners.
// The caller must schedule the complete graph before using any payload. A reply
// alone is not an executable plan; missing external dependencies remain a graph error.
type Result struct {
	// Complete confirms a completed decision for the whole batch, including a
	// refusal. The zero result is never a successful no-op.
	Complete      bool
	Contributions []plangraph.Contribution[Operation]
	Changes       []ChangePlan
	// Parents accounts for every table with an Action, in table order, and
	// every assigned ParentKind, in kind order. Missing receipts are errors.
	Parents []ParentPlan
	// Rewrites transfers explicitly claimed common steps to contributed units.
	// The host validates and applies these claims before scheduling the graph.
	Rewrites []plangraph.Rewrite
	// Diagnostics describes a completed refusal. A refused batch carries no
	// contributions, rewrites, or change/parent receipts, including a successful prefix.
	Diagnostics []Diagnostic
}

// Service plans a complete batch without mutating its inputs or performing I/O.
// Semantic refusals are diagnostics in a complete result. Errors mean planning
// could not be performed and discard all results. Implementations are safe for
// concurrent calls and honor cancellation.
type Service interface {
	PlanFeatures(context.Context, Request) (Result, error)
}

// Runtime combines selected planning with its local model codecs. Composition
// remains with the caller; a missing service never selects a built-in fallback.
type Runtime interface {
	schemaext.ModelRuntime
	Service
}
