// Package featureplan defines contextual batches of feature changes and their
// owner-contributed operations. It does not select providers, render SQL, or
// execute a plan. Every contribution must join the host's complete plan graph.
package featureplan

import (
	"context"
	"slices"

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

// TypeContext holds captured type definitions, including unchanged types.
// Identities use common type kinds; references retain their SQL spelling for
// the receiving owner to interpret. Each side contains unique identities.
// An empty side means no supplied facts, not an inspected empty namespace.
// A missing type is not proof of absence. The zero value supplies no context.
type TypeContext struct {
	Desired []schemaext.DefinedType
	Current []schemaext.DefinedType
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
	// Types supplies common identity and reference facts across owners. It
	// carries no owner-specific model values and does not narrow to Changes.
	Types TypeContext
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
	// Policy is the operator's planning policy for this plan, as the planner
	// that builds the request received it. The zero value asks for nothing.
	Policy Policy
}

// Policy is how the operator asked one plan to be made. It is a request, not a
// prediction: an owner without a behavior plans as it would without the
// request, and refuses a change it cannot make in its own words. Each field
// has the meaning of the planext.Options field of the same name.
type Policy struct {
	// AllowTableRebuild plans a change the target cannot make in place as an
	// explicit rebuild of its table, rather than refuse it.
	AllowTableRebuild bool
	// TableRebuildRequest is how the operator asks for AllowTableRebuild, such
	// as --allow-table-rebuild, for a refusal to name. A planner that fills a
	// policy sets it whether or not the operator asked, because the refusal is
	// what an operator who did not ask reads.
	TableRebuildRequest string
	// OnlineAlter applies the plan without blocking the writes already running,
	// where the target has a way to, and fails at the statement where it
	// cannot.
	OnlineAlter bool
	// ConcurrentIndexBuilds builds every added index without blocking writes.
	// ConcurrentIndexBuildSubjects names the indexes to build that way when it
	// is false.
	ConcurrentIndexBuilds        bool
	ConcurrentIndexBuildSubjects []objectidentity.ID
	// ConcurrentIndexDrops drops every removed index without blocking writes.
	// ConcurrentIndexDropSubjects names the indexes to drop that way when it is
	// false.
	ConcurrentIndexDrops        bool
	ConcurrentIndexDropSubjects []objectidentity.ID
	// OmitNullBackfill makes a column NOT NULL without filling its NULL rows
	// first.
	OmitNullBackfill bool
}

// Clone returns a policy that shares no slice with p.
func (p Policy) Clone() Policy {
	p.ConcurrentIndexBuildSubjects = slices.Clone(p.ConcurrentIndexBuildSubjects)
	p.ConcurrentIndexDropSubjects = slices.Clone(p.ConcurrentIndexDropSubjects)
	return p
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
	// CreateTable creates the declared table, which nothing observed. The
	// CREATE TABLE statement carries the settings attached to the table; the
	// table's named children are not part of it, and each owner creates its
	// own in steps it contributes, placed where the objects they name exist.
	// A receipt with no steps says the owner has nothing to create there.
	CreateTable ParentAction = "create-table"
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
	// PhasePrerequisite is for an operation on an object that common objects
	// of any family may depend on and that itself depends on schemas alone,
	// such as a PostgreSQL extension, which supplies types, routines and
	// operator classes. A host that accepts it places a creation or a change
	// after it creates schemas and before it creates or changes any other
	// object, and a drop after it removes every other object. The migration
	// planners of the PostgreSQL family accept it, and a whole-schema render
	// runs such a step after its schema statements and before the rest; every
	// other host refuses it.
	PhasePrerequisite Phase = "prerequisite"
	// PhaseType is for an operation on a data type that columns, routine
	// signatures and other types may name, and that may itself name an enum,
	// a type an extension supplies, or a routine, such as a PostgreSQL
	// domain. A host that accepts it places a creation, a change in place,
	// and both halves of a type dropped and created again after it creates
	// the enums and the routines whose signatures name no type the plan
	// creates, and before it creates those routines that do and the tables.
	// A step that drops a type no step of the phase creates again runs after
	// the host removes tables, routines and roles, and before it removes
	// enums. The owner orders its own steps in each window, the drop of a
	// rebuilt type before its creation among them. The migration planners of
	// the PostgreSQL family accept it, and a whole-schema render runs such a
	// step after its enums and before the routines that name a type and the
	// tables; every other host refuses it.
	PhaseType Phase = "type"
)

// Valid reports whether p is a phase this contract defines.
func (p Phase) Valid() bool {
	return p == PhaseDefault || p == PhaseDependent || p == PhasePrerequisite || p == PhaseType
}

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
