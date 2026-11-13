// Package planeffect describes what a planned statement does to a database
// object: the object it changes, the kind of statement that changes it, and the
// value each property it reports holds before the statement runs and after.
//
// A saved plan, and the documents `ptah schema plan --json` and `ptah schema
// apply --json` print, record the description beside each statement it covers.
// A reviewer then reads what a statement changes, where reading the SQL cannot
// tell a changed value from a restated one: CREATE OR REPLACE VIEW has to state
// every option the view keeps, so a body change that keeps security_invoker
// reads like one that turns it on.
//
// It describes the views of a target that keeps view options, and their
// security options. A statement it does not cover carries no effect, which says
// nothing about what the statement changes.
package planeffect

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/objectlookup"
	"ptah.run/internal/tableref"
	"ptah.run/internal/viewoptions"
)

// Version is the version of the vocabulary every [Effect] carries.
//
// It rises when a field changes meaning or leaves. A new object kind, action or
// property does not raise it, and neither does a new field: a reader leaves
// out what it does not know, and reads the statement for it instead.
const Version = 1

// Effect is what one statement does to one object.
type Effect struct {
	// Version is [Version].
	Version int `json:"version"`
	// Object is the object the statement changes.
	Object Object `json:"object"`
	// Action is the kind of statement that changes it.
	Action Action `json:"action"`
	// Properties are the properties of Object the effect reports, sorted by
	// name. A property it does not list is not described, which is not the
	// same as unchanged.
	Properties []Property `json:"properties,omitempty"`
}

// Object names a database object.
type Object struct {
	// Kind is the kind of object.
	Kind Kind `json:"kind"`
	// Schema is the schema that holds the object, as the database spells it.
	Schema string `json:"schema,omitempty"`
	// Name is the object's own name, as the database spells it.
	Name string `json:"name"`
}

// Kind is the kind of an [Object].
type Kind string

// KindView is a view. A materialized view is another kind.
const KindView Kind = "view"

// Action is the kind of statement an [Effect] describes.
type Action string

const (
	// ActionCreate creates the object, which does not exist before the
	// statement runs.
	ActionCreate Action = "create"
	// ActionReplace writes the object again in place, as CREATE OR REPLACE
	// VIEW does: the view takes the definition and the options the statement
	// states and loses the options it does not state.
	ActionReplace Action = "replace"
	// ActionAlter changes some properties of the object and keeps the others,
	// as ALTER VIEW ... SET and RESET do.
	ActionAlter Action = "alter"
	// ActionDrop drops the object, which does not exist after the statement
	// runs.
	ActionDrop Action = "drop"
)

// Property is one property of an object, with the value it holds before the
// statement runs and after.
//
// The value of a view's security option is a boolean: the value PostgreSQL
// applies, which is false for a view that holds no value for the option.
type Property struct {
	// Name names the property. For a view, it is the option's name:
	// security_barrier or security_invoker.
	Name string `json:"name"`
	// Before is the value before the statement runs. It is absent when the
	// object does not exist then.
	Before *bool `json:"before,omitempty"`
	// After is the value after the statement runs. It is absent when the
	// object does not exist then.
	After *bool `json:"after,omitempty"`
	// Changed reports that Before and After differ. An absent value differs
	// from every value, so a property of an object the statement creates or
	// drops is changed.
	Changed bool `json:"changed"`
}

// Target is what a description reads beside the statements.
type Target struct {
	// Current is the database the plan starts from, as it was read.
	Current *catalog.Database
	// DefaultSchema is the schema an unqualified name resolves to, and the
	// schema of an object Current reports without one.
	DefaultSchema string
	// Semantics resolves the name a statement gives an object to the object
	// Current holds, the way the comparison resolved it.
	Semantics identifier.Semantics
	// ViewOptions reports that the target keeps view options and reports them
	// back. A target without it has no view described.
	ViewOptions bool
}

// Describe returns the effects of each statement of a plan.
//
// nodes are the plan's nodes. units holds, for each statement in the order the
// plan runs them, the index in nodes of the node whose rendering the statement
// is, as [ptah.run/migration/planner.PlannedUnit] records it; a node renders
// one statement or more. The result has one entry per statement, nil for a
// statement it does not describe, and is nil when it describes none.
//
// The value before each statement is the one Current holds, carried through
// the statements before it that the description follows. A statement whose
// object it cannot resolve, or whose starting state an earlier statement left
// uncertain, has no effect, and neither does a node that rendered a number of
// statements the description does not expect.
func Describe(target Target, nodes []ast.Node, units []int) [][]Effect {
	if !target.ViewOptions || len(units) == 0 {
		return nil
	}
	rendered := make(map[int]int, len(units))
	for _, node := range units {
		rendered[node]++
	}
	views := newViewStates(target)
	effects := make([][]Effect, len(units))
	described := false
	position := make(map[int]int, len(units))
	for i, index := range units {
		at := position[index]
		position[index]++
		if index < 0 || index >= len(nodes) {
			continue
		}
		statement := statementOf{at: at, of: rendered[index]}
		effect, ok := views.follow(nodes[index], statement)
		if ok {
			effects[i] = []Effect{effect}
			described = true
		}
	}
	if !described {
		return nil
	}
	return effects
}

// statementOf places one statement among the statements its node rendered.
type statementOf struct {
	// at is the statement's position, from 0.
	at int
	// of is how many statements the node rendered.
	of int
}

// presence is what the description knows about whether a view exists at a
// point in the plan.
type presence int

const (
	// unknown says nothing is known, the options included.
	unknown presence = iota
	// present says the view exists and holds the tracked options.
	present
	// absent says the view does not exist.
	absent
	// uncertain says the view holds the tracked options if it exists, and a
	// statement that may have dropped it ran since.
	uncertain
)

// viewState is one view the plan reaches.
type viewState struct {
	// name is the name objectlookup matches a statement's name against.
	name     string
	object   Object
	presence presence
	options  []string
}

// viewStates is every view the plan reaches, as the statements followed so
// far leave it.
type viewStates struct {
	target Target
	views  []viewState
}

func newViewStates(target Target) *viewStates {
	states := &viewStates{target: target}
	if target.Current == nil {
		return states
	}
	for _, view := range target.Current.Views {
		schema := view.Schema
		if schema == "" {
			schema = target.DefaultSchema
		}
		states.views = append(states.views, viewState{
			name:     view.QualifiedName(),
			object:   Object{Kind: KindView, Schema: schema, Name: view.Name},
			presence: present,
			options:  slices.Clone(view.Options),
		})
	}
	return states
}

// follow is the effect of one statement, and moves the views it reaches to the
// state the statement leaves them in. ok is false when the statement has no
// effect to report.
func (s *viewStates) follow(node ast.Node, statement statementOf) (Effect, bool) {
	switch node := node.(type) {
	case *ast.CreateViewNode:
		return s.create(node, statement)
	case *ast.AlterViewOptionsNode:
		return s.alter(node, statement)
	case *ast.DropViewNode:
		return s.drop(node, statement)
	}
	if cascades(node) {
		s.mayHaveDropped()
	}
	return Effect{}, false
}

// create follows CREATE VIEW and CREATE OR REPLACE VIEW. The node renders the
// statement first, and a COMMENT ON VIEW after it when it carries a comment;
// the comment changes no option and has no effect.
func (s *viewStates) create(node *ast.CreateViewNode, statement statementOf) (Effect, bool) {
	if statement.of != 1 && (statement.of != 2 || node.Comment == "") {
		s.forget(node.Name)
		return Effect{}, false
	}
	if statement.at != 0 {
		return Effect{}, false
	}
	view := s.resolve(node.Name)
	if view == nil {
		return Effect{}, false
	}
	before, known := view.held()
	after := slices.Clone(node.Options)
	view.presence, view.options = present, after
	if !node.Replace {
		// CREATE VIEW succeeds only where no view of the name exists, so the
		// view did not exist before it, whatever the earlier statements left.
		return s.effect(view.object, ActionCreate, nil, &after), true
	}
	if !known {
		return Effect{}, false
	}
	return s.effect(view.object, ActionReplace, before, &after), true
}

// alter follows ALTER VIEW ... SET, which the node renders first when it sets
// an option, and ALTER VIEW ... RESET, which it renders after when it resets
// one. The options of a materialized view are storage parameters, which are
// not described.
func (s *viewStates) alter(node *ast.AlterViewOptionsNode, statement statementOf) (Effect, bool) {
	if node.Materialized {
		return Effect{}, false
	}
	if statement.of != alterStatements(node) {
		s.forget(node.Name)
		return Effect{}, false
	}
	view := s.resolve(node.Name)
	if view == nil {
		return Effect{}, false
	}
	if view.presence != present && view.presence != uncertain {
		// An ALTER of a view the plan dropped fails, and nothing is known
		// about one in an unknown state.
		return Effect{}, false
	}
	before := slices.Clone(view.options)
	var (
		after []string
		err   error
	)
	if len(node.Set) > 0 && statement.at == 0 {
		after, err = viewoptions.Apply(before, node.Set, nil)
	} else {
		after, err = viewoptions.Apply(before, nil, node.Reset)
	}
	if err != nil {
		view.presence = unknown
		return Effect{}, false
	}
	// The ALTER succeeds only on a view that exists, and a statement that may
	// have dropped the view changes no option of one it left.
	view.presence, view.options = present, after
	return s.effect(view.object, ActionAlter, &before, &after), true
}

// drop follows DROP VIEW. A DROP VIEW ... IF EXISTS of a view a statement may
// have dropped already may have dropped nothing, so it has no effect.
func (s *viewStates) drop(node *ast.DropViewNode, statement statementOf) (Effect, bool) {
	if statement.of != 1 {
		s.forget(node.Name)
		return Effect{}, false
	}
	view := s.resolve(node.Name)
	if view == nil {
		if node.Cascade {
			s.mayHaveDropped()
		}
		return Effect{}, false
	}
	was, before := view.presence, slices.Clone(view.options)
	view.presence, view.options = absent, nil
	if node.Cascade {
		s.mayHaveDropped()
	}
	// The DROP succeeds only on a view that exists, unless IF EXISTS lets it
	// drop nothing, and a statement that may have dropped the view changes no
	// option of one it left.
	if was != present && (was != uncertain || node.IfExists) {
		return Effect{}, false
	}
	return s.effect(view.object, ActionDrop, &before, nil), true
}

// held is the options the view holds before a statement, nil when it does not
// exist then. known is false when the description cannot say.
func (v *viewState) held() (*[]string, bool) {
	switch v.presence {
	case present:
		options := slices.Clone(v.options)
		return &options, true
	case absent:
		return nil, true
	default:
		return nil, false
	}
}

// resolve returns the view a statement names. A name that resolves to no view
// the plan has reached becomes a view of its own, in an unknown state, so the
// next statement that names it finds it again.
func (s *viewStates) resolve(name string) *viewState {
	view := objectlookup.Find(s.views, name, s.target.Semantics, func(view viewState) string { return view.name })
	if view != nil {
		return view
	}
	reference, parsed := tableref.Parse(name)
	if !parsed {
		return nil
	}
	schema := reference.Schema
	if schema == "" {
		schema = s.target.DefaultSchema
	}
	s.views = append(s.views, viewState{
		name:     name,
		object:   Object{Kind: KindView, Schema: schema, Name: reference.Name},
		presence: unknown,
	})
	return &s.views[len(s.views)-1]
}

// forget puts the view a statement names in an unknown state.
func (s *viewStates) forget(name string) {
	if view := s.resolve(name); view != nil {
		view.presence, view.options = unknown, nil
	}
}

// mayHaveDropped records a statement that drops what depends on what it drops,
// which can take a view with it.
func (s *viewStates) mayHaveDropped() {
	for i := range s.views {
		if s.views[i].presence == present {
			s.views[i].presence = uncertain
		}
	}
}

// cascades reports whether node drops what depends on the object it drops. A
// view can be among it.
func cascades(node ast.Node) bool {
	switch node := node.(type) {
	case *ast.DropTableNode:
		return node.Cascade
	case *ast.DropMaterializedViewNode:
		return node.Cascade
	case *ast.DropTypeNode:
		return node.Cascade
	case *ast.DropFunctionNode:
		return node.Cascade
	case *ast.AlterTableNode:
		return slices.ContainsFunc(node.Operations, func(operation ast.AlterOperation) bool {
			drop, isDrop := operation.(*ast.DropColumnOperation)
			return isDrop && drop.Cascade
		})
	}
	return false
}

// effect is the effect of a statement that leaves object holding after, nil
// when it does not exist then, where it held before.
func (s *viewStates) effect(object Object, action Action, before, after *[]string) Effect {
	effect := Effect{Version: Version, Object: object, Action: action}
	for _, name := range viewoptions.SecurityOptions() {
		property, ok := securityProperty(name, before, after)
		if ok {
			effect.Properties = append(effect.Properties, property)
		}
	}
	return effect
}

// securityProperty is the security option name before and after a statement.
// ok is false when either value cannot be read as a boolean.
func securityProperty(name string, before, after *[]string) (Property, bool) {
	property := Property{Name: name}
	if before != nil {
		on, ok := viewoptions.SecurityOn(*before, name)
		if !ok {
			return Property{}, false
		}
		property.Before = &on
	}
	if after != nil {
		on, ok := viewoptions.SecurityOn(*after, name)
		if !ok {
			return Property{}, false
		}
		property.After = &on
	}
	property.Changed = !sameValue(property.Before, property.After)
	return property, true
}

// sameValue reports whether two values are equal, an absent value equal only
// to another absent one.
func sameValue(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// alterStatements is how many statements an option change renders: one ALTER
// VIEW ... SET when it sets an option, and one ALTER VIEW ... RESET when it
// resets one.
func alterStatements(node *ast.AlterViewOptionsNode) int {
	statements := 0
	if len(node.Set) > 0 {
		statements++
	}
	if len(node.Reset) > 0 {
		statements++
	}
	return statements
}
