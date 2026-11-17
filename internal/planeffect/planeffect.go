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
// security options, and the views of a target whose views run with their
// definer's or their invoker's privileges, and that mode. A statement it does
// not cover carries no effect, which says nothing about what the statement
// changes.
package planeffect

import (
	"reflect"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/mysqlviewsecurity"
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
	// BeforePlan is Object as the database held it when the plan started, as
	// the plan read it. A statement's Before is what the statements ahead of
	// it leave, so a create, which finds no view, records no Before; this says
	// whether a view of the name existed before the plan, and what it held. A
	// consumer reads from it that a create makes a view again, after a
	// statement that took the view and records no effect for it, such as a
	// drop that cascades. It is nil when the plan read no catalog, since
	// nothing then says whether the view existed.
	BeforePlan *Held `json:"before_plan,omitempty"`
}

// Held is an object as it stood at one point of a plan.
type Held struct {
	// Exists reports that the object was in the catalog the plan read. That
	// catalog is the one the plan compares with, after the read scope and the
	// exclusions, so a view outside them reads as absent.
	Exists bool `json:"exists"`
	// Properties are the values the object held then of the properties the
	// effect reports, sorted by name. It is empty when the object did not
	// exist then. A property it does not list is not described.
	Properties []HeldProperty `json:"properties,omitempty"`
}

// HeldProperty is the value one property of an object held.
type HeldProperty struct {
	// Name names the property, as [Property.Name] does.
	Name string `json:"name"`
	// Value is the value, of the type the property's name gives it.
	Value any `json:"value"`
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

// SQLSecurity names the property of a MySQL or MariaDB view that says whose
// privileges the view runs with: its SQL SECURITY mode. A refused plan names
// the mode by the same name.
const SQLSecurity = mysqlviewsecurity.Name

// Property is one property of an object, with the value it holds before the
// statement runs and after.
//
// The value of a view's security option, security_barrier or security_invoker,
// is a bool: the value PostgreSQL applies, which is false for a view that holds
// no value for the option. The value of [SQLSecurity] is a string, DEFINER or
// INVOKER: the mode MySQL or MariaDB applies, which is DEFINER for a view
// written without the clause.
type Property struct {
	// Name names the property. For a view, it is the option's name,
	// security_barrier or security_invoker, or [SQLSecurity].
	Name string `json:"name"`
	// Before is the value before the statement runs, of the type the
	// property's name gives it. It is nil when the object does not exist then.
	Before any `json:"before,omitempty"`
	// After is the value after the statement runs, of the type the property's
	// name gives it. It is nil when the object does not exist then.
	After any `json:"after,omitempty"`
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
	// back.
	ViewOptions bool
	// ViewSQLSecurity reports that a view of the target runs with its
	// definer's privileges or its invoker's, as the catalog records, the way
	// MySQL and MariaDB record SQL SECURITY. A target with neither it nor
	// ViewOptions has no view described.
	ViewSQLSecurity bool
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
	if !target.ViewOptions && !target.ViewSQLSecurity || len(units) == 0 {
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
	// unknown says nothing is known, what the view holds included.
	unknown presence = iota
	// present says the view exists and holds what its state records.
	present
	// absent says the view does not exist.
	absent
	// uncertain says the view holds what its state records if it exists, and a
	// statement that may have dropped it ran since.
	uncertain
)

// viewState is one view the plan reaches.
type viewState struct {
	// name is the name a statement's name is matched against, by identity
	// under [viewStates.semantics].
	name     string
	object   Object
	presence presence
	held     viewHeld
	// start is what the view held when the plan started, nil when it did not
	// exist then. It is what Current reports, and stays what it is whatever
	// the statements do.
	start *viewHeld
}

// viewHeld is what a view holds of the properties an effect describes.
type viewHeld struct {
	// options are the view's options, as the catalog or a statement spells
	// them.
	options []string
	// security is the view's SQL SECURITY mode, as the catalog or a statement
	// spells it. Empty from the catalog is a mode it did not report; empty in
	// a statement is the server's default.
	security string
}

// clone is a copy of held that shares nothing with it.
func (h viewHeld) clone() viewHeld {
	return viewHeld{options: slices.Clone(h.options), security: h.security}
}

// viewStates is every view the plan reaches, as the statements followed so
// far leave it.
type viewStates struct {
	target Target
	// semantics is the target's, with its default schema replaced by the
	// connection's: an unqualified name in a statement is resolved by the server
	// through the connection, so `v` under a connection to schema app is app.v
	// whatever the dialect's default.
	semantics identifier.Semantics
	// properties are the properties a view of the target is described by,
	// sorted by name.
	properties []viewPropertyReader
	views      []viewState
}

// viewPropertyReader reads one property of a view from what it holds. ok is
// false when it cannot read the value.
type viewPropertyReader struct {
	name  string
	value func(viewHeld) (any, bool)
}

// viewProperties are the properties a view of target is described by, sorted
// by name.
func viewProperties(target Target) []viewPropertyReader {
	var readers []viewPropertyReader
	if target.ViewOptions {
		for _, name := range viewoptions.SecurityOptions() {
			readers = append(readers, viewPropertyReader{name: name, value: securityOption(name)})
		}
	}
	if target.ViewSQLSecurity {
		readers = append(readers, viewPropertyReader{name: SQLSecurity, value: sqlSecurity})
	}
	slices.SortFunc(readers, func(a, b viewPropertyReader) int { return strings.Compare(a.name, b.name) })
	return readers
}

func newViewStates(target Target) *viewStates {
	semantics := target.Semantics.Clone()
	if target.DefaultSchema != "" {
		semantics.DefaultSchema = target.DefaultSchema
	}
	states := &viewStates{target: target, semantics: semantics, properties: viewProperties(target)}
	if target.Current == nil {
		return states
	}
	for _, view := range target.Current.Views {
		schema := view.Schema
		if schema == "" {
			schema = target.DefaultSchema
		}
		held := viewHeld{options: slices.Clone(view.Options), security: view.Security}
		start := held.clone()
		states.views = append(states.views, viewState{
			name:     view.QualifiedName(),
			object:   Object{Kind: KindView, Schema: schema, Name: view.Name},
			presence: present,
			held:     held,
			start:    &start,
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
// statement first, then a COMMENT ON VIEW when it carries a comment and an
// ALTER VIEW ... OWNER TO when it carries an owner (see [createStatements]);
// neither changes an option, so neither has an effect. A dialect that writes
// the comment as a comment line renders it as no statement at all.
func (s *viewStates) create(node *ast.CreateViewNode, statement statementOf) (Effect, bool) {
	if statement.of > createStatements(node) {
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
	before, known := view.before()
	// A statement that names no mode gets the server's default.
	after := viewHeld{options: slices.Clone(node.Options), security: mysqlviewsecurity.Effective(node.Security)}
	view.presence, view.held = present, after
	if !node.Replace {
		// CREATE VIEW succeeds only where no view of the name exists, so the
		// view did not exist before it, whatever the earlier statements left.
		return s.effect(view, ActionCreate, nil, &after), true
	}
	if !known {
		return Effect{}, false
	}
	return s.effect(view, ActionReplace, before, &after), true
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
	before, after := view.held.clone(), view.held.clone()
	var err error
	if len(node.Set) > 0 && statement.at == 0 {
		after.options, err = viewoptions.Apply(before.options, node.Set, nil)
	} else {
		after.options, err = viewoptions.Apply(before.options, nil, node.Reset)
	}
	if err != nil {
		view.presence = unknown
		return Effect{}, false
	}
	// The ALTER succeeds only on a view that exists, and a statement that may
	// have dropped the view changes no option of one it left.
	view.presence, view.held = present, after
	return s.effect(view, ActionAlter, &before, &after), true
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
	was, before := view.presence, view.held.clone()
	view.presence, view.held = absent, viewHeld{}
	if node.Cascade {
		s.mayHaveDropped()
	}
	// The DROP succeeds only on a view that exists, unless IF EXISTS lets it
	// drop nothing, and a statement that may have dropped the view changes no
	// option of one it left.
	if was != present && (was != uncertain || node.IfExists) {
		return Effect{}, false
	}
	return s.effect(view, ActionDrop, &before, nil), true
}

// before is what the view holds before a statement, nil when it does not exist
// then. known is false when the description cannot say.
func (v *viewState) before() (*viewHeld, bool) {
	switch v.presence {
	case present:
		held := v.held.clone()
		return &held, true
	case absent:
		return nil, true
	default:
		return nil, false
	}
}

// resolve returns the view a statement names. A name that resolves to no view
// the plan has reached becomes a view of its own, in an unknown state, so the
// next statement that names it finds it again.
//
// It matches by identity alone, an unqualified name taken in the connection's
// schema. The comparison also pairs an unqualified name with a view of another
// schema when only one view has that name; here that would read the options of
// a view the statement does not touch, and an effect would claim an unchanged
// option on a view whose option the statement changes.
func (s *viewStates) resolve(name string) *viewState {
	key := s.semantics.QualifiedTableIdentityKey(name)
	for i := range s.views {
		if s.semantics.QualifiedTableIdentityKey(s.views[i].name) == key {
			return &s.views[i]
		}
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
		view.presence, view.held = unknown, viewHeld{}
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
//
// It reads the Cascade flag of whatever node or table operation it is given
// rather than naming the node types that carry one. A list of types misses the
// next node that gains the flag, and a cascade it misses leaves a dropped view
// described as present: a CREATE OR REPLACE VIEW after it then claims an
// unchanged option on a view the server creates anew.
func cascades(node ast.Node) bool {
	if alter, isAlter := node.(*ast.AlterTableNode); isAlter {
		return slices.ContainsFunc(alter.Operations, func(operation ast.AlterOperation) bool {
			return cascadeFlag(operation)
		})
	}
	return cascadeFlag(node)
}

// cascadeFlag reports whether value is, or points to, a struct whose Cascade
// field is a set boolean.
func cascadeFlag(value any) bool {
	reflected := reflect.ValueOf(value)
	for reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface {
		if reflected.IsNil() {
			return false
		}
		reflected = reflected.Elem()
	}
	if reflected.Kind() != reflect.Struct {
		return false
	}
	flag := reflected.FieldByName("Cascade")
	return flag.IsValid() && flag.Kind() == reflect.Bool && flag.Bool()
}

// effect is the effect of a statement that leaves view holding after, nil
// when it does not exist then, where it held before.
func (s *viewStates) effect(view *viewState, action Action, before, after *viewHeld) Effect {
	effect := Effect{Version: Version, Object: view.object, Action: action}
	if s.target.Current != nil {
		effect.BeforePlan = &Held{Exists: view.start != nil}
	}
	for _, reader := range s.properties {
		property, ok := viewProperty(reader.name, before, after, reader.value)
		if ok {
			effect.Properties = append(effect.Properties, property)
		}
		if view.start == nil {
			continue
		}
		if value, ok := reader.value(*view.start); ok {
			effect.BeforePlan.Properties = append(effect.BeforePlan.Properties, HeldProperty{Name: reader.name, Value: value})
		}
	}
	return effect
}

// viewProperty is the property name of a view before and after a statement,
// as value reads it from what the view holds. ok is false when value cannot
// read either.
func viewProperty(name string, before, after *viewHeld, value func(viewHeld) (any, bool)) (Property, bool) {
	property := Property{Name: name}
	if before != nil {
		read, ok := value(*before)
		if !ok {
			return Property{}, false
		}
		property.Before = read
	}
	if after != nil {
		read, ok := value(*after)
		if !ok {
			return Property{}, false
		}
		property.After = read
	}
	// An absent value is nil, which equals only another absent one. Every
	// value is a bool or a string, so the comparison cannot panic.
	property.Changed = property.Before != property.After
	return property, true
}

// securityOption reads the security option name from a view's options, as a
// bool. ok is false when the value cannot be read as one.
func securityOption(name string) func(viewHeld) (any, bool) {
	return func(held viewHeld) (any, bool) {
		on, ok := viewoptions.SecurityOn(held.options, name)
		return on, ok
	}
}

// sqlSecurity reads a view's SQL SECURITY mode, as the string the catalog
// spells it. ok is false for anything but DEFINER or INVOKER, which includes a
// mode the catalog did not report.
func sqlSecurity(held viewHeld) (any, bool) {
	mode, err := mysqlviewsecurity.Parse(held.security)
	if err != nil || mode == "" {
		return nil, false
	}
	return mode, true
}

// createStatements is the most statements a CREATE VIEW node renders: the
// CREATE, a COMMENT ON VIEW for a comment, and an ALTER VIEW ... OWNER TO for
// an owner. A target that cannot hold a view comment writes it as a comment
// line, which is no statement, so a node may render fewer. Without the owner
// term, the CREATE of a view the plan gives an owner, a declared one or the
// one a rebuild gives back, reads as a node this package cannot follow and
// carries no effect.
func createStatements(node *ast.CreateViewNode) int {
	statements := 1
	if node.Comment != "" {
		statements++
	}
	if node.Owner != "" {
		statements++
	}
	return statements
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
