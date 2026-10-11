// Package typeplan plans PostgreSQL domain, composite type and range type
// changes into operations for a host's dependency graph, for the owner of
// package pgtype. It performs no I/O.
package typeplan

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/deporder"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/usertypescope"
)

// Service plans user type changes and declarations. Its zero value is ready
// for concurrent use.
//
// A type is created, changed in place, or dropped and created again in
// [featureplan.PhaseType], before the routines and tables that may name it,
// and a type nothing declares is dropped in that phase's removal window,
// after the tables. The plan orders its own steps: the drops of the types it
// rebuilds first, a type that names another before it, then every creation, a
// type another names first, each followed by its comment, then the changes in
// place, domains before composite types.
//
// A domain changes in place when only its CHECK, its default or its NOT NULL
// differ: ALTER DOMAIN drops every CHECK the catalog names and adds the
// declared one, sets the default, and sets or drops NOT NULL. A composite type
// changes in place when the comparison found an attribute delta ALTER TYPE
// reaches the declaration with. Everything else, a range type's every
// difference among it, is a drop and a creation, because PostgreSQL has no
// in-place form for it. The drop is not CASCADE, so it fails while a column
// uses the type rather than taking the column with it. A type that names a
// rebuilt one is rebuilt too, whatever its own change, because the drop would
// fail while it still names the type (stokaro/ptah#1717).
//
// A dropped type goes with CASCADE and a warning note. The comment of a type
// that survives unchanged joins [featureplan.PhaseDependent], after every
// object the plan creates; a rebuilt type takes its comment from the creation.
//
// A type the target cannot create is refused, naming the scope that keeps the
// declaration to the targets that host it: a column declared with the type
// would otherwise be left naming something the server has no definition of.
type Service struct{}

// PlanFeatures returns one receipt per change in input order, or a completed
// refusal with no usable prefix. A user type belongs to no table, so a request
// that names parent kinds is refused.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: a user type belongs to no table", schemaext.ErrInvalidValue)
	}
	changes := make([]*change, 0, len(request.Changes))
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		planned, err := changeOf(index, record)
		if err != nil {
			return featureplan.Result{}, err
		}
		changes = append(changes, planned)
	}
	if refused, problem := refuse(request.Target, request.Capabilities, changes); refused != nil {
		problem.Kind = string(refused.changeKind)
		return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: problem, Change: new(refused.index)}}}, nil
	}
	steps := planChanges(changes)
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(changes))}
	for i, planned := range changes {
		result.Changes[i] = featureplan.ChangePlan{Subject: planned.subject, Kind: planned.changeKind, Strategy: planned.strategy(), Steps: planned.steps}
	}
	if contribution := steps.contribution(); len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// PlanDeclarations creates each declared type for a whole-schema render, a
// type another names first, each followed by its comment.
func (Service) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	changes := make([]*change, 0, len(request.Objects))
	for index, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		creation, err := creationOf(object)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		planned, err := changeOf(index, schemaext.ChangeRecord{Subject: object.Ref, Value: creation})
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		changes = append(changes, planned)
	}
	if refused, problem := refuse(request.Target, request.Capabilities, changes); refused != nil {
		// A declaration's refusal names the declared model, which is what
		// the render was handed, not the change it was planned as.
		problem.Kind = string(refused.model)
		return featureplan.DeclarationResult{Complete: true,
			Diagnostics: []featureplan.DeclarationDiagnostic{{Problem: problem, Object: new(refused.index)}}}, nil
	}
	steps := planDeclarations(changes)
	result := featureplan.DeclarationResult{Complete: true, Declarations: make([]featureplan.DeclarationPlan, len(changes))}
	for i, planned := range changes {
		result.Declarations[i] = featureplan.DeclarationPlan{Subject: planned.subject, Strategy: planned.strategy(), Steps: planned.steps}
	}
	if contribution := steps.contribution(); len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

func validateRequest(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !platform.IsPostgresFamily(target) {
		return fmt.Errorf("%w: PostgreSQL user type planning on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

// creationOf is the change that creates a declared type.
func creationOf(object schemaext.Object) (schemaext.ChangeValue, error) {
	switch declared := object.Value.(type) {
	case *pgtype.DesiredDomain:
		return &pgtype.DomainChange{After: declared}, nil
	case *pgtype.DesiredComposite:
		return &pgtype.CompositeChange{After: declared}, nil
	case *pgtype.DesiredRange:
		return &pgtype.RangeChange{After: declared}, nil
	default:
		return nil, fmt.Errorf("%w: expected a declared user type, got %T", schemaext.ErrInvalidValue, object.Value)
	}
}

// change is one input change as the plan reads it.
type change struct {
	index      int
	subject    objectidentity.ID
	model      schemaext.Kind
	changeKind schemaext.Kind
	// name is the type's name as a statement writes it before quoting.
	name string
	// before and after report the two operands.
	before, after bool
	// create creates the declared type, and comment is its declared comment.
	create  ast.ExtensionPayload
	comment string
	// alter changes the surviving type in place, nil where the change has no
	// in-place form or states nothing to change to.
	alter ast.ExtensionPayload
	// changed reports a difference other than the comment, inPlace that every
	// such difference has an in-place form, and commented that the comment
	// differs.
	changed, inPlace, commented bool
	// held and declared are the type spellings the current definition and the
	// declaration name.
	held, declared []string
	// rebuilt is decided once every change is read.
	rebuilt bool
	// steps names the change's operations in plan order.
	steps []plangraph.StepID
}

// changeOf reads one change, refusing a change of another model than its
// subject names and one that is not valid.
func changeOf(index int, record schemaext.ChangeRecord) (*change, error) {
	if err := pgtype.ValidateRef(record.Subject); err != nil {
		return nil, err
	}
	planned := &change{index: index, subject: record.Subject, model: schemaext.Kind(record.Subject.Kind),
		name: pgtype.TargetOf(record.Subject).QualifiedName()}
	var (
		valid    error
		differs  []string
		inPlace  []string
		matching schemaext.Kind
	)
	switch value := record.Value.(type) {
	case *pgtype.DomainChange:
		valid, matching, differs, inPlace = value.Validate(), pgtype.DomainKind, value.Differs,
			[]string{pgtype.DiffCheck, pgtype.DiffDefault, pgtype.DiffNotNull}
		readDomain(planned, value)
	case *pgtype.CompositeChange:
		valid, matching, differs = value.Validate(), pgtype.CompositeKind, value.Differs
		if len(value.Added) > 0 || len(value.Removed) > 0 {
			inPlace = []string{pgtype.DiffAttributes}
		}
		readComposite(planned, value)
	case *pgtype.RangeChange:
		valid, matching, differs = value.Validate(), pgtype.RangeKind, value.Differs
		readRange(planned, value)
	default:
		return nil, fmt.Errorf("%w: unexpected user type change %T", schemaext.ErrInvalidValue, record.Value)
	}
	if valid != nil {
		return nil, valid
	}
	if matching != planned.model {
		return nil, fmt.Errorf("%w: a %s change for %s", schemaext.ErrInvalidValue, matching, record.Subject)
	}
	planned.changeKind = record.Value.Kind()
	planned.inPlace = true
	for _, attribute := range differs {
		switch {
		case attribute == pgtype.DiffComment:
			planned.commented = true
		case slices.Contains(inPlace, attribute):
			planned.changed = true
		default:
			planned.changed, planned.inPlace = true, false
		}
	}
	return planned, nil
}

func readDomain(planned *change, value *pgtype.DomainChange) {
	planned.before, planned.after = value.Before != nil, value.After != nil
	if value.Before != nil {
		planned.held = []string{value.Before.BaseType}
	}
	if value.After == nil {
		return
	}
	planned.declared = []string{value.After.BaseType}
	planned.create, planned.comment = pgtype.CreateDomainOf(planned.subject, value.After), value.After.Comment
	if value.Before != nil {
		planned.alter = alterDomain(planned.subject, value)
	}
}

// alterDomain is the ALTER DOMAIN that reaches the declaration, or nil where
// the change states nothing to change to.
//
// A replaced CHECK drops every constraint the catalog names, not only the
// first: a domain holding two against a declaration holding one has one too
// many, and the next comparison would ask for the same replacement again.
func alterDomain(subject objectidentity.ID, value *pgtype.DomainChange) ast.ExtensionPayload {
	alter := &pgtype.AlterDomain{Target: pgtype.TargetOf(subject)}
	if value.Differ(pgtype.DiffCheck) && value.After.Check != "" {
		for _, check := range value.Before.Checks {
			alter.DropChecks = append(alter.DropChecks, check.Name)
		}
		alter.AddCheck = value.After.Check
	}
	if value.Differ(pgtype.DiffDefault) {
		alter.SetDefault = defaultSQL(value.After)
	}
	if value.Differ(pgtype.DiffNotNull) {
		alter.NotNull = new(value.After.NotNull)
	}
	if alter.AddCheck == "" && alter.SetDefault == "" && alter.NotNull == nil {
		return nil
	}
	return alter
}

// defaultSQL is the declared default as SQL, empty where the declaration
// states none: an expression as written, and a literal as a quoted string.
func defaultSQL(domain *pgtype.DesiredDomain) string {
	switch {
	case domain.DefaultExpr != "":
		return domain.DefaultExpr
	case domain.Default != "":
		return sqlident.StringLiteral(domain.Default)
	default:
		return ""
	}
}

func readComposite(planned *change, value *pgtype.CompositeChange) {
	planned.before, planned.after = value.Before != nil, value.After != nil
	if value.Before != nil {
		planned.held = attributeTypes(value.Before.Attributes)
	}
	if value.After == nil {
		return
	}
	planned.declared = attributeTypes(value.After.Attributes)
	planned.create = &pgtype.CreateComposite{Target: pgtype.TargetOf(planned.subject), Attributes: slices.Clone(value.After.Attributes)}
	planned.comment = value.After.Comment
	if len(value.Added) > 0 || len(value.Removed) > 0 {
		// Removals first: an attribute can leave and another arrive under the
		// same name, and dropping first keeps the two from colliding.
		planned.alter = &pgtype.AlterComposite{Target: pgtype.TargetOf(planned.subject), Drop: slices.Clone(value.Removed),
			Add: slices.Clone(value.Added)}
	}
}

func attributeTypes(attributes []pgtype.Attribute) []string {
	types := make([]string, 0, len(attributes))
	for _, attribute := range attributes {
		types = append(types, attribute.Type)
	}
	return types
}

func readRange(planned *change, value *pgtype.RangeChange) {
	planned.before, planned.after = value.Before != nil, value.After != nil
	if value.Before != nil {
		planned.held = []string{value.Before.Subtype}
	}
	if value.After != nil {
		planned.declared = []string{value.After.Subtype}
		planned.create, planned.comment = pgtype.CreateRangeOf(planned.subject, value.After), value.After.Comment
	}
}

// survives reports a change of a type both sides hold.
func (c *change) survives() bool { return c.before && c.after }

// strategy says how the change is carried out.
func (c *change) strategy() string {
	switch {
	case !c.before:
		return "create the type before the columns, routines and types that name it"
	case !c.after:
		return "drop the type with CASCADE after the objects that use it"
	case c.rebuilt:
		return "drop the type and create it again, as nothing changes it in place; the drop is not CASCADE and fails while the type is in use"
	case c.changed && c.alter != nil && c.commented:
		return "change the type in place, then set its comment"
	case c.changed && c.alter != nil:
		return "change the type in place"
	case c.commented:
		return "set the type's comment"
	default:
		return "keep the type, as the change states nothing to change it to"
	}
}

// refuse returns the first creation of a kind the target cannot create,
// beside the refusal, or nil. Of several, it names the lexicographically
// first, as the common refusal does: two gates answering the same schema
// must name the same object, and input order moves whenever a declaration is
// reordered.
func refuse(target string, caps capability.Capabilities, changes []*change) (*change, schemavalidation.Diagnostic) {
	for _, kind := range []struct {
		model schemaext.Kind
		key   capability.Capability
	}{{pgtype.DomainKind, capability.DomainTypes}, {pgtype.CompositeKind, capability.CompositeTypes}, {pgtype.RangeKind, capability.RangeTypes}} {
		if caps.Has(kind.key) {
			continue
		}
		var first *change
		for _, planned := range changes {
			if planned.model == kind.model && !planned.before && (first == nil || planned.subject.Name.Source < first.subject.Name.Source) {
				first = planned
			}
		}
		if first != nil {
			return first, schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Object: first.name,
				Feature: usertypescope.Statement(kind.key), Message: usertypescope.Refusal(target, kind.key, first.subject.Name.Source)}
		}
	}
	return nil, schemavalidation.Diagnostic{}
}

// sequence is the steps of one window in the order they run, and the
// dependencies that keep that order.
type sequence struct {
	steps []plangraph.Step[featureplan.Operation]
	edges []plangraph.Dependency
}

// add appends a step after the steps before it and after its change's
// earlier steps, which may be in another window, and names it on its change.
// The windows order a change's steps in a host, and the edge says so to the
// plan graph too, which refuses two unordered effects on one type.
func (s *sequence) add(planned *change, step plangraph.Step[featureplan.Operation]) {
	var previous plangraph.StepID
	if count := len(s.steps); count > 0 {
		previous = s.steps[count-1].ID
		s.edges = append(s.edges, plangraph.Dependency{Before: previous, After: step.ID})
	}
	if count := len(planned.steps); count > 0 && planned.steps[count-1] != previous {
		s.edges = append(s.edges, plangraph.Dependency{Before: planned.steps[count-1], After: step.ID})
	}
	s.steps = append(s.steps, step)
	planned.steps = append(planned.steps, step.ID)
}

// windows are the plan's three sequences: the type creation window, the type
// removal window, and the dependent window the comments of surviving types
// join.
type windows struct {
	creation, removal, comments sequence
}

func (w windows) contribution() plangraph.Contribution[featureplan.Operation] {
	return plangraph.Contribution[featureplan.Operation]{Owner: pgtype.Owner,
		Steps:        slices.Concat(w.creation.steps, w.removal.steps, w.comments.steps),
		Dependencies: slices.Concat(w.creation.edges, w.removal.edges, w.comments.edges)}
}

// planChanges orders the steps of a batch of changes.
func planChanges(changes []*change) windows {
	markRebuilt(changes)
	var plan windows
	// The drops of rebuilt types run against the database as it stands, so
	// they are ordered by what the current definitions name.
	for _, planned := range dropOrder(changes) {
		plan.creation.add(planned, step(planned, "drop", plangraph.Drop, featureplan.PhaseType, &pgtype.Drop{Target: pgtype.TargetOf(planned.subject)},
			"Recreate modified "+pgtype.Label(planned.subject)+"; drop is non-CASCADE and fails if the "+rebuildNoun(planned)+" is in use"))
	}
	creations := slices.Concat(byKind(changes, func(c *change) bool { return !c.before }, pgtype.DomainKind, pgtype.RangeKind, pgtype.CompositeKind),
		byKind(changes, func(c *change) bool { return c.rebuilt }, pgtype.DomainKind, pgtype.CompositeKind, pgtype.RangeKind))
	for _, planned := range createOrder(creations) {
		addCreation(&plan.creation, planned)
	}
	for _, planned := range byKind(changes, func(c *change) bool { return c.survives() && c.changed && !c.rebuilt && c.alter != nil },
		pgtype.DomainKind, pgtype.CompositeKind) {
		plan.creation.add(planned, step(planned, "alter", plangraph.Alter, featureplan.PhaseType, planned.alter))
	}
	for _, planned := range byKind(changes, func(c *change) bool { return !c.after }, pgtype.DomainKind, pgtype.CompositeKind, pgtype.RangeKind) {
		plan.removal.add(planned, step(planned, "drop", plangraph.Drop, featureplan.PhaseType,
			&pgtype.Drop{Target: pgtype.TargetOf(planned.subject), Cascade: true},
			"WARNING: Make sure no columns use this "+pgtype.Label(planned.subject)+"!"))
	}
	for _, planned := range byKind(changes, func(c *change) bool { return c.survives() && c.commented && !c.rebuilt },
		pgtype.CompositeKind, pgtype.DomainKind, pgtype.RangeKind) {
		// Surrounding whitespace is not part of a comment, which is how the
		// comparison found this one different.
		plan.comments.add(planned, step(planned, "comment", plangraph.Alter, featureplan.PhaseDependent,
			&pgtype.Comment{Target: pgtype.TargetOf(planned.subject), Comment: strings.TrimSpace(planned.comment)}))
	}
	return plan
}

// planDeclarations orders the creation steps of declared types.
func planDeclarations(changes []*change) windows {
	var plan windows
	for _, planned := range createOrder(byKind(changes, func(*change) bool { return true }, pgtype.DomainKind, pgtype.RangeKind, pgtype.CompositeKind)) {
		addCreation(&plan.creation, planned)
	}
	return plan
}

// addCreation adds a type's creation and, where it declares one, its comment.
func addCreation(window *sequence, planned *change) {
	window.add(planned, step(planned, "create", plangraph.Create, featureplan.PhaseType, planned.create))
	if planned.comment != "" {
		window.add(planned, step(planned, "comment", plangraph.Alter, featureplan.PhaseType,
			&pgtype.Comment{Target: pgtype.TargetOf(planned.subject), Comment: planned.comment}))
	}
}

// rebuildNoun is how a rebuild note names the type it drops.
func rebuildNoun(planned *change) string {
	if planned.model == pgtype.DomainKind {
		return "domain"
	}
	return "type"
}

// step is one operation of a change.
func step(planned *change, suffix string, action plangraph.Action, phase featureplan.Phase, payload ast.ExtensionPayload,
	notes ...string,
) plangraph.Step[featureplan.Operation] {
	var impact schemaext.Effect
	if source, ok := payload.(schemaext.EffectSource); ok {
		impact = source.Effect()
	}
	return plangraph.Step[featureplan.Operation]{
		ID:          plangraph.StepID{Owner: pgtype.Owner, Name: fmt.Sprintf("type/%06d/%s", planned.index, suffix)},
		Payload:     featureplan.Operation{Role: ast.StatementExtension, Phase: phase, Payload: payload, Notes: notes},
		Effects:     []plangraph.Effect{{Subject: pgtype.Subject(planned.subject), Action: action}},
		Transaction: plangraph.TransactionAllowed, Impact: impact,
	}
}

// byKind selects the changes keep accepts, kind by kind in the order given,
// each kind ordered by name.
func byKind(changes []*change, keep func(*change) bool, kinds ...schemaext.Kind) []*change {
	var selected []*change
	for _, kind := range kinds {
		var ofKind []*change
		for _, planned := range changes {
			if planned.model == kind && keep(planned) {
				ofKind = append(ofKind, planned)
			}
		}
		slices.SortStableFunc(ofKind, func(a, b *change) int { return cmp.Compare(a.name, b.name) })
		selected = append(selected, ofKind...)
	}
	return selected
}

// markRebuilt decides which surviving types are dropped and created again:
// the ones whose change has no in-place form, and, closed over the
// references between them, the changed ones whose current definition names a
// rebuilt type. The closure runs to a fixpoint because references chain: a
// composite over a composite over a rebuilt domain is two steps from the
// cause.
func markRebuilt(changes []*change) {
	rebuilt := make(map[string]bool)
	for _, planned := range changes {
		if planned.survives() && planned.changed && !planned.inPlace {
			rebuilt[bareTypeName(planned.name)] = true
		}
	}
	for grew := true; grew; {
		grew = false
		for _, planned := range changes {
			name := bareTypeName(planned.name)
			if !planned.survives() || !planned.changed || rebuilt[name] ||
				!slices.ContainsFunc(planned.held, func(spelling string) bool { return rebuilt[bareTypeName(spelling)] }) {
				continue
			}
			rebuilt[name], grew = true, true
		}
	}
	for _, planned := range changes {
		planned.rebuilt = planned.survives() && planned.changed && rebuilt[bareTypeName(planned.name)]
	}
}

// bareTypeName reduces a type spelling to the name references are matched
// on: lower-cased, unqualified, and without the array or length decoration a
// catalog spelling may carry.
func bareTypeName(name string) string {
	trimmed := strings.ToLower(strings.TrimSpace(name))
	trimmed = strings.TrimSuffix(trimmed, "[]")
	if open := strings.IndexByte(trimmed, '('); open >= 0 {
		trimmed = trimmed[:open]
	}
	if dot := strings.LastIndexByte(trimmed, '.'); dot >= 0 {
		trimmed = trimmed[dot+1:]
	}
	return strings.TrimSpace(strings.Trim(trimmed, `"`))
}

// dropOrder orders the rebuilt types for their drops: one that names another
// first, by the definitions the database holds.
func dropOrder(changes []*change) []*change {
	rebuilt := byKind(changes, func(c *change) bool { return c.rebuilt }, pgtype.DomainKind, pgtype.CompositeKind, pgtype.RangeKind)
	types := make([]deporder.UserType, 0, len(rebuilt))
	for _, planned := range rebuilt {
		types = append(types, deporder.UserType{Name: planned.name, References: planned.held})
	}
	return reorder(rebuilt, deporder.UserTypesForDrop(types))
}

// createOrder orders creations: one another names first, by the
// declarations.
func createOrder(creations []*change) []*change {
	types := make([]deporder.UserType, 0, len(creations))
	for _, planned := range creations {
		types = append(types, deporder.UserType{Name: planned.name, References: planned.declared})
	}
	return reorder(creations, deporder.UserTypesForCreate(types))
}

// reorder returns changes in the order names lists them. Two changes of one
// name, which PostgreSQL refuses in one namespace, keep their input order
// side by side rather than losing one.
func reorder(changes []*change, names []string) []*change {
	byName := make(map[string][]*change, len(changes))
	for _, planned := range changes {
		byName[planned.name] = append(byName[planned.name], planned)
	}
	ordered := make([]*change, 0, len(changes))
	for _, name := range names {
		ordered = append(ordered, byName[name]...)
		delete(byName, name)
	}
	return ordered
}
