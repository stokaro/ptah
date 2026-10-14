package oratype

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
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/deporder"
	"ptah.run/internal/typeorder"
	"ptah.run/internal/usertypescope"
)

// PlanService plans user type changes and the types a whole-schema render
// declares. Its zero value is ready for concurrent use.
//
// Every statement joins [featureplan.PhaseType]: a creation before the
// routines and tables that may name the type, and a drop after the tables
// whose columns were typed by it, since Oracle refuses to drop a type a column
// still uses. A change orders its statements this way: the domains it
// creates, then the object types it creates, then the object types it
// replaces, each kind by qualified name; and in the removal window the domains
// it drops, then the object types.
//
// An object type whose attributes changed is replaced in place by CREATE OR
// REPLACE TYPE, which Oracle refuses while a column uses the type.
//
// A domain whose base type or NOT NULL changed is refused by name. Measured on
// 23.26.3.0.0, ALTER DOMAIN changes a domain's DISPLAY, ORDER and annotations
// and nothing else, DROP DOMAIN answers ORA-11502 while a column uses the
// domain, and DROP DOMAIN ... FORCE takes the domain off those columns. No
// statement the owner can plan makes the change, and planning none would
// report the declaration applied while the domain kept its old definition.
//
// A type the target cannot create is refused, naming the scope that keeps the
// declaration to the targets that host it: Oracle 21 has no CREATE DOMAIN.
type PlanService struct{}

// PlanFeatures returns one receipt per change in input order, or a completed
// refusal with no usable prefix. A user type belongs to no table, so a request
// that names parent kinds is refused.
func (PlanService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := checkTarget(ctx, request.Target, "planning"); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: a user type belongs to no table", schemaext.ErrInvalidValue)
	}
	changes := make([]*planned, 0, len(request.Changes))
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		change, err := plannedChange(index, record)
		if err != nil {
			return featureplan.Result{}, err
		}
		changes = append(changes, change)
	}
	if refused, problem := refuse(request.Target, request.Capabilities, changes); refused != nil {
		problem.Kind = string(refused.changeKind)
		return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: problem, Change: new(refused.index)}}}, nil
	}
	var creation, removal sequence
	for _, change := range slices.Concat(byName(changes, DomainKind, isCreation), byName(changes, ObjectTypeKind, isCreation),
		byName(changes, ObjectTypeKind, isReplacement)) {
		creation.add(change, change.step(plangraph.Create))
	}
	for _, change := range slices.Concat(byName(changes, DomainKind, isRemoval), byName(changes, ObjectTypeKind, isRemoval)) {
		removal.add(change, change.step(plangraph.Drop))
	}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(changes))}
	for i, change := range changes {
		result.Changes[i] = featureplan.ChangePlan{Subject: change.subject, Kind: change.changeKind, Strategy: change.strategy(), Steps: change.steps}
	}
	if contribution := contributionOf(creation, removal); len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// PlanDeclarations creates each declared type for a whole-schema render: the
// domains and then the object types, each kind by qualified name, except
// that a type another one names comes first.
func (PlanService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := checkTarget(ctx, request.Target, "planning"); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	changes := make([]*planned, 0, len(request.Objects))
	for index, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		var value schemaext.ChangeValue
		switch declared := object.Value.(type) {
		case *DesiredDomain:
			value = &DomainChange{After: declared}
		case *DesiredObjectType:
			value = &ObjectTypeChange{After: declared}
		default:
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a declared Oracle user type, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		change, err := plannedChange(index, schemaext.ChangeRecord{Subject: object.Ref, Value: value})
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		changes = append(changes, change)
	}
	if refused, problem := refuse(request.Target, request.Capabilities, changes); refused != nil {
		// A declaration's refusal names the declared model, which is what the
		// render was handed, not the change it was planned as.
		problem.Kind = string(refused.model)
		return featureplan.DeclarationResult{Complete: true,
			Diagnostics: []featureplan.DeclarationDiagnostic{{Problem: problem, Object: new(refused.index)}}}, nil
	}
	var creation sequence
	for _, change := range createOrder(slices.Concat(byName(changes, DomainKind, isCreation), byName(changes, ObjectTypeKind, isCreation)), request.Types) {
		creation.add(change, change.step(plangraph.Create))
	}
	result := featureplan.DeclarationResult{Complete: true, Declarations: make([]featureplan.DeclarationPlan, len(changes))}
	for i, change := range changes {
		result.Declarations[i] = featureplan.DeclarationPlan{Subject: change.subject, Strategy: change.strategy(), Steps: change.steps}
	}
	if contribution := contributionOf(creation, sequence{}); len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// planned is one input change as the plan reads it.
type planned struct {
	index      int
	subject    objectidentity.ID
	model      schemaext.Kind
	changeKind schemaext.Kind
	// before and after report the two operands.
	before, after bool
	// differs is what the comparison found different in a type both sides
	// hold.
	differs []string
	// statement is the change's creation or replacement, nil for a drop and
	// for a domain change, which the plan refuses.
	statement ast.ExtensionPayload
	// declared names the type spellings the declaration names.
	declared []string
	// steps names the change's operations in plan order.
	steps []plangraph.StepID
}

// plannedChange reads one change, refusing an invalid one and one of another
// model than its subject names.
func plannedChange(index int, record schemaext.ChangeRecord) (*planned, error) {
	if err := ValidateRef(record.Subject); err != nil {
		return nil, err
	}
	change := &planned{index: index, subject: record.Subject, model: schemaext.Kind(record.Subject.Kind)}
	var matching schemaext.Kind
	switch value := record.Value.(type) {
	case *DomainChange:
		if err := value.Validate(); err != nil {
			return nil, err
		}
		matching, change.before, change.after = DomainKind, value.Before != nil, value.After != nil
		change.differs = value.Differs
		if value.After != nil && value.Before == nil {
			change.statement, change.declared = CreateDomainOf(record.Subject, value.After), []string{value.After.BaseType}
		}
	case *ObjectTypeChange:
		if err := value.Validate(); err != nil {
			return nil, err
		}
		matching, change.before, change.after = ObjectTypeKind, value.Before != nil, value.After != nil
		if value.After != nil {
			change.statement = ReplaceObjectTypeOf(record.Subject, value.After, value.Before != nil)
			change.declared = attributeTypes(value.After.Attributes)
		}
	default:
		return nil, fmt.Errorf("%w: unexpected Oracle user type change %T", schemaext.ErrInvalidValue, record.Value)
	}
	if matching != change.model {
		return nil, fmt.Errorf("%w: a %s change for %s", schemaext.ErrInvalidValue, matching, record.Subject)
	}
	change.changeKind = record.Value.Kind()
	return change, nil
}

func isCreation(change *planned) bool    { return !change.before }
func isReplacement(change *planned) bool { return change.before && change.after }
func isRemoval(change *planned) bool     { return !change.after }

// strategy says how the change is carried out.
func (c *planned) strategy() string {
	switch {
	case !c.before:
		return "create the type before the routines and tables that may name it"
	case !c.after:
		return "drop the type after the tables whose columns were typed by it; the drop fails while a column still uses it"
	default:
		return "replace the object type with CREATE OR REPLACE TYPE, which Oracle refuses while a column uses it"
	}
}

// step is the change's one operation: its statement, or its drop.
func (c *planned) step(action plangraph.Action) plangraph.Step[featureplan.Operation] {
	payload := c.statement
	if action == plangraph.Drop {
		payload = &Drop{Target: TargetOf(c.subject)}
	} else if c.before {
		action = plangraph.Alter
	}
	var impact schemaext.Effect
	if source, ok := payload.(schemaext.EffectSource); ok {
		impact = source.Effect()
	}
	return plangraph.Step[featureplan.Operation]{
		// Zero-padded, so a scheduler that orders independent steps by name
		// keeps the order of the changes.
		ID:          plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("type/%06d/%s", c.index, action)},
		Payload:     featureplan.Operation{Role: ast.StatementExtension, Phase: featureplan.PhaseType, Payload: payload},
		Effects:     []plangraph.Effect{{Subject: Subject(c.subject), Action: action}},
		Transaction: plangraph.TransactionAllowed, Impact: impact,
	}
}

// refuse returns the first change the target cannot make, beside the
// refusal, or nil: a creation of a kind the target cannot create, and then a
// change of a domain's base type or NOT NULL. Of several creations, it names
// the lexicographically first, domains before object types, as the common
// refusal does: two gates answering the same schema must name the same object.
func refuse(target string, caps capability.Capabilities, changes []*planned) (*planned, schemavalidation.Diagnostic) {
	if refused, problem := refuseCreation(target, caps, changes); refused != nil {
		return refused, problem
	}
	return refuseDomainChange(changes)
}

// refuseDomainChange returns the lexicographically first domain whose base
// type or NOT NULL changed, beside its refusal, or nil.
func refuseDomainChange(changes []*planned) (*planned, schemavalidation.Diagnostic) {
	var first *planned
	for _, change := range changes {
		if change.model == DomainKind && change.before && change.after &&
			(first == nil || TargetOf(change.subject).QualifiedName() < TargetOf(first.subject).QualifiedName()) {
			first = change
		}
	}
	if first == nil {
		return nil, schemavalidation.Diagnostic{}
	}
	name := TargetOf(first.subject).QualifiedName()
	return first, schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Object: name, Feature: "ALTER DOMAIN",
		Message: fmt.Sprintf("oracle: domain %s: Oracle cannot change %s in place, since ALTER DOMAIN changes only "+
			"a domain's DISPLAY, ORDER and annotations; declare a domain under a new name and type the columns by it, "+
			"or drop and create the domain by hand", name, domainDifferences(first.differs))}
}

// domainDifferences names what changed in a domain the way a refusal says it.
func domainDifferences(differs []string) string {
	named := make([]string, 0, len(differs))
	for _, difference := range differs {
		switch difference {
		case DiffBaseType:
			named = append(named, "its base type")
		case DiffNotNull:
			named = append(named, "its NOT NULL")
		}
	}
	if len(named) == 0 {
		return "its definition"
	}
	return strings.Join(named, " and ")
}

// refuseCreation returns the first creation of a kind the target cannot
// create, beside the refusal, or nil.
func refuseCreation(target string, caps capability.Capabilities, changes []*planned) (*planned, schemavalidation.Diagnostic) {
	for _, kind := range []struct {
		model schemaext.Kind
		key   capability.Capability
	}{{DomainKind, capability.DomainTypes}, {ObjectTypeKind, capability.CompositeTypes}} {
		if caps.Has(kind.key) {
			continue
		}
		var first *planned
		for _, change := range changes {
			if change.model == kind.model && !change.before && (first == nil || change.subject.Name.Source < first.subject.Name.Source) {
				first = change
			}
		}
		if first != nil {
			return first, schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Object: TargetOf(first.subject).QualifiedName(),
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

// add appends a step after the step before it and names it on its change.
func (s *sequence) add(change *planned, step plangraph.Step[featureplan.Operation]) {
	if count := len(s.steps); count > 0 {
		s.edges = append(s.edges, plangraph.Dependency{Before: s.steps[count-1].ID, After: step.ID})
	}
	s.steps = append(s.steps, step)
	change.steps = append(change.steps, step.ID)
}

func contributionOf(creation, removal sequence) plangraph.Contribution[featureplan.Operation] {
	return plangraph.Contribution[featureplan.Operation]{Owner: Owner,
		Steps: slices.Concat(creation.steps, removal.steps), Dependencies: slices.Concat(creation.edges, removal.edges)}
}

// byName selects the changes of kind keep accepts, ordered by qualified name.
func byName(changes []*planned, kind schemaext.Kind, keep func(*planned) bool) []*planned {
	var selected []*planned
	for _, change := range changes {
		if change.model == kind && keep(change) {
			selected = append(selected, change)
		}
	}
	slices.SortStableFunc(selected, func(a, b *planned) int {
		return cmp.Compare(TargetOf(a.subject).QualifiedName(), TargetOf(b.subject).QualifiedName())
	})
	return selected
}

// createOrder orders creations so a type another one names comes first, by
// the declarations, and keeps the given order otherwise.
func createOrder(creations []*planned, definitions []schemaext.DefinedType) []*planned {
	types := make([]deporder.UserType, 0, len(creations))
	byName := make(map[string][]*planned, len(creations))
	for _, change := range creations {
		name := TargetOf(change.subject).QualifiedName()
		types = append(types, deporder.UserType{Name: name, References: change.declared})
		byName[name] = append(byName[name], change)
	}
	ordered := make([]*planned, 0, len(creations))
	for _, name := range deporder.UserTypesForCreate(typeorder.Vocabulary(types, definitions)) {
		ordered = append(ordered, byName[name]...)
		delete(byName, name)
	}
	return ordered
}
