package featureselect

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ErrPartialScope is a scope that selects some of the tables a feature
// object binds and not the others. Comparing the object would change its
// bindings on the tables outside the scope, and leaving it out would hide the
// bindings inside it, so the selection is refused (ADR 0020).
var ErrPartialScope = errors.New("the scope selects part of a feature object")

// RelationRuntime captures the tables feature objects bind, through their
// owners' relation discovery, and says which kinds have one.
type RelationRuntime interface {
	schemaext.RelationRuntime
	RelationKinds(target string, representation schemaext.Representation) ([]schemaext.Kind, error)
}

// Side is one captured state whose standalone feature objects may bind
// tables: the desired or the current side of a comparison.
type Side struct {
	Representation schemaext.Representation
	Objects        schemaext.Objects
	Coverage       schemaext.Coverage
}

// Bindings records the tables each standalone feature object binds, as its
// owner's relation discovery reports them, over every side it was captured
// from. A standalone object belongs to no table, such as a SQL Server security
// policy, which binds predicates to several. The zero value records nothing,
// and selecting with it keeps every standalone object, as [Tables] does.
type Bindings struct {
	objects map[objectidentity.Key]binding
}

type binding struct {
	ref    objectidentity.ID
	tables []objectidentity.ID
}

// CaptureBindings asks the owners of the standalone objects on sides which
// tables each binds. Only kinds with relation discovery on target are asked;
// the others keep no record, so their selection stays their own. A table one
// side binds counts for the object on every side: a scope that keeps the
// declaration and drops the observation would plan a creation of a policy
// that exists.
//
// The tables are taken from each owner's receipt even where the receipt is
// not complete: an incomplete receipt leaves out what an expression calls,
// never a table the object binds. Sides without a standalone object need no
// runtime; a nil runtime is refused when one is present, since the selection
// could not be decided. Any failure returns no bindings.
func CaptureBindings(ctx context.Context, runtime RelationRuntime, target string, semantics identifier.Semantics, sides ...Side) (Bindings, error) {
	bindings := Bindings{objects: make(map[objectidentity.Key]binding)}
	for _, side := range sides {
		objects, err := side.Objects.Select(standalone).All()
		if err != nil {
			return Bindings{}, err
		}
		if len(objects) == 0 {
			continue
		}
		if runtime == nil {
			return Bindings{}, fmt.Errorf("%w: no runtime can say which tables the standalone feature objects bind", ptaherr.ErrUnsupportedFeature)
		}
		kinds, err := runtime.RelationKinds(target, side.Representation)
		if err != nil {
			return Bindings{}, err
		}
		for _, kind := range kinds {
			values := make([]schemaext.RelationValue, 0)
			for _, object := range objects {
				if object.Value.Kind() == kind {
					values = append(values, schemaext.RelationValue{Value: object.Value, Subject: schemaext.RelationSubject{
						Kind: kind, Placement: schemaext.ObjectPlacement, Subject: object.Ref}})
				}
			}
			if len(values) == 0 {
				continue
			}
			snapshot, err := runtime.CaptureRelations(ctx, schemaext.RelationRequest{Target: target, Representation: side.Representation,
				Identifiers: semantics, Values: values, Coverage: side.Coverage.SelectKinds([]schemaext.Kind{kind})})
			if err != nil {
				return Bindings{}, err
			}
			for _, record := range snapshot.Records() {
				bindings.add(record)
			}
		}
	}
	return bindings, ctx.Err()
}

func (b Bindings) add(record schemaext.ValueRelations) {
	key := record.Subject.Subject.Key()
	found := b.objects[key]
	if found.ref.Kind == "" {
		found.ref = record.Subject.Subject
	}
	for _, dependency := range record.Dependencies {
		if dependency.Kind == objectidentity.KindTable && !slices.ContainsFunc(found.tables, func(table objectidentity.ID) bool {
			return table.Key() == dependency.Key()
		}) {
			found.tables = append(found.tables, dependency)
		}
	}
	b.objects[key] = found
}

// Select is [Tables] for objects that bind tables without belonging to one.
// A standalone object whose every bound table keepTable keeps is kept whole,
// one with none is left out with its knowledge, and one with some is refused
// with [ErrPartialScope], naming the tables on each side of the scope. A
// standalone object with no recorded table is kept, as [Tables] keeps it.
//
// named lists the kinds the caller selects by their own identity, such as a
// continuous aggregate an include names. For them the tables decide only the
// refusal of a split; keeping or leaving one out stays the caller's
// selection. Inputs remain unchanged; a refusal returns no selection.
func (b Bindings) Select(objects schemaext.Objects, coverage schemaext.Coverage, keepTable func(schema, table string) bool,
	named ...schemaext.Kind,
) (schemaext.Objects, schemaext.Coverage, error) {
	decided := make(map[objectidentity.Key]bool, len(b.objects))
	var problems []error
	for key, bound := range b.objects {
		var inside, outside []string
		for _, table := range bound.tables {
			name := table.Schema.Source + "." + table.Name.Source
			if keepTable(table.Schema.Source, table.Name.Source) {
				inside = append(inside, name)
			} else {
				outside = append(outside, name)
			}
		}
		switch {
		case len(inside) != 0 && len(outside) != 0:
			slices.Sort(inside)
			slices.Sort(outside)
			problems = append(problems, fmt.Errorf("%w: %s %s.%s binds %s, which the scope selects, and %s, which it does not; "+
				"select every table it binds or none of them", ErrPartialScope, bound.ref.Kind, bound.ref.Schema.Source, bound.ref.Name.Source,
				strings.Join(inside, ", "), strings.Join(outside, ", ")))
		case slices.Contains(named, schemaext.Kind(bound.ref.Kind)):
		case len(inside) == 0 && len(outside) != 0:
			decided[key] = false
		}
	}
	if len(problems) != 0 {
		slices.SortFunc(problems, func(a, b error) int { return strings.Compare(a.Error(), b.Error()) })
		return schemaext.Objects{}, schemaext.Coverage{}, errors.Join(problems...)
	}
	keep := func(ref objectidentity.ID) bool {
		if kept, found := decided[ref.Key()]; found {
			return kept
		}
		return keepOwned(ref, keepTable)
	}
	return objects.Select(keep), coverage.SelectSubjects(keep), nil
}

// standalone reports an object that belongs to no table.
func standalone(ref objectidentity.ID) bool {
	return ref.Parent.Empty() && ref.Kind != objectidentity.KindTable
}
