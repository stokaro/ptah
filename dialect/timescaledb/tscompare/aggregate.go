package tscompare

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
)

// AggregateService compares continuous aggregates by schema and name. Its zero
// value is ready for concurrent use.
//
// The identity is resolved under the request's identifier rules rather than
// compared as captured: a read scoped to one schema reports an aggregate
// unqualified, and a document that names the schema reports it qualified.
// Comparing the two spellings reported an addition and a removal for one
// unchanged aggregate, and the plan created it before dropping it.
//
// The body is compared only where a server normalized the declaration.
// TimescaleDB stores a rewritten definition, so the declared text and the
// catalog's differ for every aggregate that has not changed; comparing them
// directly would plan a drop and a create on every run, and each one discards
// the materialized history the aggregate exists to keep. Without a
// normalization only the option is compared (stokaro/ptah#1026).
type AggregateService struct{}

type aggregatePair struct {
	desiredRef, currentRef objectidentity.ID
	desired                *tsschema.DesiredContinuousAggregate
	current                *tsschema.ObservedContinuousAggregate
}

// CompareObjects preserves observed aggregates a source cannot describe and
// never treats an unread namespace as empty: a declared aggregate whose
// presence was not established is undecided, and an unread namespace plans no
// removal because nothing in it was observed. Inputs remain unchanged.
func (AggregateService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: TimescaleDB comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{tsschema.ContinuousAggregateKind}) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: unsupported continuous aggregate comparison kinds", schemaext.ErrInvalidValue)
	}
	pairs, err := aggregatePairs(ctx, request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := refuseRelationNames(request, pairs); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: schemaext.ObjectState{Objects: request.Desired.Objects}}
	var adopted []schemaext.SubjectCoverage
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		record, err := compareAggregate(request, pair, &result)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		if record != nil {
			adopted = append(adopted, *record)
		}
	}
	result.Desired.Coverage, err = aggregateCoverage(request, adopted)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, nil
}

func compareAggregate(request schemaext.ObjectComparisonRequest, pair aggregatePair, result *schemaext.ObjectComparisonResult) (*schemaext.SubjectCoverage, error) {
	subject := pair.desiredRef
	if pair.desired == nil {
		subject = pair.currentRef
	}
	if pair.desired != nil && aggregateLimited(request.Desired.Coverage, pair.desiredRef) {
		undecidedObject(result, subject, "the desired source cannot describe the complete continuous aggregate")
		return nil, nil
	}
	currentKnowledge := request.Current.Coverage.Lookup(tsschema.ContinuousAggregateKind, subject)
	if pair.current != nil && aggregateLimited(request.Current.Coverage, pair.currentRef) ||
		pair.current == nil && unknown(currentKnowledge) {
		if pair.desired != nil {
			undecidedObject(result, subject, "the current continuous aggregate or its absence was not established: "+currentKnowledge.Reason)
		}
		return nil, nil
	}
	if pair.desired == nil {
		desiredKnowledge := request.Desired.Coverage.Lookup(tsschema.ContinuousAggregateKind, pair.currentRef)
		if unknown(desiredKnowledge) {
			// A description that could not express a continuous aggregate has
			// not asked for one to be dropped; the drop it would plan discards a
			// materialization no rollback rebuilds.
			declared, err := pair.current.Desired()
			if err != nil {
				return nil, err
			}
			result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: pair.currentRef, Value: declared})
			if err != nil {
				return nil, err
			}
			return &schemaext.SubjectCoverage{Kind: tsschema.ContinuousAggregateKind, Subject: pair.currentRef,
				Knowledge: schemaext.Knowledge{State: schemaext.Complete}}, nil
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.currentRef,
			Value: &tsdiff.ContinuousAggregate{Before: pair.current}})
		return nil, nil
	}
	if pair.current == nil {
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.desiredRef,
			Value: &tsdiff.ContinuousAggregate{After: pair.desired}})
		return nil, nil
	}
	if SameAggregate(pair.desired, pair.current) {
		return nil, nil
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.desiredRef,
		Value: &tsdiff.ContinuousAggregate{Before: pair.current, After: pair.desired}})
	return nil, nil
}

// SameAggregate reports whether a declaration asks for the aggregate the
// observation describes.
//
// A declaration that did not write the option always matches: it takes the
// server's own default, which is not a constant across releases. The body is
// compared only when a server normalized the declaration; the catalog text is
// already in that server's spelling, so the two are compared after trimming the
// catalog's own punctuation.
func SameAggregate(declared *tsschema.DesiredContinuousAggregate, observed *tsschema.ObservedContinuousAggregate) bool {
	if declared.MaterializedOnly != nil && (observed.MaterializedOnly == nil || *declared.MaterializedOnly != *observed.MaterializedOnly) {
		return false
	}
	if declared.Normalized == nil {
		return true
	}
	return tsschema.FoldBody(declared.Normalized.Body) == tsschema.FoldBody(observed.Definition)
}

// refuseRelationNames refuses a declared table, view or materialized view
// whose name a live continuous aggregate occupies. An aggregate holds its name
// as a relation -- pg_class reports relkind 'v' -- so the statement creating
// the declaration would answer `relation ... already exists` halfway through
// the script. Refusing before planning says which object holds the name. The
// declaration this is most often meant for is an aggregate written as a
// materialized view, which is the natural mistake: PostgreSQL calls it one.
func refuseRelationNames(request schemaext.ObjectComparisonRequest, pairs []aggregatePair) error {
	builder := objectidentity.NewBuilder(request.Identifiers)
	type declaredRelation struct {
		kind string
		ref  objectidentity.ID
	}
	declared := make(map[objectidentity.Key]declaredRelation)
	occupy := func(kind string, ref objectidentity.ID) {
		key := builder.SchemaScopedParts(objectidentity.Kind(tsschema.ContinuousAggregateKind), tsschema.AuthoredSchema(ref), ref.Name.Source).Key()
		if _, found := declared[key]; !found {
			declared[key] = declaredRelation{kind: kind, ref: ref}
		}
	}
	for _, parent := range request.Parents {
		if parent.Desired {
			occupy("table", parent.Subject)
		}
	}
	for _, ref := range request.DeclaredRelations {
		kind := "view"
		if ref.Kind == objectidentity.KindMatView {
			kind = "materialized view"
		}
		occupy(kind, ref)
	}
	var problems []error
	for _, pair := range pairs {
		if pair.current == nil {
			continue
		}
		relation, found := declared[rekey(request.Identifiers, pair.currentRef).Key()]
		if !found {
			continue
		}
		problems = append(problems, fmt.Errorf("%w: declared %s %q is a TimescaleDB continuous aggregate on this server, materializing %s.%s: "+
			"applying this declaration would create a relation the name already belongs to; declare it as a continuous "+
			"aggregate instead, rename the %s, or drop the aggregate with DROP MATERIALIZED VIEW",
			ptaherr.ErrInvalidSchemaDiff, relation.kind, tsschema.QualifiedName(relation.ref), pair.current.HypertableSchema, pair.current.HypertableName, relation.kind))
	}
	return errors.Join(problems...)
}

func aggregatePairs(ctx context.Context, request schemaext.ObjectComparisonRequest) ([]aggregatePair, error) {
	pairs := make(map[objectidentity.Key]*aggregatePair)
	for _, source := range []struct {
		state     schemaext.ObjectState
		direction schemaext.Representation
	}{{request.Desired, schemaext.Desired}, {request.Current, schemaext.Observed}} {
		objects, err := source.state.Objects.All()
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := tsschema.ValidateContinuousAggregateRef(object.Ref); err != nil {
				return nil, err
			}
			key := rekey(request.Identifiers, object.Ref).Key()
			pair := pairs[key]
			if pair == nil {
				pair = &aggregatePair{}
				pairs[key] = pair
			}
			if err := capturePair(pair, object, source.direction); err != nil {
				return nil, err
			}
		}
	}
	result := make([]aggregatePair, 0, len(pairs))
	for _, key := range slices.SortedFunc(maps.Keys(pairs), func(a, b objectidentity.Key) int {
		return schemaext.CompareRefs(pairRef(*pairs[a]), pairRef(*pairs[b]))
	}) {
		result = append(result, *pairs[key])
	}
	return result, nil
}

func capturePair(pair *aggregatePair, object schemaext.Object, direction schemaext.Representation) error {
	switch value := object.Value.(type) {
	case *tsschema.DesiredContinuousAggregate:
		if direction != schemaext.Desired || pair.desired != nil {
			return fmt.Errorf("%w: duplicate or misplaced continuous aggregate declaration %s", schemaext.ErrInvalidValue, object.Ref)
		}
		if err := tsschema.ValidateDesiredContinuousAggregate(value); err != nil {
			return err
		}
		pair.desiredRef, pair.desired = object.Ref, value
	case *tsschema.ObservedContinuousAggregate:
		if direction != schemaext.Observed || pair.current != nil {
			return fmt.Errorf("%w: duplicate or misplaced continuous aggregate observation %s", schemaext.ErrInvalidValue, object.Ref)
		}
		if err := tsschema.ValidateObservedContinuousAggregate(value); err != nil {
			return err
		}
		pair.currentRef, pair.current = object.Ref, value
	default:
		return fmt.Errorf("%w: unexpected continuous aggregate operand %T", schemaext.ErrInvalidValue, object.Value)
	}
	return nil
}

func pairRef(pair aggregatePair) objectidentity.ID {
	if pair.desired != nil {
		return pair.desiredRef
	}
	return pair.currentRef
}

// rekey resolves a captured identity under the comparison's identifier rules.
// A defaulted schema takes the target's own default rather than the one the
// source assumed, which is what joins an unqualified declaration to the same
// aggregate a read reported in the connection's schema.
func rekey(semantics identifier.Semantics, ref objectidentity.ID) objectidentity.ID {
	return tsschema.ContinuousAggregateRefWith(semantics, tsschema.AuthoredSchema(ref), ref.Name.Source)
}

// aggregateCoverage keeps the source's own claims and records the observed
// aggregates it adopted, so a later stage knows they are known rather than
// declared.
func aggregateCoverage(request schemaext.ObjectComparisonRequest, adopted []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	kinds := request.Desired.Coverage.KindRecords()
	if len(kinds) == 0 {
		enrolled, err := tsschema.Coverage(tsschema.ContinuousAggregateKind, schemaext.Desired,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the desired source did not describe continuous aggregates"}, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		kinds = enrolled.KindRecords()
	}
	records := make(map[objectidentity.Key]schemaext.SubjectCoverage)
	for _, record := range request.Desired.Coverage.SubjectRecords() {
		records[record.Subject.Key()] = record
	}
	for _, record := range adopted {
		records[record.Subject.Key()] = record
	}
	return schemaext.NewCoverage(schemaext.Desired, kinds, slices.Collect(maps.Values(records)))
}

func aggregateLimited(coverage schemaext.Coverage, ref objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(tsschema.ContinuousAggregateKind, ref)
	return found && unknown(knowledge)
}

func undecidedObject(result *schemaext.ObjectComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: tsschema.ContinuousAggregateKind, Subject: subject, Reason: reason})
}
