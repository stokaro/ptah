package pgtypesource

import (
	"fmt"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

func rangeCoverage(limits []coverage.Object) (schemaext.Coverage, error) {
	knowledge := schemaext.Knowledge{State: schemaext.Complete}
	var subjects []schemaext.SubjectCoverage
	for _, limit := range limits {
		if limit.Kind != coverage.Range {
			return schemaext.Coverage{}, fmt.Errorf("%w: unrelated range coverage limit", schemaext.ErrInvalidValue)
		}
		unknown := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source leaves these range types unmanaged"}
		if limit.Name == "" {
			knowledge = unknown
			continue
		}
		schema, name := annotation.QualifiedName("", limit.Name)
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgtype.RangeKind, Subject: pgtype.Ref(pgtype.RangeKind, schema, name), Knowledge: unknown})
	}
	return pgtype.Coverage(pgtype.RangeKind, schemaext.Desired, knowledge, subjects)
}

// rangeLimits is the not-described record a document carries for range types.
// Only a record saying the namespace was not described becomes a limit. A
// coverage holding no range record makes no claim: a read of a target without
// range types, such as SQLite or MySQL, records none, and writing a limit for
// it would declare a gap in a family that target does not have.
func rangeLimits(known schemaext.Coverage) ([]annotation.EncodedLimit, error) {
	var limits []annotation.EncodedLimit
	wholeUnknown := false
	for _, record := range known.KindRecords() {
		if record.Model.Kind != pgtype.RangeKind {
			return nil, fmt.Errorf("%w: unrelated range coverage model", schemaext.ErrInvalidValue)
		}
		wholeUnknown = record.Knowledge.State != schemaext.Complete
	}
	if wholeUnknown {
		limits = append(limits, annotation.EncodedLimit{Kind: string(coverage.Range)})
	}
	for _, record := range known.SubjectRecords() {
		if record.Kind != pgtype.RangeKind {
			return nil, fmt.Errorf("%w: unrelated range subject", schemaext.ErrInvalidValue)
		}
		if err := pgtype.ValidateRef(record.Subject); err != nil {
			return nil, err
		}
		if len(record.Targets) > 0 {
			return nil, fmt.Errorf("%w: a range coverage limit cannot preserve a target scope", ptaherr.ErrUnsupportedFeature)
		}
		switch record.Knowledge.State {
		case schemaext.Complete:
			if wholeUnknown {
				return nil, fmt.Errorf("%w: a range source cannot preserve a complete subject inside an unknown namespace", ptaherr.ErrUnsupportedFeature)
			}
		case schemaext.Uninspected, schemaext.Unrepresentable:
			limits = append(limits, annotation.EncodedLimit{Kind: string(coverage.Range), Name: pgtype.QualifiedName(record.Subject)})
		default:
			return nil, fmt.Errorf("%w: a range source cannot preserve an explicit %s claim", ptaherr.ErrUnsupportedFeature, record.Knowledge.State)
		}
	}
	return limits, nil
}

// sourceCoverage is what a document read through the owner knows of its
// models. It describes every domain and composite type: a not-described
// record of either kind is a common record, since Oracle's domains and
// composite types read it too, and the runtime folds it into this claim
// before the owner compares. The document's range records limit its range
// types.
func sourceCoverage(limits []coverage.Object) (schemaext.Coverage, error) {
	claim, err := rangeCoverage(limits)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, kind := range []schemaext.Kind{pgtype.DomainKind, pgtype.CompositeKind} {
		complete, err := pgtype.Coverage(kind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if claim, err = claim.Combine(complete); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return claim, nil
}
