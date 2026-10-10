package ydbcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
)

// ExternalService compares YDB external data sources and external tables in
// one batch, since a data source the plan drops and creates again takes the
// external tables over it along. Two of one kind are equal when
// [ydbexternal.SameDataSource] or [ydbexternal.SameTable] says so, read
// against the request's DatabasePath: the server keeps a secret's path and a
// table's data source as an absolute path.
type ExternalService struct{}

// externalObject is one external object of either kind, as each side holds
// it.
type externalObject[D, O schemaext.Value] struct {
	ref     objectidentity.ID
	desired D
	current O
}

// externalKind is what one kind's comparison needs to know of it.
type externalKind[D, O schemaext.Value] struct {
	kind      schemaext.Kind
	family    string
	namespace string
	codecs    []schemaext.Codec
	enroll    func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error)
	same      func(desired D, current O, root string) bool
	keep      func(current O) schemaext.Value
	change    func(current O, desired D) schemaext.ChangeValue
}

// CompareObjects plans a creation only where the read established absence,
// since CREATE EXTERNAL ... carries no guard a plan writes, and a removal
// only where the desired source claims to describe the kind. An incomplete
// observation never establishes absence or a destructive change. A data
// source the plan drops and creates again -- on a target without
// [capability.ExternalObjectReplace], or with a new source type, which CREATE
// OR REPLACE refuses while a table reads the source -- takes along each
// declared table the database holds over it, as a change whose operands may
// describe the same table. A plan that drops a data source a declared table
// reads is refused. A target without the external_data_sources capability is
// refused by the planner: the comparison reports what differs either way,
// and a read of such a target records each object it lists as unread.
func (ExternalService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if request.Target != "ydb" {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: YDB comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	kinds := slices.Sorted(slices.Values(request.Kinds))
	if !request.Identifiers.Equal(identifier.ForDialect("ydb")) || !slices.Equal(kinds, []schemaext.Kind{ydbexternal.SourceKind, ydbexternal.TableKind}) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: invalid external object comparison vocabulary or identifiers", schemaext.ErrInvalidValue)
	}
	root := request.DatabasePath
	_, sourceResult, err := compareExternalKind(ctx, request, externalKind[*ydbexternal.DesiredSource, *ydbexternal.ObservedSource]{
		kind: ydbexternal.SourceKind, family: "external data source", namespace: "external data sources",
		codecs: ydbexternal.Codecs()[:2], enroll: ydbexternal.SourceCoverage,
		same: func(desired *ydbexternal.DesiredSource, current *ydbexternal.ObservedSource, root string) bool {
			return ydbexternal.SameDataSource(desired.Spec, current.Spec, root)
		},
		keep: func(current *ydbexternal.ObservedSource) schemaext.Value { return current.Desired() },
		change: func(current *ydbexternal.ObservedSource, desired *ydbexternal.DesiredSource) schemaext.ChangeValue {
			return &ydbdiff.ExternalDataSource{Before: current, After: desired}
		},
	}, root)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	tables, tableResult, err := compareExternalKind(ctx, request, externalKind[*ydbexternal.DesiredTable, *ydbexternal.ObservedTable]{
		kind: ydbexternal.TableKind, family: "external table", namespace: "external tables",
		codecs: ydbexternal.Codecs()[2:], enroll: ydbexternal.TableCoverage,
		same: func(desired *ydbexternal.DesiredTable, current *ydbexternal.ObservedTable, root string) bool {
			return ydbexternal.SameTable(desired.Spec, current.Spec, root)
		},
		keep: func(current *ydbexternal.ObservedTable) schemaext.Value { return current.Desired() },
		change: func(current *ydbexternal.ObservedTable, desired *ydbexternal.DesiredTable) schemaext.ChangeValue {
			return &ydbdiff.ExternalTable{Before: current, After: desired}
		},
	}, root)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := refuseOrphanedTables(sourceResult.Changes, tables, root); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	tableResult.Changes = append(tableResult.Changes, tablesOverRecreatedSources(sourceResult.Changes, tableResult.Changes, tables, request.Capabilities, root)...)
	slices.SortFunc(tableResult.Changes, func(a, b schemaext.ChangeRecord) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	coverage, err := sourceResult.Desired.Coverage.Combine(tableResult.Desired.Coverage)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	objects := sourceResult.Desired.Objects
	added, err := tableResult.Desired.Objects.Select(func(ref objectidentity.ID) bool {
		return schemaext.Kind(ref.Kind) == ydbexternal.TableKind
	}).All()
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	for _, object := range added {
		if _, found, err := objects.Get(object.Ref); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		} else if !found {
			if objects, err = objects.With(object); err != nil {
				return schemaext.ObjectComparisonResult{}, err
			}
		}
	}
	return schemaext.ObjectComparisonResult{Complete: true, Desired: schemaext.ObjectState{Objects: objects, Coverage: coverage},
		Changes:   append(sourceResult.Changes, tableResult.Changes...),
		Undecided: append(sourceResult.Undecided, tableResult.Undecided...)}, nil
}

// compareExternalKind compares the objects of one kind the way a standalone
// owner compares its own, and refuses a drop a dotted limit reads like.
func compareExternalKind[D, O schemaext.Value](ctx context.Context, request schemaext.ObjectComparisonRequest,
	spec externalKind[D, O], root string,
) (map[objectidentity.Key]externalObject[D, O], schemaext.ObjectComparisonResult, error) {
	sub := request
	sub.Kinds = []schemaext.Kind{spec.kind}
	sub.Desired = selectKind(request.Desired, spec.kind)
	sub.Current = selectKind(request.Current, spec.kind)
	objects := make(map[objectidentity.Key]externalObject[D, O])
	collect := func(state schemaext.ObjectState, direction schemaext.Representation) error {
		return collectStandalone(ctx, state, direction, spec.kind, spec.family, spec.codecs, ydbexternal.ValidateIdentity,
			func(ref objectidentity.ID, desired D, current O) {
				value := objects[ref.Key()]
				value.ref = ref
				if !isNilValue(desired) {
					value.desired = desired
				}
				if !isNilValue(current) {
					value.current = current
				}
				objects[ref.Key()] = value
			})
	}
	if err := collect(sub.Desired, schemaext.Desired); err != nil {
		return nil, schemaext.ObjectComparisonResult{}, err
	}
	if err := collect(sub.Current, schemaext.Observed); err != nil {
		return nil, schemaext.ObjectComparisonResult{}, err
	}
	presence := make([]standalonePresence, 0, len(objects))
	held := make(map[objectidentity.Key]bool, len(objects))
	for key, value := range objects {
		presence = append(presence, standalonePresence{ref: value.ref, desired: !isNilValue(value.desired), current: !isNilValue(value.current)})
		held[key] = !isNilValue(value.current)
	}
	coverage, err := standaloneCoverage(sub, presence, spec.kind, spec.namespace, spec.enroll)
	if err != nil {
		return nil, schemaext.ObjectComparisonResult{}, err
	}
	result, err := completeStandaloneComparison(ctx, sub, objects, coverage, spec.kind, spec.family,
		func(value externalObject[D, O]) objectidentity.ID { return value.ref },
		func(request schemaext.ObjectComparisonRequest, value externalObject[D, O], result *schemaext.ObjectComparisonResult) error {
			return compareExternalObject(request, spec, value, result, root)
		})
	if err != nil {
		return nil, schemaext.ObjectComparisonResult{}, err
	}
	var drops []objectidentity.ID
	for _, change := range result.Changes {
		if dropsObject(change.Value) {
			drops = append(drops, change.Subject)
		}
	}
	if err := refuseDottedLimits(sub.Desired.Coverage, spec.family, held, drops, ydbexternal.Display); err != nil {
		return nil, schemaext.ObjectComparisonResult{}, err
	}
	return objects, result, nil
}

func compareExternalObject[D, O schemaext.Value](request schemaext.ObjectComparisonRequest, spec externalKind[D, O],
	value externalObject[D, O], result *schemaext.ObjectComparisonResult, root string,
) error {
	declared, held := !isNilValue(value.desired), !isNilValue(value.current)
	desiredKnowledge := request.Desired.Coverage.Lookup(spec.kind, value.ref)
	if declared && standaloneLimited(request.Desired.Coverage, spec.kind, value.ref) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: spec.kind, Subject: value.ref,
			Reason: "the desired source cannot describe the " + spec.family})
		return nil
	}
	claimed := declared || !unknown(desiredKnowledge)
	currentKnowledge := request.Current.Coverage.Lookup(spec.kind, value.ref)
	if standaloneLimited(request.Current.Coverage, spec.kind, value.ref) || (!held && unknown(currentKnowledge)) {
		if claimed {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: spec.kind, Subject: value.ref,
				Reason: "the " + spec.family + " or its absence was not established: " + currentKnowledge.Reason})
		}
		return nil
	}
	if !claimed {
		if held {
			var err error
			result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: value.ref, Value: spec.keep(value.current)})
			return err
		}
		return nil
	}
	switch {
	case !declared && !held:
		return nil
	case declared && held && spec.same(value.desired, value.current, root):
		return nil
	default:
		var current O
		var desired D
		if held {
			current = value.current
		}
		if declared {
			desired = value.desired
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: value.ref, Value: spec.change(current, desired)})
		return nil
	}
}

// selectKind is the part of state about kind.
func selectKind(state schemaext.ObjectState, kind schemaext.Kind) schemaext.ObjectState {
	return schemaext.ObjectState{
		Objects:  state.Objects.Select(func(ref objectidentity.ID) bool { return schemaext.Kind(ref.Kind) == kind }),
		Coverage: state.Coverage.SelectKinds([]schemaext.Kind{kind}),
	}
}

// isNilValue reports a value that holds no object: a nil interface or a typed
// nil pointer.
func isNilValue(value schemaext.Value) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case *ydbexternal.DesiredSource:
		return typed == nil
	case *ydbexternal.ObservedSource:
		return typed == nil
	case *ydbexternal.DesiredTable:
		return typed == nil
	case *ydbexternal.ObservedTable:
		return typed == nil
	}
	return false
}

// dropsObject reports a change that drops its object.
func dropsObject(change schemaext.ChangeValue) bool {
	switch typed := change.(type) {
	case *ydbdiff.ExternalDataSource:
		return typed.After == nil
	case *ydbdiff.ExternalTable:
		return typed.After == nil
	}
	return false
}

// recreatedSources are the paths of the data sources whose change the tables
// over them cannot stay through; see [ydbdiff.ExternalDataSource.Displaces].
func recreatedSources(changes []schemaext.ChangeRecord, caps capability.Capabilities) map[objectidentity.Key]bool {
	recreated := make(map[objectidentity.Key]bool)
	for _, record := range changes {
		if change, ok := record.Value.(*ydbdiff.ExternalDataSource); ok && change.Displaces(caps) {
			recreated[record.Subject.Key()] = true
		}
	}
	return recreated
}

// tablesOverRecreatedSources asks for each declared table the database holds
// over a data source the plan recreates, and that no change names already:
// the source cannot be dropped while a table reads it.
func tablesOverRecreatedSources(sourceChanges, tableChanges []schemaext.ChangeRecord,
	tables map[objectidentity.Key]externalObject[*ydbexternal.DesiredTable, *ydbexternal.ObservedTable], caps capability.Capabilities, root string,
) []schemaext.ChangeRecord {
	recreated := recreatedSources(sourceChanges, caps)
	if len(recreated) == 0 {
		return nil
	}
	named := make(map[objectidentity.Key]bool, len(tableChanges))
	for _, change := range tableChanges {
		named[change.Subject.Key()] = true
	}
	var extra []schemaext.ChangeRecord
	for key, table := range tables {
		if table.desired == nil || table.current == nil || named[key] {
			continue
		}
		source, found := ydbexternal.ResolveSource(root, table.desired.Spec.DataSource)
		if !found || !recreated[source.Key()] {
			continue
		}
		extra = append(extra, schemaext.ChangeRecord{Subject: table.ref,
			Value: &ydbdiff.ExternalTable{Before: table.current, After: table.desired}})
	}
	return extra
}

// refuseOrphanedTables refuses a plan that drops a data source a declared
// external table reads. The plan would keep the table over a source that is
// gone: 25.4.1.15 and later refuse the drop, and 25.1.4.7 to 25.3.1.25 take
// it and leave a table nothing can drop (`path hasn't been resolved`).
func refuseOrphanedTables(sourceChanges []schemaext.ChangeRecord,
	tables map[objectidentity.Key]externalObject[*ydbexternal.DesiredTable, *ydbexternal.ObservedTable], root string,
) error {
	dropped := make(map[objectidentity.Key]objectidentity.ID)
	for _, record := range sourceChanges {
		if dropsObject(record.Value) {
			dropped[record.Subject.Key()] = record.Subject
		}
	}
	if len(dropped) == 0 {
		return nil
	}
	refs := make([]objectidentity.ID, 0, len(tables))
	for _, table := range tables {
		refs = append(refs, table.ref)
	}
	slices.SortFunc(refs, schemaext.CompareRefs)
	for _, ref := range refs {
		table := tables[ref.Key()]
		if table.desired == nil {
			continue
		}
		source, found := ydbexternal.ResolveSource(root, table.desired.Spec.DataSource)
		if name, gone := dropped[source.Key()]; found && gone {
			return (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
				Code: schemavalidation.InvalidSchema, Kind: "external table", Object: ydbexternal.Display(ref.Schema.Source, ref.Name.Source),
				Message: "external table " + ydbexternal.Display(ref.Schema.Source, ref.Name.Source) + " reads data source " +
					ydbexternal.Display(name.Schema.Source, name.Name.Source) +
					", which the plan drops; declare the data source or move the table to one the plan keeps",
			}}}).Err(platform.YDB)
		}
	}
	return nil
}
