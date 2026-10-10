package ydbplan

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbsecret"
)

// ExternalService plans statements on YDB external data sources and external
// tables in one batch, since a data source the plan drops and creates again
// takes the external tables over it along.
//
// YDB alters neither object (`Alter operation for EXTERNAL_DATA_SOURCE
// objects is not implemented`), so a changed one is replaced: by CREATE OR
// REPLACE on a target with [capability.ExternalObjectReplace], and otherwise
// by a DROP and a CREATE. A data source with external tables over it cannot
// be dropped (`Other entities depend on this data source`), and CREATE OR
// REPLACE refuses a new source type while a table reads the source (measured
// on 26.2.1.14), so a data source the plan drops and creates again takes each
// table over it along: the comparison asks for those tables, and the plan
// drops them before the source and creates them after. A table that moves
// off a data source the plan drops is dropped and created too.
//
// Every statement is early: it runs as soon as its dependencies allow, ahead
// of the common statements. It follows a common statement that frees its
// path, and a dropped object precedes one that creates an object at its path
// or above it (see [schemePathDependencies]). A data source reads the secrets
// its _SECRET_PATH options name, and an external table reads its data source,
// so the host orders a secret's creation before them and its drop after.
type ExternalService struct{}

// externalGroup orders the statements of one batch: tables are dropped before
// the data sources they read, and created after.
type externalGroup int

const (
	externalTableDrops externalGroup = iota
	externalSourceDrops
	externalSourceWrites
	externalTableWrites
)

// externalStep is one statement the batch plans.
type externalStep struct {
	input     int
	ref       objectidentity.ID
	group     externalGroup
	action    plangraph.Action
	operation ydbast.ExternalOperation
	payload   standalonePayload
	// reads are what the statement reads by path: the secrets of a data
	// source, the data source of an external table.
	reads []objectidentity.ID
	// source is the data source a table statement reads or read, to order
	// it against that source's statements.
	sources []objectidentity.ID
}

// slot is the scheme path the statement's object holds.
func (s externalStep) slot() objectidentity.ID {
	return ydbscheme.Path(s.ref.Schema.Source, s.ref.Name.Source)
}

// externalChange is one change of the batch with the statements it needs.
type externalChange struct {
	input    int
	ref      objectidentity.ID
	kind     schemaext.Kind
	strategy string
	steps    []externalStep
	// refused is why the batch cannot take the change, before any
	// capability is asked.
	refused error
}

// PlanFeatures returns the statements each external object change needs. A
// refused change returns no operation from the batch. A canceled context
// returns no result.
func (ExternalService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != "ydb" {
		return featureplan.Result{}, fmt.Errorf("%w: YDB planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !request.Identifiers.Equal(identifier.ForDialect("ydb")) || len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: invalid external object planning scope", schemaext.ErrInvalidValue)
	}
	changes, err := externalChanges(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	if diagnostics := externalRefusals(request, changes); len(diagnostics) > 0 {
		return featureplan.Result{Complete: true, Diagnostics: diagnostics}, nil
	}
	return externalContribution(ctx, request, changes)
}

// externalChanges lowers the batch's changes in input order. An error is a
// request no owner could answer.
func externalChanges(ctx context.Context, request featureplan.Request) ([]externalChange, error) {
	sources := make(map[objectidentity.Key]*ydbdiff.ExternalDataSource)
	tables := make(map[objectidentity.Key]*ydbdiff.ExternalTable)
	seen := make(map[objectidentity.Key]bool)
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cloned, err := record.Clone()
		if err != nil {
			return nil, err
		}
		if seen[record.Subject.Key()] {
			return nil, fmt.Errorf("%w: duplicate external object change", schemaext.ErrInvalidValue)
		}
		seen[record.Subject.Key()] = true
		if err := ydbexternal.ValidateIdentity(record.Subject); err != nil {
			return nil, err
		}
		switch change := cloned.Value.(type) {
		case *ydbdiff.ExternalDataSource:
			if schemaext.Kind(record.Subject.Kind) != ydbexternal.SourceKind {
				return nil, fmt.Errorf("%w: a data source change names %s", schemaext.ErrInvalidValue, record.Subject.Kind)
			}
			err = change.Validate()
			sources[record.Subject.Key()] = change
		case *ydbdiff.ExternalTable:
			if schemaext.Kind(record.Subject.Kind) != ydbexternal.TableKind {
				return nil, fmt.Errorf("%w: an external table change names %s", schemaext.ErrInvalidValue, record.Subject.Kind)
			}
			err = change.Validate()
			tables[record.Subject.Key()] = change
		default:
			err = fmt.Errorf("%w: unexpected external object change %T", schemaext.ErrInvalidValue, cloned.Value)
		}
		if err != nil {
			return nil, err
		}
	}
	caps := request.Capabilities
	displaced := make(map[objectidentity.Key]bool)
	for key, change := range sources {
		displaced[key] = change.After == nil || change.Displaces(caps)
	}
	changes := make([]externalChange, 0, len(request.Changes))
	for index, record := range request.Changes {
		if source, ok := sources[record.Subject.Key()]; ok {
			changes = append(changes, lowerSource(index, record.Subject, source, caps, request.DatabasePath))
			continue
		}
		changes = append(changes, lowerTable(index, record.Subject, tables[record.Subject.Key()], caps, displaced, request.DatabasePath))
	}
	return changes, nil
}

func lowerSource(index int, ref objectidentity.ID, change *ydbdiff.ExternalDataSource, caps capability.Capabilities, root string) externalChange {
	schema, name := ref.Schema.Source, ref.Name.Source
	result := externalChange{input: index, ref: ref, kind: ydbdiff.ExternalDataSourceKind}
	drop := externalStep{input: index, ref: ref, group: externalSourceDrops, action: plangraph.Drop, operation: ydbast.ExternalDrop,
		payload: &ydbast.ExternalDataSource{Operation: ydbast.ExternalDrop, Schema: schema, Name: name}}
	write := func(operation ydbast.ExternalOperation, action plangraph.Action) externalStep {
		reads, err := secretReads(root, change.After.Spec.Options)
		if err != nil {
			result.refused = fmt.Errorf("external data source %s: %w", ydbexternal.Display(schema, name), err)
		}
		return externalStep{input: index, ref: ref, group: externalSourceWrites, action: action, operation: operation, reads: reads,
			payload: &ydbast.ExternalDataSource{Operation: operation, Schema: schema, Name: name, Spec: change.After.Spec.Clone()}}
	}
	switch {
	case change.Before == nil:
		result.strategy = "create the data source"
		result.steps = []externalStep{write(ydbast.ExternalCreate, plangraph.Create)}
	case change.After == nil:
		result.strategy = "drop the data source; the system it reads is untouched"
		result.steps = []externalStep{drop}
	case !caps.Has(capability.ExternalObjectReplace):
		result.strategy = "drop the data source and create it again, with the external tables over it"
		result.steps = []externalStep{write(ydbast.ExternalRecreate, plangraph.Alter)}
	case change.Displaces(caps):
		result.strategy = "replace the data source in place with CREATE OR REPLACE, with the external tables over it dropped first"
		result.steps = []externalStep{write(ydbast.ExternalReplace, plangraph.Alter)}
	default:
		result.strategy = "replace the data source in place with CREATE OR REPLACE"
		result.steps = []externalStep{write(ydbast.ExternalReplace, plangraph.Alter)}
	}
	return result
}

func lowerTable(index int, ref objectidentity.ID, change *ydbdiff.ExternalTable, caps capability.Capabilities, displaced map[objectidentity.Key]bool, root string) externalChange {
	schema, name := ref.Schema.Source, ref.Name.Source
	result := externalChange{input: index, ref: ref, kind: ydbdiff.ExternalTableKind}
	var before, after []objectidentity.ID
	if change.Before != nil {
		if source, ok := ydbexternal.ResolveSource(root, change.Before.Spec.DataSource); ok {
			before = []objectidentity.ID{source}
		}
	}
	if change.After != nil {
		if source, ok := ydbexternal.ResolveSource(root, change.After.Spec.DataSource); ok {
			after = []objectidentity.ID{source}
		}
	}
	drop := externalStep{input: index, ref: ref, group: externalTableDrops, action: plangraph.Drop, operation: ydbast.ExternalDrop, sources: before,
		payload: &ydbast.ExternalTable{Operation: ydbast.ExternalDrop, Schema: schema, Name: name}}
	write := func(operation ydbast.ExternalOperation, action plangraph.Action) externalStep {
		return externalStep{input: index, ref: ref, group: externalTableWrites, action: action, operation: operation, reads: after, sources: after,
			payload: &ydbast.ExternalTable{Operation: operation, Schema: schema, Name: name, Spec: change.After.Spec.Clone()}}
	}
	readsDisplaced := slices.ContainsFunc(slices.Concat(before, after), func(source objectidentity.ID) bool { return displaced[source.Key()] })
	switch {
	case change.Before == nil:
		result.strategy = "create the external table"
		result.steps = []externalStep{write(ydbast.ExternalCreate, plangraph.Create)}
	case change.After == nil:
		result.strategy = "drop the external table; the files it reads stay"
		result.steps = []externalStep{drop}
	case caps.Has(capability.ExternalObjectReplace) && !readsDisplaced:
		result.strategy = "replace the external table in place with CREATE OR REPLACE"
		result.steps = []externalStep{write(ydbast.ExternalReplace, plangraph.Alter)}
	default:
		result.strategy = "drop the external table and create it again"
		result.steps = []externalStep{drop, write(ydbast.ExternalCreate, plangraph.Create)}
	}
	return result
}

// secretReads names each YDB secret a data source's _SECRET_PATH options name,
// read against root, the absolute path of the database. A path outside root is
// refused, since no data source of the database can read the secret; a path
// written absolute where root is not known, or one that cannot name a secret,
// reads nothing this plan manages.
func secretReads(root string, options map[string]string) ([]objectidentity.ID, error) {
	var reads []objectidentity.ID
	for _, option := range slices.Sorted(maps.Keys(options)) {
		written := options[option]
		if !strings.HasSuffix(strings.ToUpper(option), "_SECRET_PATH") || strings.TrimSpace(written) == "" {
			continue
		}
		ref, err := ydbsecret.ResolvePath(root, written)
		if errors.Is(err, ydbsecret.ErrOutsideDatabase) {
			return nil, fmt.Errorf("option %s names secret %q, which is outside the database /%s, so the data source cannot read it",
				option, written, strings.Trim(root, "/"))
		}
		if err != nil || slices.ContainsFunc(reads, func(read objectidentity.ID) bool { return read.Key() == ref.Key() }) {
			continue
		}
		reads = append(reads, ref)
	}
	return reads, nil
}

// externalRefusals refuses, before any operation is returned, a statement the
// target cannot take: every one on a target without the external_data_sources
// capability, a declaration the line refuses, two objects written at one path,
// and an external table over a data source the batch writes that is not
// object storage.
func externalRefusals(request featureplan.Request, changes []externalChange) []featureplan.Diagnostic {
	var diagnostics []featureplan.Diagnostic
	conflicts := externalConflicts(changes)
	for _, change := range changes {
		if change.refused == nil {
			change.refused = conflicts[change.input]
		}
		if change.refused != nil {
			diagnostics = append(diagnostics, standaloneDiagnostic(change.kind, change.input, change.ref, change.refused))
			continue
		}
		for _, step := range change.steps {
			if refusal := externalRefusal(request.Capabilities, step); refusal != nil {
				problem := schemavalidation.Diagnostic{Code: schemavalidation.InvalidSchema, Kind: string(change.kind),
					Object: change.ref.String(), Message: refusal.Err(request.Target).Error()}
				if refusal.Key != "" {
					problem.Code, problem.Feature = schemavalidation.UnsupportedFeature, string(refusal.Key)
				}
				diagnostics = append(diagnostics, featureplan.Diagnostic{Change: new(change.input), Problem: problem})
				break
			}
		}
	}
	return diagnostics
}

// externalConflicts finds, by change, the objects the batch cannot write
// together: an object written at the path another one is written at, since
// YDB keeps one object at a path (`unexpected path type`, measured on 25.1.4.7
// and 26.2.1.14), and an external table over a data source the batch writes
// with a source type other than ObjectStorage, since an external table reads
// files (`Only ObjectStorage source type supported but got PostgreSQL`).
func externalConflicts(changes []externalChange) map[int]error {
	conflicts := make(map[int]error)
	written := make(map[objectidentity.Key]objectidentity.ID)
	sourceTypes := make(map[objectidentity.Key]string)
	for _, change := range changes {
		for _, step := range change.steps {
			if step.action == plangraph.Drop {
				continue
			}
			if source, ok := step.payload.(*ydbast.ExternalDataSource); ok {
				sourceTypes[step.ref.Key()] = source.Spec.SourceType
			}
			if other, taken := written[step.slot().Key()]; taken && other.Key() != step.ref.Key() {
				conflicts[change.input] = fmt.Errorf("%s %s has the path of %s %s, and YDB keeps one object at a path (`unexpected path type`)",
					externalFamily(step.ref), ydbexternal.Display(step.ref.Schema.Source, step.ref.Name.Source),
					externalFamily(other), ydbexternal.Display(other.Schema.Source, other.Name.Source))
				continue
			}
			written[step.slot().Key()] = step.ref
		}
	}
	for _, change := range changes {
		for _, step := range change.steps {
			table, ok := step.payload.(*ydbast.ExternalTable)
			if !ok || step.action == plangraph.Drop || len(step.sources) == 0 || conflicts[change.input] != nil {
				continue
			}
			sourceType, declared := sourceTypes[step.sources[0].Key()]
			if declared && sourceType != "ObjectStorage" {
				conflicts[change.input] = fmt.Errorf("external table %s reads data source %s, a %s source; an external table reads files, "+
					"from an ObjectStorage source (`Only ObjectStorage source type supported`)",
					ydbexternal.Display(step.ref.Schema.Source, step.ref.Name.Source), table.Spec.DataSource, sourceType)
			}
		}
	}
	return conflicts
}

// externalFamily names the kind of the external object ref.
func externalFamily(ref objectidentity.ID) string {
	if schemaext.Kind(ref.Kind) == ydbexternal.SourceKind {
		return "external data source"
	}
	return "external table"
}

func externalRefusal(caps capability.Capabilities, step externalStep) *ydbexternal.Refusal {
	display := ydbexternal.Display(step.ref.Schema.Source, step.ref.Name.Source)
	switch payload := step.payload.(type) {
	case *ydbast.ExternalDataSource:
		if step.operation == ydbast.ExternalDrop {
			return ydbexternal.CheckDrop("DROP EXTERNAL DATA SOURCE "+display, caps)
		}
		return cmp.Or(ydbexternal.CheckDataSource(display, payload.Spec, caps), replaceRefusal(step, "external data source "+display, caps))
	case *ydbast.ExternalTable:
		if step.operation == ydbast.ExternalDrop {
			return ydbexternal.CheckDrop("DROP EXTERNAL TABLE "+display, caps)
		}
		return cmp.Or(ydbexternal.CheckTable(display, payload.Spec, caps), replaceRefusal(step, "external table "+display, caps))
	}
	return nil
}

func replaceRefusal(step externalStep, subject string, caps capability.Capabilities) *ydbexternal.Refusal {
	if step.operation != ydbast.ExternalReplace {
		return nil
	}
	return ydbexternal.CheckReplace(subject, caps)
}

// externalContribution orders the batch's statements and returns one receipt
// per change.
func externalContribution(ctx context.Context, request featureplan.Request, changes []externalChange) (featureplan.Result, error) {
	common := indexCommonSteps(request.CommonSteps)
	var steps []externalStep
	for _, change := range changes {
		steps = append(steps, change.steps...)
	}
	slices.SortStableFunc(steps, func(a, b externalStep) int {
		return cmp.Or(cmp.Compare(a.group, b.group), schemaext.CompareRefs(a.ref, b.ref))
	})
	ids := make(map[int][]plangraph.StepID, len(changes))
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for index, step := range steps {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		family := externalFamily(step.ref)
		id := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("%s/%06d/%s", strings.ReplaceAll(family, " ", "-"), index, step.action)}
		edges, err := schemePathDependencies(family, id, step.slot(), step.action, common.steps)
		if err != nil {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{
				standaloneDiagnostic(changes[step.input].kind, step.input, step.ref, err)}}, nil
		}
		contribution.Dependencies = append(contribution.Dependencies, edges...)
		effects := []plangraph.Effect{{Subject: step.ref, Action: step.action}, {Subject: step.slot(), Action: step.action}}
		for _, read := range step.reads {
			effects = append(effects, plangraph.Effect{Subject: read, Action: plangraph.Read})
		}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: step.payload},
			Effects:     effects,
			Transaction: plangraph.TransactionForbidden, Impact: step.payload.Effect(), Placement: plangraph.PlacementEarly,
		})
		ids[step.input] = append(ids[step.input], id)
	}
	contribution.Dependencies = append(contribution.Dependencies, externalOrder(steps, contribution.Steps)...)
	for _, change := range changes {
		result.Changes[change.input] = featureplan.ChangePlan{Subject: change.ref, Kind: change.kind, Strategy: change.strategy, Steps: ids[change.input]}
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

// externalOrder orders the batch's own statements: a drop before a creation
// at its path, an external table's drop before any statement on the data
// source it read, and a data source's creation or replacement before the
// external tables that read it are created or replaced.
func externalOrder(steps []externalStep, planned []plangraph.Step[featureplan.Operation]) []plangraph.Dependency {
	var edges []plangraph.Dependency
	for i, first := range steps {
		for j, second := range steps {
			if first.group >= second.group {
				continue
			}
			sameSlot := first.slot().Key() == second.slot().Key()
			switch {
			case first.action == plangraph.Drop && second.action == plangraph.Create && sameSlot,
				first.group == externalTableDrops && second.group <= externalSourceWrites && readsSource(first, second.ref),
				first.group == externalSourceWrites && second.group == externalTableWrites && readsSource(second, first.ref):
				edges = append(edges, plangraph.Dependency{Before: planned[i].ID, After: planned[j].ID})
			}
		}
	}
	return edges
}

// readsSource reports a table statement that reads or read the data source
// ref.
func readsSource(step externalStep, ref objectidentity.ID) bool {
	return slices.ContainsFunc(step.sources, func(source objectidentity.ID) bool { return source.Key() == ref.Key() })
}

// PlanDeclarations derives one CREATE per declared data source and external
// table. The operations share the owner's graph rules with migrations, so a
// data source precedes the tables over it and follows the secrets it reads.
func (s ExternalService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	planning := featureplan.Request{Target: request.Target, Identifiers: request.Identifiers,
		Capabilities: request.Capabilities, CommonSteps: request.CommonSteps,
		Changes: make([]schemaext.ChangeRecord, len(request.Objects))}
	for i, object := range request.Objects {
		var change schemaext.ChangeValue
		switch value := object.Value.(type) {
		case *ydbexternal.DesiredSource:
			if value != nil {
				change = &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: value.Spec.Clone(), StructName: value.StructName}}
			}
		case *ydbexternal.DesiredTable:
			if value != nil {
				change = &ydbdiff.ExternalTable{After: &ydbexternal.DesiredTable{Spec: value.Spec.Clone(), StructName: value.StructName}}
			}
		}
		if change == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired external object", schemaext.ErrInvalidValue)
		}
		planning.Changes[i] = schemaext.ChangeRecord{Subject: object.Ref, Value: change}
	}
	reply, err := s.PlanFeatures(ctx, planning)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: reply.Complete, Contributions: reply.Contributions}
	for _, diagnostic := range reply.Diagnostics {
		diagnostic = diagnostic.Clone()
		diagnostic.Problem.Kind = string(modelKind(schemaext.Kind(diagnostic.Problem.Kind)))
		result.Diagnostics = append(result.Diagnostics, featureplan.DeclarationDiagnostic{Problem: diagnostic.Problem, Object: diagnostic.Change})
	}
	for _, receipt := range reply.Changes {
		strategy := "create the declared data source"
		if receipt.Kind == ydbdiff.ExternalTableKind {
			strategy = "create the declared external table"
		}
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: receipt.Subject, Strategy: strategy, Steps: receipt.Steps})
	}
	return result, nil
}

// modelKind is the model kind a change kind of this owner describes.
func modelKind(kind schemaext.Kind) schemaext.Kind {
	if kind == ydbdiff.ExternalTableKind {
		return ydbexternal.TableKind
	}
	return ydbexternal.SourceKind
}
