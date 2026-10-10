package ydbplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TopicService plans statements on standalone YDB topics. A topic depends on
// nothing but its path, so each statement is early and runs before the common
// statements, except when it has to follow one that frees its path: a topic
// created where the plan drops a table, or below such a path, follows that
// drop, since every directory above a topic must hold no other object. A
// dropped topic likewise goes before a common statement that creates an object
// at its path or above it. A transfer reads its topic by path: a created or
// changed topic comes before every statement that reads it, and a dropped one
// after every such statement, so a transfer the plan drops is gone before its
// topic.
type TopicService struct{}

type topicChange struct {
	input     int
	operation *ydbast.Topic
}

// PlanFeatures returns one operation per created, changed or dropped topic.
// A refused change returns no operation from the batch.
func (TopicService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
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
		return featureplan.Result{}, fmt.Errorf("%w: invalid topic planning scope", schemaext.ErrInvalidValue)
	}
	changes, err := topicOperations(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	if diagnostics := topicRefusals(request, changes); len(diagnostics) > 0 {
		return featureplan.Result{Complete: true, Diagnostics: diagnostics}, nil
	}
	slices.SortFunc(changes, func(a, b topicChange) int {
		return schemaext.CompareRefs(a.operation.Subject(), b.operation.Subject())
	})
	common := indexCommonSteps(request.CommonSteps)
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for index, change := range changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		operation := change.operation
		action := topicAction(operation)
		id := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("topic/%06d/%s", index, action)}
		slot := ydbscheme.Path(operation.Schema, operation.Name)
		edges, err := topicDependencies(id, operation.Subject(), slot, action, common)
		if err != nil {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{topicDiagnostic(change.input, operation, err)}}, nil
		}
		contribution.Dependencies = append(contribution.Dependencies, edges...)
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: operation},
			Effects:     []plangraph.Effect{{Subject: operation.Subject(), Action: action}, {Subject: slot, Action: action}},
			Transaction: plangraph.TransactionForbidden, Impact: operation.Effect(), Placement: plangraph.PlacementEarly,
		})
		result.Changes[change.input] = featureplan.ChangePlan{Subject: operation.Subject(), Kind: ydbdiff.TopicKind,
			Strategy: topicStrategy(operation), Steps: []plangraph.StepID{id}}
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

// topicDependencies orders one statement on the topic ref, at the scheme path
// slot, against the common statements: the handoffs at its path and the
// directories above it (see [schemePathDependencies]), a drop after every
// statement that reads the topic, and a creation or change before every
// statement that reads it. The statement is early, so it runs as soon as
// these allow, ahead of the common statements.
func topicDependencies(id plangraph.StepID, ref, slot objectidentity.ID, action plangraph.Action, common commonSteps) ([]plangraph.Dependency, error) {
	edges, err := schemePathDependencies("topic", id, slot, action, common.steps)
	if err != nil {
		return nil, err
	}
	var readers []plangraph.StepID
	for _, use := range common.uses[ref.Key()] {
		if use.action == plangraph.Read {
			readers = append(readers, common.steps[use.position].ID)
		}
	}
	if action == plangraph.Drop {
		for _, reader := range readers {
			edges = append(edges, plangraph.Dependency{Before: reader, After: id})
		}
	}
	if action != plangraph.Drop {
		for _, reader := range readers {
			edges = append(edges, plangraph.Dependency{Before: id, After: reader})
		}
	}
	return edges, nil
}

func topicOperations(ctx context.Context, request featureplan.Request) ([]topicChange, error) {
	changes := make([]topicChange, 0, len(request.Changes))
	seen := make(map[objectidentity.Key]bool)
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cloned, err := record.Clone()
		if err != nil {
			return nil, err
		}
		change, ok := cloned.Value.(*ydbdiff.Topic)
		if !ok || seen[record.Subject.Key()] {
			return nil, fmt.Errorf("%w: unexpected or duplicate topic change", schemaext.ErrInvalidValue)
		}
		seen[record.Subject.Key()] = true
		operation := &ydbast.Topic{Schema: record.Subject.Schema.Source, Name: record.Subject.Name.Source, Change: *change}
		if operation.Subject() != record.Subject {
			return nil, fmt.Errorf("%w: topic change has an invalid standalone identity", schemaext.ErrInvalidValue)
		}
		if err := operation.Validate(); err != nil {
			return nil, err
		}
		changes = append(changes, topicChange{input: index, operation: operation})
	}
	return changes, nil
}

// topicRefusals refuses, before any operation is returned, a statement the
// target cannot take: every one on a target without the topics capability, a
// declaration the line cannot hold, and an in-place change YDB refuses --
// fewer partitions, or auto-partitioning disabled again -- which only
// dropping the topic and every message in it could make.
func topicRefusals(request featureplan.Request, changes []topicChange) []featureplan.Diagnostic {
	var diagnostics []featureplan.Diagnostic
	for _, change := range changes {
		operation := change.operation
		var spec ydbtopic.Spec
		if operation.Change.After != nil {
			spec = operation.Change.After.Spec
		}
		refusal := ydbtopic.Check(operation.Schema, operation.Name, spec, request.Capabilities)
		if refusal == nil && operation.Change.Before != nil && operation.Change.After != nil {
			refusal = ydbtopic.ChangeRefusal(operation.Schema, operation.Name, operation.Change.After.Spec, operation.Change.Before.Spec)
		}
		if refusal == nil {
			continue
		}
		// A refusal without a key is a change YDB makes on no line, which
		// is as unsupported as a line without the key.
		diagnostics = append(diagnostics, featureplan.Diagnostic{Change: new(change.input), Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.UnsupportedFeature, Kind: string(ydbdiff.TopicKind), Object: operation.Subject().String(),
			Feature: string(refusal.Key), Message: refusal.Err(request.Target).Error(),
		}})
	}
	return diagnostics
}

func topicAction(operation *ydbast.Topic) plangraph.Action {
	switch {
	case operation.Change.Before == nil:
		return plangraph.Create
	case operation.Change.After == nil:
		return plangraph.Drop
	default:
		return plangraph.Alter
	}
}

func topicStrategy(operation *ydbast.Topic) string {
	switch {
	case operation.Change.Before == nil:
		return "create the topic with its consumers and declared settings"
	case operation.Change.After == nil:
		return "drop the topic, every message it holds and every consumer's position in it"
	default:
		return "change the topic's settings and consumers in place"
	}
}

func topicDiagnostic(index int, operation *ydbast.Topic, err error) featureplan.Diagnostic {
	return featureplan.Diagnostic{Change: new(index), Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.InvalidSchema, Kind: string(ydbdiff.TopicKind), Object: operation.Subject().String(), Message: err.Error(),
	}}
}

// PlanDeclarations derives one CREATE TOPIC per declared topic. The operations
// share the owner's graph rules with migrations, so a topic precedes the
// transfers that read it and a declared table at its path is refused.
func (s TopicService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	planning := featureplan.Request{Target: request.Target, Identifiers: request.Identifiers,
		Capabilities: request.Capabilities, CommonSteps: request.CommonSteps,
		Changes: make([]schemaext.ChangeRecord, len(request.Objects))}
	for i, object := range request.Objects {
		value, ok := object.Value.(*ydbtopic.Desired)
		if !ok || value == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired topic", schemaext.ErrInvalidValue)
		}
		planning.Changes[i] = schemaext.ChangeRecord{Subject: object.Ref,
			Value: &ydbdiff.Topic{After: &ydbtopic.Desired{Spec: value.Spec.Clone(), StructName: value.StructName}}}
	}
	reply, err := s.PlanFeatures(ctx, planning)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: reply.Complete, Contributions: reply.Contributions}
	for _, diagnostic := range reply.Diagnostics {
		diagnostic = diagnostic.Clone()
		diagnostic.Problem.Kind = string(ydbtopic.Kind)
		result.Diagnostics = append(result.Diagnostics, featureplan.DeclarationDiagnostic{Problem: diagnostic.Problem, Object: diagnostic.Change})
	}
	for _, receipt := range reply.Changes {
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: receipt.Subject,
			Strategy: "create the declared topic with its consumers", Steps: receipt.Steps})
	}
	return result, nil
}
