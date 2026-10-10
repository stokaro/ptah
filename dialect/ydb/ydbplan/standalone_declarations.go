package ydbplan

import (
	"context"
	"fmt"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
)

// standaloneDeclarations is how one standalone owner plans its declarations:
// the model it reads, its family's name in a refusal, the strategy each
// declaration reports, the creation a declared value asks for, and the
// owner's migration planner.
type standaloneDeclarations struct {
	kind     schemaext.Kind
	family   string
	strategy string
	create   func(schemaext.Value) (schemaext.ChangeValue, bool)
	plan     func(context.Context, featureplan.Request) (featureplan.Result, error)
}

// planDeclarations derives one creation per declared object through the
// owner's migration planner. The create operands express the requested
// command, not an observation about a database, and they share the owner's
// operation and graph rules with migrations.
func (d standaloneDeclarations) planDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	planning := featureplan.Request{Target: request.Target, Identifiers: request.Identifiers,
		Capabilities: request.Capabilities, CommonSteps: request.CommonSteps,
		Changes: make([]schemaext.ChangeRecord, len(request.Objects))}
	for i, object := range request.Objects {
		change, ok := d.create(object.Value)
		if !ok {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired %s", schemaext.ErrInvalidValue, d.family)
		}
		planning.Changes[i] = schemaext.ChangeRecord{Subject: object.Ref, Value: change}
	}
	reply, err := d.plan(ctx, planning)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: reply.Complete, Contributions: reply.Contributions}
	for _, diagnostic := range reply.Diagnostics {
		diagnostic = diagnostic.Clone()
		diagnostic.Problem.Kind = string(d.kind)
		result.Diagnostics = append(result.Diagnostics, featureplan.DeclarationDiagnostic{Problem: diagnostic.Problem, Object: diagnostic.Change})
	}
	for _, receipt := range reply.Changes {
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: receipt.Subject,
			Strategy: d.strategy, Steps: receipt.Steps})
	}
	return result, nil
}
