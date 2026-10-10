// Package mssqlplan plans SQL Server security policy changes and authored
// policies into operations for the host's dependency graph. It orders each
// policy after the tables, columns and functions it binds and before their
// removal, hands a table from one enabled policy to another without breaking
// SQL Server's one-policy-per-table rule, and performs no I/O.
package mssqlplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/mssql/mssqlast"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Owner is the contribution owner of every security policy step.
const Owner = mssqlschema.Owner

// Service plans security policy changes and declarations. Its zero value is
// ready for concurrent use.
//
// A change is one step in the dependent phase: created or changed after the
// host creates and changes the objects a policy binds, dropped before the
// host removes them. The step reads every table, predicate function and
// argument column the policy binds before and after the change, so the host
// orders it after their creation and before their removal, which SQL Server
// refuses while a schema-bound policy references them, and refuses a plan
// that cannot do both.
//
// SQL Server enables one policy with a predicate on a table and refuses a
// second with Msg 33264. When a plan moves a table from one enabled policy to
// another, the step that releases the table, an alteration or a drop, runs
// before the step that takes it, and both require the plan's transaction, so
// no other session sees the table without a predicate between them. The host
// leaves the order of dependent steps to their owner, so a drop can precede
// a creation here.
type Service struct{}

type planned struct {
	input  int
	change *mssqldiff.SecurityPolicy
	ref    objectidentity.ID
	step   plangraph.Step[featureplan.Operation]
	plan   featureplan.ChangePlan
}

// PlanFeatures returns one ChangePlan per change in input order, or a
// completed refusal with no usable prefix. A successful reply must join the
// host graph before any operation is rendered or executed.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: a security policy has no table parent to assess", schemaext.ErrInvalidValue)
	}
	steps := make([]*planned, 0, len(request.Changes))
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		step, err := planChange(request.Identifiers, index, record)
		if err != nil {
			return featureplan.Result{}, err
		}
		steps = append(steps, step)
	}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(steps))}
	handoffs(steps, &contribution)
	for _, step := range steps {
		contribution.Steps = append(contribution.Steps, step.step)
		result.Changes[step.input] = step.plan
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// PlanDeclarations derives one CREATE operation per authored policy for a
// whole-schema render, after every common step: a policy binds tables and
// functions, and nothing the render creates reads a policy. Two enabled
// policies binding one table are refused, naming both, since the script would
// stop at the second with Msg 33264.
func (Service) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	named := make([]mssqlschema.NamedPolicy, 0, len(request.Objects))
	for index, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		declared, ok := object.Value.(*mssqlschema.DesiredSecurityPolicy)
		if !ok || declared == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired security policy, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		record := schemaext.ChangeRecord{Subject: object.Ref, Value: &mssqldiff.SecurityPolicy{After: declared,
			Access: mssqldiff.Assess(request.Identifiers, nil, declared)}}
		step, err := planChange(request.Identifiers, index, record)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		step.step.Payload.Phase = featureplan.PhaseDefault
		named = append(named, mssqlschema.NamedPolicy{Name: objectName(step.ref), Policy: declared})
		for _, common := range request.CommonSteps {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: common.ID, After: step.step.ID})
		}
		contribution.Steps = append(contribution.Steps, step.step)
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref,
			Strategy: "create the security policy after the tables and functions it binds", Steps: []plangraph.StepID{step.step.ID}})
	}
	for _, conflict := range mssqlschema.EnabledTableConflicts(named) {
		index := slices.IndexFunc(named, func(policy mssqlschema.NamedPolicy) bool { return policy.Name == conflict.Second })
		result.Diagnostics = append(result.Diagnostics, featureplan.DeclarationDiagnostic{Object: new(index), Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.InvalidSchema, Kind: string(mssqlschema.SecurityPolicyKind), Object: conflict.Second.String(), Feature: "security policy",
			Message: fmt.Sprintf("enabled security policies %s and %s both bind table %s; SQL Server enables one policy per table "+
				"and refuses the other with Msg 33264: disable one of them, or move the table's predicates into one policy",
				conflict.First, conflict.Second, conflict.Table),
		}})
	}
	if len(result.Diagnostics) > 0 {
		return featureplan.DeclarationResult{Complete: true, Diagnostics: result.Diagnostics}, nil
	}
	if len(contribution.Steps) > 0 {
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
	if platform.NormalizeDialect(target) != platform.SQLServer {
		return fmt.Errorf("%w: SQL Server security policy planning on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

func planChange(semantics identifier.Semantics, index int, record schemaext.ChangeRecord) (*planned, error) {
	cloned, err := record.Clone()
	if err != nil {
		return nil, err
	}
	change, ok := cloned.Value.(*mssqldiff.SecurityPolicy)
	if !ok {
		return nil, fmt.Errorf("%w: unexpected security policy change %T", schemaext.ErrInvalidValue, cloned.Value)
	}
	if err := change.Validate(); err != nil {
		return nil, err
	}
	if err := mssqlschema.ValidateSecurityPolicyRef(cloned.Subject); err != nil {
		return nil, err
	}
	// A defaulted schema is written out as the target's default, so the
	// statement does not depend on the connecting user's default schema.
	ref := mssqlschema.SecurityPolicyRefWith(semantics, cloned.Subject.Schema.Authored(), cloned.Subject.Name.Source)
	action, strategy := plangraph.Alter, "alter the security policy in place"
	switch edit := change.Edit(); {
	case change.Before == nil:
		action, strategy = plangraph.Create, "create the security policy after the tables and functions it binds"
	case change.After == nil:
		action, strategy = plangraph.Drop, "drop the security policy before the tables and functions it binds"
	case edit.Drop:
		strategy = "drop and create the security policy in one transaction, since its schema binding or replication behavior cannot be altered"
	}
	payload := &mssqlast.SecurityPolicy{Schema: ref.Schema.Source, Name: ref.Name.Source, Change: *change}
	id := plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("security-policy/%06d/%s", index, action)}
	step := plangraph.Step[featureplan.Operation]{ID: id,
		Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: payload, Phase: featureplan.PhaseDependent},
		Effects:     append([]plangraph.Effect{{Subject: ref, Action: action}}, reads(semantics, change)...),
		Transaction: plangraph.TransactionAllowed, Impact: change.Effect(),
	}
	if change.Edit().Statements() > 1 {
		step.Transaction = plangraph.TransactionRequired
	}
	return &planned{input: index, change: change, ref: ref, step: step,
		plan: featureplan.ChangePlan{Subject: cloned.Subject, Kind: mssqldiff.SecurityPolicyKind, Strategy: strategy, Steps: []plangraph.StepID{id}}}, nil
}

// reads are the tables, functions and argument columns a change's policy
// binds before and after it. A statement that creates, alters or drops the
// policy needs each of them to exist, so the host orders the step after their
// creation and before their removal.
func reads(semantics identifier.Semantics, change *mssqldiff.SecurityPolicy) []plangraph.Effect {
	var predicates []mssqlschema.Predicate
	if change.Before != nil {
		predicates = append(predicates, change.Before.Predicates...)
	}
	if change.After != nil {
		predicates = append(predicates, change.After.Predicates...)
	}
	builder := objectidentity.NewBuilder(semantics)
	var effects []plangraph.Effect
	seen := make(map[objectidentity.Key]bool)
	add := func(ref objectidentity.ID) {
		if !seen[ref.Key()] {
			seen[ref.Key()] = true
			effects = append(effects, plangraph.Effect{Subject: ref, Action: plangraph.Read})
		}
	}
	for _, predicate := range mssqlschema.SortedPredicates(predicates) {
		add(table(semantics, predicate.Table))
		add(function(semantics, predicate.Function))
		for _, argument := range predicate.Arguments {
			if column, ok := mssqlschema.ArgumentColumn(argument); ok {
				add(builder.ColumnParts(predicate.Table.Schema, predicate.Table.Name, column))
			}
		}
	}
	return effects
}

// handoffs orders the plan's table hand-offs between enabled policies: the
// step that releases a table precedes the step that takes it.
func handoffs(steps []*planned, contribution *plangraph.Contribution[featureplan.Operation]) {
	for _, taker := range steps {
		claims := claimed(taker.change)
		for _, giver := range steps {
			if giver == taker || !slices.ContainsFunc(released(giver.change), func(key mssqlschema.ObjectName) bool { return slices.Contains(claims, key) }) {
				continue
			}
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: giver.step.ID, After: taker.step.ID})
			taker.step.Transaction = plangraph.TransactionRequired
			giver.step.Transaction = plangraph.TransactionRequired
		}
	}
}

// claimed and released are the tables a change starts and stops binding while
// enabled, by conflict key.
func claimed(change *mssqldiff.SecurityPolicy) []mssqlschema.ObjectName {
	return subtract(enabledTables(change.After), observedTables(change.Before))
}

func released(change *mssqldiff.SecurityPolicy) []mssqlschema.ObjectName {
	return subtract(observedTables(change.Before), enabledTables(change.After))
}

func enabledTables(policy *mssqlschema.DesiredSecurityPolicy) []mssqlschema.ObjectName {
	if policy == nil {
		return nil
	}
	if enabled, _, _ := policy.Resolved(); !enabled {
		return nil
	}
	return tableKeys(policy.Predicates)
}

func observedTables(policy *mssqlschema.ObservedSecurityPolicy) []mssqlschema.ObjectName {
	if policy == nil || !policy.Enabled {
		return nil
	}
	return tableKeys(policy.Predicates)
}

func tableKeys(predicates []mssqlschema.Predicate) []mssqlschema.ObjectName {
	var keys []mssqlschema.ObjectName
	for _, predicate := range predicates {
		if key := predicate.Table.ConflictKey(); !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	return keys
}

func subtract(from, remove []mssqlschema.ObjectName) []mssqlschema.ObjectName {
	return slices.DeleteFunc(slices.Clone(from), func(key mssqlschema.ObjectName) bool { return slices.Contains(remove, key) })
}

func table(semantics identifier.Semantics, name mssqlschema.ObjectName) objectidentity.ID {
	return objectidentity.NewBuilder(semantics).TableParts(name.Schema, name.Name)
}

func function(semantics identifier.Semantics, name mssqlschema.ObjectName) objectidentity.ID {
	ref := table(semantics, name)
	ref.Kind = objectidentity.KindFunction
	return ref
}

func objectName(ref objectidentity.ID) mssqlschema.ObjectName {
	return mssqlschema.ObjectName{Schema: ref.Schema.Source, Name: ref.Name.Source}
}
