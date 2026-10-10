// Package policyplan plans PostgreSQL row-security changes into operations for
// a host's dependency graph, for the owner of package pgpolicy, and accounts for
// a table's policies and switches through the host's operations on the table.
// It performs no I/O.
package policyplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/feature/pgpolicy"
)

// Service plans policy and table-switch changes. Its zero value is ready for
// concurrent use.
//
// Every operation asks for [featureplan.PhaseDependent]: a policy names
// tables, columns, routines and roles, and nothing common reads it. A policy
// that changes is dropped and created again in one step, which requires a
// transaction, because PostgreSQL alters neither a policy's command nor its
// composition in place. A comment the declaration holds is set after the
// policy is created, since a creation or a replacement starts without one.
//
// The owner orders the steps of one table by the access each can change: a
// step that can only narrow access runs before one whose effect is unknown,
// and both before one that can widen it. A plan that runs without a
// transaction then never admits more than its start or its end does: a
// restrictive policy renamed is created before the old one is dropped, a
// permissive one is dropped before the new one is created, and row security is
// enabled before a policy that admits rows is created and disabled only after
// the policies it hid are gone.
//
// A table the plan creates gets its policies from the owner, in steps of the
// dependent phase like every other, and its switches from the statements that
// follow its CREATE TABLE. A whole-schema render has no phases, so there each
// declared policy is created after every common step instead.
type Service struct{}

// planned is one change's steps, with the table they order against and the
// access rank of the step that implements it.
type planned struct {
	steps []plangraph.Step[featureplan.Operation]
	edges []plangraph.Dependency
	plan  featureplan.ChangePlan
	table objectidentity.Key
	main  plangraph.StepID
	rank  int
}

// PlanFeatures returns complete receipts, one per change in input order, and a
// receipt for each table the host creates, drops or alters, or a completed
// refusal with no usable prefix. A successful reply must join the host graph
// before any operation is rendered or executed.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	for _, kind := range request.ParentKinds {
		if kind != pgpolicy.PolicyKind && kind != pgpolicy.TableStateKind {
			return featureplan.Result{}, fmt.Errorf("%w: row-security planning assesses only its own models, not %q", schemaext.ErrInvalidValue, kind)
		}
	}
	roles := objectidentity.NewBuilder(request.Identifiers)
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: pgpolicy.Owner}
	var all []planned
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		step, err := planChange(roles, index, record)
		if err != nil {
			return featureplan.Result{}, err
		}
		result.Changes[index] = step.plan
		contribution.Steps = append(contribution.Steps, step.steps...)
		contribution.Dependencies = append(contribution.Dependencies, step.edges...)
		all = append(all, step)
	}
	contribution.Dependencies = append(contribution.Dependencies, accessOrder(all)...)
	for index, table := range request.Tables {
		switch table.Action {
		case "":
			continue
		case featureplan.CreateTable:
			steps, parents, err := planCreatedTable(roles, index, table, request.ParentKinds)
			if err != nil {
				return featureplan.Result{}, err
			}
			contribution.Steps = append(contribution.Steps, steps.steps...)
			contribution.Dependencies = append(contribution.Dependencies, steps.edges...)
			result.Parents = append(result.Parents, parents...)
		default:
			parents, refused := assessParent(index, table, request.ParentKinds)
			if refused != nil {
				return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{*refused}}, nil
			}
			result.Parents = append(result.Parents, parents...)
		}
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

// PlanDeclarations creates each authored policy for a whole-schema render,
// with its comment after it, once every common step has run: its expressions
// may name any table, view or routine the render creates, and nothing the
// render creates reads a policy. A policy is always a table's child, so the
// request declares its table.
func (Service) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	roles := objectidentity.NewBuilder(request.Identifiers)
	result := featureplan.DeclarationResult{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: pgpolicy.Owner}
	for index, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		declared, ok := object.Value.(*pgpolicy.DesiredPolicy)
		if !ok || declared == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a declared policy, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		step, err := planPolicy(roles, fmt.Sprintf("declared/%06d", index), object.Ref,
			&pgpolicy.PolicyChange{After: declared, Access: pgpolicy.CreatedTableAccess()}, featureplan.PhaseDefault)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		first := step.plan.Steps[0]
		for _, common := range request.CommonSteps {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: common.ID, After: first})
		}
		contribution.Steps = append(contribution.Steps, step.steps...)
		contribution.Dependencies = append(contribution.Dependencies, step.edges...)
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref,
			Strategy: "create the policy after every object the render creates, since its expressions may name any of them", Steps: step.plan.Steps})
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
	if !platform.IsPostgresFamily(target) {
		return fmt.Errorf("%w: PostgreSQL row-security planning on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

func planChange(roles objectidentity.Builder, index int, record schemaext.ChangeRecord) (planned, error) {
	cloned, err := record.Clone()
	if err != nil {
		return planned{}, err
	}
	switch change := cloned.Value.(type) {
	case *pgpolicy.PolicyChange:
		return planPolicy(roles, fmt.Sprintf("policy/%06d", index), cloned.Subject, change, featureplan.PhaseDependent)
	case *pgpolicy.TableStateChange:
		return planTableState(index, cloned.Subject, change)
	default:
		return planned{}, fmt.Errorf("%w: unexpected row-security change %T", schemaext.ErrInvalidValue, cloned.Value)
	}
}

// planPolicy plans one policy change in phase as the steps name and
// name/comment.
func planPolicy(roles objectidentity.Builder, name string, subject objectidentity.ID, change *pgpolicy.PolicyChange, phase featureplan.Phase) (planned, error) {
	if err := change.Validate(); err != nil {
		return planned{}, err
	}
	if err := pgpolicy.ValidatePolicyRef(subject); err != nil {
		return planned{}, err
	}
	table := pgpolicy.Table(subject)
	result := planned{table: table.Key(), rank: accessRank(change.Access.Access)}
	result.plan = featureplan.ChangePlan{Subject: subject, Kind: change.Kind(), Strategy: policyStrategy(change)}
	reads := []plangraph.Effect{{Subject: table, Action: plangraph.Read}}
	if change.After != nil {
		reads = append(reads, roleReads(roles, change.After)...)
	}
	main := plangraph.StepID{Owner: pgpolicy.Owner, Name: name}
	if !change.CommentOnly {
		action, transaction := plangraph.Alter, plangraph.TransactionRequired
		switch {
		case change.Before == nil:
			action, transaction = plangraph.Create, plangraph.TransactionAllowed
		case change.After == nil:
			action, transaction = plangraph.Drop, plangraph.TransactionAllowed
		}
		result.steps = append(result.steps, plangraph.Step[featureplan.Operation]{ID: main,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Phase: phase, Payload: &pgpolicy.PolicyOperation{
				Schema: subject.Schema.Authored(), Table: subject.Parent.Source, Name: subject.Name.Source, Change: *change.Copy()}},
			Effects: append([]plangraph.Effect{{Subject: subject, Action: action}}, reads...), Transaction: transaction, Impact: change.Effect(),
		})
		result.main = main
	}
	// A creation and a replacement start without a comment, so a declared one
	// is set after them; a comment-only change sets it, or clears it, alone.
	if change.After != nil && (change.CommentOnly || change.After.Comment != "") {
		comment := plangraph.StepID{Owner: pgpolicy.Owner, Name: name + "/comment"}
		result.steps = append(result.steps, plangraph.Step[featureplan.Operation]{ID: comment,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Phase: phase, Payload: &pgpolicy.PolicyCommentOperation{
				Schema: subject.Schema.Authored(), Table: subject.Parent.Source, Name: subject.Name.Source, Comment: change.After.Comment}},
			Effects:     []plangraph.Effect{{Subject: subject, Action: plangraph.Alter}, {Subject: table, Action: plangraph.Read}},
			Transaction: plangraph.TransactionAllowed,
			Impact:      schemaext.Effect{Impact: schemaext.Additive, Reason: "sets the comment of a row-security policy"},
		})
		if result.main.Name != "" {
			result.edges = append(result.edges, plangraph.Dependency{Before: result.main, After: comment})
		} else {
			result.main = comment
		}
	}
	for _, step := range result.steps {
		result.plan.Steps = append(result.plan.Steps, step.ID)
	}
	return result, nil
}

// roleReads names the roles a policy's TO list reads. A keyword names no role
// of its own.
func roleReads(builder objectidentity.Builder, policy *pgpolicy.DesiredPolicy) []plangraph.Effect {
	var effects []plangraph.Effect
	for _, role := range pgpolicy.CanonicalRoles(policy.Roles) {
		if role.Name != "" {
			effects = append(effects, plangraph.Effect{Subject: builder.Role(role.Name), Action: plangraph.Read})
		}
	}
	return effects
}

func policyStrategy(change *pgpolicy.PolicyChange) string {
	switch {
	case change.CommentOnly:
		return "set the policy's comment"
	case change.Before == nil:
		return "create the policy"
	case change.After == nil:
		return "drop the policy"
	default:
		return "drop the policy and create it again in one transaction; PostgreSQL alters neither its command nor its composition in place"
	}
}

func planTableState(index int, subject objectidentity.ID, change *pgpolicy.TableStateChange) (planned, error) {
	if err := change.Validate(); err != nil {
		return planned{}, err
	}
	if subject.Kind != objectidentity.KindTable || subject.Name.Empty() {
		return planned{}, fmt.Errorf("%w: a row-security switch change names its table", schemaext.ErrInvalidValue)
	}
	id := plangraph.StepID{Owner: pgpolicy.Owner, Name: fmt.Sprintf("table-state/%06d", index)}
	step := plangraph.Step[featureplan.Operation]{ID: id,
		Payload: featureplan.Operation{Role: ast.StatementExtension, Phase: featureplan.PhaseDependent, Payload: &pgpolicy.TableStateOperation{
			Schema: subject.Schema.Authored(), Table: subject.Name.Source, Change: *change.Copy()}},
		Effects:     []plangraph.Effect{{Subject: pgpolicy.TableStateSubject(subject), Action: plangraph.Alter}, {Subject: subject, Action: plangraph.Read}},
		Transaction: plangraph.TransactionAllowed, Impact: change.Effect(),
	}
	return planned{steps: []plangraph.Step[featureplan.Operation]{step}, table: subject.Key(), main: id, rank: accessRank(change.Access.Access),
		plan: featureplan.ChangePlan{Subject: subject, Kind: change.Kind(), Strategy: "set the table's row-security switches", Steps: []plangraph.StepID{id}}}, nil
}

// accessRank orders steps from the one that can only narrow access to the one
// that can widen it.
func accessRank(access schemaext.Access) int {
	switch access {
	case schemaext.AccessNarrows:
		return 0
	case schemaext.AccessWidens:
		return 2
	default:
		return 1
	}
}

// accessOrder orders the steps of each table by their access rank, a lower
// rank first. Steps of equal rank, and steps of different tables, are left to
// the host.
func accessOrder(all []planned) []plangraph.Dependency {
	var edges []plangraph.Dependency
	for _, before := range all {
		for _, after := range all {
			if before.table == after.table && before.rank < after.rank {
				edges = append(edges, plangraph.Dependency{Before: before.main, After: after.main})
			}
		}
	}
	slices.SortFunc(edges, func(a, b plangraph.Dependency) int {
		if c := compareStep(a.Before, b.Before); c != 0 {
			return c
		}
		return compareStep(a.After, b.After)
	})
	return edges
}

func compareStep(a, b plangraph.StepID) int {
	switch {
	case a.Name < b.Name:
		return -1
	case a.Name > b.Name:
		return 1
	default:
		return 0
	}
}

// planCreatedTable creates the policies of a table the plan creates, in the
// dependent phase like every other policy step, and accounts for both models.
// The CREATE TABLE carries no policy. Its switches are the statements that
// follow it, which the renderer writes from the table's declaration. A new
// table had no rows anyone could read, so every step leaves access unchanged
// and the steps need no access order.
func planCreatedTable(roles objectidentity.Builder, index int, table featureplan.Table, kinds []schemaext.Kind) (planned, []featureplan.ParentPlan, error) {
	objects, err := table.Desired.OwnedObjects.All()
	if err != nil {
		return planned{}, nil, err
	}
	var created planned
	var steps []plangraph.StepID
	for position, object := range objects {
		declared, ok := object.Value.(*pgpolicy.DesiredPolicy)
		if !ok {
			continue
		}
		step, err := planPolicy(roles, fmt.Sprintf("created/%06d/policy/%06d", index, position), object.Ref,
			&pgpolicy.PolicyChange{After: declared, Access: pgpolicy.CreatedTableAccess()}, featureplan.PhaseDependent)
		if err != nil {
			return planned{}, nil, err
		}
		created.steps = append(created.steps, step.steps...)
		created.edges = append(created.edges, step.edges...)
		steps = append(steps, step.plan.Steps...)
	}
	strategies := map[schemaext.Kind]string{
		pgpolicy.PolicyKind:     "create the table's policies after the objects their expressions may name",
		pgpolicy.TableStateKind: "set the table's declared row-security switches in the statements after its CREATE TABLE",
	}
	var receipts []featureplan.ParentPlan
	for _, kind := range kinds {
		receipt := featureplan.ParentPlan{Subject: table.Subject, Kind: kind, Action: table.Action, Strategy: strategies[kind]}
		if kind == pgpolicy.PolicyKind {
			receipt.Steps = steps
		}
		receipts = append(receipts, receipt)
	}
	return created, receipts, nil
}

// assessParent accounts for a table's policies and switches through the
// host's operation on it, one receipt per model the request assigns. DROP
// TABLE removes both with the table. A surviving table keeps both unless a
// planned change in this plan changes them. A rebuild would create the table
// without them, and there is no plan for it here.
func assessParent(index int, table featureplan.Table, kinds []schemaext.Kind) ([]featureplan.ParentPlan, *featureplan.Diagnostic) {
	strategies := map[featureplan.ParentAction]map[schemaext.Kind]string{
		featureplan.DropTable: {
			pgpolicy.PolicyKind:     "DROP TABLE removes the table's policies with it",
			pgpolicy.TableStateKind: "DROP TABLE removes the table's row-security switches with it",
		},
		featureplan.AlterTable: {
			pgpolicy.PolicyKind:     "keep the table's policies unless a planned change in this plan changes them",
			pgpolicy.TableStateKind: "keep the table's row-security switches unless a planned change in this plan changes them",
		},
	}[table.Action]
	if strategies == nil {
		return nil, &featureplan.Diagnostic{Parent: new(index), Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.UnsupportedFeature, Kind: string(pgpolicy.PolicyKind), Object: table.Subject.String(),
			Feature: "row-level security", Message: fmt.Sprintf("Ptah has no plan for a table's row-security state through parent action %q", table.Action),
		}}
	}
	var receipts []featureplan.ParentPlan
	for _, kind := range kinds {
		receipts = append(receipts, featureplan.ParentPlan{Subject: table.Subject, Kind: kind, Action: table.Action, Strategy: strategies[kind]})
	}
	return receipts, nil
}
