// Package extensionplan plans PostgreSQL extension changes into operations
// for a host's dependency graph, for the owner of package pgextension. It
// performs no I/O.
package extensionplan

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgextension/extensionrender"
)

// Service plans extension changes and declarations. Its zero value is ready
// for concurrent use.
//
// Every operation asks for [featureplan.PhasePrerequisite]: other objects use
// what an extension supplies, so a plan installs and changes extensions before
// it creates anything else and drops them after it removes everything else.
// Within one extension the version moves first, because an update script
// creates its objects in the extension's current schema, then the schema, then
// the comment; a comment the declaration states follows the installation.
//
// Two changes are refused, because the server refuses them: a version lower
// than the installed one, since an extension carries update scripts in one
// direction only, and a move of an extension that is not relocatable.
// CockroachDB and Spanner create an extension and do nothing else with one,
// so an installation into a named schema and every change of an installed
// extension are refused there.
type Service struct{}

// PlanFeatures returns one receipt per change in input order, or a completed
// refusal with no usable prefix. Extensions belong to no table, so a request
// that names parent kinds is refused.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: an extension belongs to no table", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: pgextension.Owner}
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		change, ok := record.Value.(*pgextension.Change)
		if !ok {
			return featureplan.Result{}, fmt.Errorf("%w: unexpected extension change %T", schemaext.ErrInvalidValue, record.Value)
		}
		steps, refusal, err := planChange(request.Target, fmt.Sprintf("extension/%06d", index), record.Subject, change)
		if err != nil {
			return featureplan.Result{}, err
		}
		if refusal != nil {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: *refusal, Change: new(index)}}}, nil
		}
		result.Changes[index] = steps.plan
		contribution.Steps = append(contribution.Steps, steps.steps...)
		contribution.Dependencies = append(contribution.Dependencies, steps.edges...)
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// PlanDeclarations installs each declared extension for a whole-schema
// render, before the objects the render creates, which may use what it
// supplies.
func (Service) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: pgextension.Owner}
	for index, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		declared, ok := object.Value.(*pgextension.DesiredExtension)
		if !ok {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a declared extension, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		steps, refusal, err := planChange(request.Target, fmt.Sprintf("declared/%06d", index), object.Ref, &pgextension.Change{After: declared})
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		if refusal != nil {
			// A declaration's refusal names the declared model, which is what
			// the render was handed, not the change it was planned as.
			refusal.Kind = string(pgextension.ExtensionKind)
			return featureplan.DeclarationResult{Complete: true, Diagnostics: []featureplan.DeclarationDiagnostic{{Problem: *refusal, Object: new(index)}}}, nil
		}
		contribution.Steps = append(contribution.Steps, steps.steps...)
		contribution.Dependencies = append(contribution.Dependencies, steps.edges...)
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref,
			Strategy: "install the extension before the objects that may use what it supplies", Steps: steps.plan.Steps})
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
		return fmt.Errorf("%w: PostgreSQL extension planning on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

// planned is one change's steps in order, the dependencies that keep that
// order, and its receipt.
type planned struct {
	steps []plangraph.Step[featureplan.Operation]
	edges []plangraph.Dependency
	plan  featureplan.ChangePlan
}

// add appends a step after the steps before it.
func (p *planned) add(step plangraph.Step[featureplan.Operation]) {
	if count := len(p.steps); count > 0 {
		p.edges = append(p.edges, plangraph.Dependency{Before: p.steps[count-1].ID, After: step.ID})
	}
	p.steps = append(p.steps, step)
	p.plan.Steps = append(p.plan.Steps, step.ID)
}

// planChange plans one change as steps name/..., or returns the refusal the
// server would answer with.
func planChange(target, name string, subject objectidentity.ID, change *pgextension.Change) (planned, *schemavalidation.Diagnostic, error) {
	if err := change.Validate(); err != nil {
		return planned{}, nil, err
	}
	if err := pgextension.ValidateRef(subject); err != nil {
		return planned{}, nil, err
	}
	if refusal := refuse(target, subject, change); refusal != nil {
		return planned{}, refusal, nil
	}
	extension := pgextension.Name(subject)
	result := planned{plan: featureplan.ChangePlan{Subject: subject, Kind: change.Kind(), Strategy: strategy(change)}}
	step := func(suffix string, action plangraph.Action, payload ast.ExtensionPayload, impact schemaext.Effect, notes ...string) plangraph.Step[featureplan.Operation] {
		return plangraph.Step[featureplan.Operation]{ID: plangraph.StepID{Owner: pgextension.Owner, Name: name + "/" + suffix},
			Payload: featureplan.Operation{Role: ast.StatementExtension, Phase: featureplan.PhasePrerequisite, Payload: payload, Notes: notes},
			Effects: []plangraph.Effect{{Subject: pgextension.Subject(subject), Action: action}}, Transaction: plangraph.TransactionAllowed, Impact: impact}
	}
	behavioral := schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes the version or the schema of an extension and of the objects it owns"}
	switch {
	case change.Before == nil:
		result.add(step("create", plangraph.Create, &pgextension.Operation{Name: extension, Action: pgextension.Create,
			Schema: change.After.Schema, Version: change.After.Version, IfNotExists: change.After.IfNotExists}, change.Effect()))
	case change.After == nil:
		result.add(step("drop", plangraph.Drop, &pgextension.Operation{Name: extension, Action: pgextension.Drop}, change.Effect(),
			fmt.Sprintf("WARNING: Removing extension '%s' may break existing functionality that depends on it", extension),
			"Consider reviewing all database objects that use this extension before proceeding",
			"Extension removal may cascade to dependent objects - review carefully",
			fmt.Sprintf("Remove extension '%s' as it's no longer required by the schema", extension)))
	default:
		if change.UpdatesVersion() {
			to := strings.TrimSpace(change.After.Version)
			result.add(step("update", plangraph.Alter, &pgextension.Operation{Name: extension, Action: pgextension.UpdateVersion, Version: to}, behavioral,
				fmt.Sprintf("Update extension %s from %s to %s", extension, change.Before.Version, to)))
		}
		if change.Schema != "" {
			result.add(step("move", plangraph.Alter, &pgextension.Operation{Name: extension, Action: pgextension.SetSchema, Schema: change.Schema}, behavioral,
				fmt.Sprintf("Move extension %s from schema %s to %s", extension, change.Before.Schema, change.Schema)))
		}
	}
	if change.After != nil && (change.Comment || change.Before == nil && change.After.Comment != "") {
		result.add(step("comment", plangraph.Alter, &pgextension.CommentOperation{Name: extension, Comment: change.After.Comment},
			schemaext.Effect{Impact: schemaext.Additive, Reason: "changes only the comment of an extension"}))
	}
	return result, nil, nil
}

// refuse returns the refusal of a change the server would refuse, or nil.
func refuse(target string, subject objectidentity.ID, change *pgextension.Change) *schemavalidation.Diagnostic {
	extension := pgextension.Name(subject)
	hosts := extensionrender.HostsAlterations(target)
	normalized := platform.NormalizeDialect(target)
	switch {
	case change.Before == nil && change.After.Schema != "" && !hosts:
		return &schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(pgextension.ChangeKind), Object: extension,
			Feature: "PostgreSQL extension installation schemas",
			Message: fmt.Sprintf("%s does not support PostgreSQL extension installation schema %q for extension %q", normalized, change.After.Schema, extension)}
	case change.Before == nil || change.After == nil:
		return nil
	case !hosts:
		return &schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(pgextension.ChangeKind), Object: extension,
			Feature: "ALTER EXTENSION", Message: fmt.Sprintf("%s cannot alter PostgreSQL extension %q; this target has no ALTER EXTENSION", normalized, extension)}
	case change.UpdatesVersion() && isVersionDowngrade(change.Before.Version, change.After.Version):
		return &schemavalidation.Diagnostic{Code: schemavalidation.InvalidSchema, Kind: string(pgextension.ChangeKind), Object: extension,
			Message: fmt.Sprintf("cannot move PostgreSQL extension %q from version %q back to version %q; an extension carries update scripts "+
				"in one direction only, and the server answers \"has no update path\" rather than downgrading",
				extension, change.Before.Version, strings.TrimSpace(change.After.Version))}
	case change.Schema != "" && !change.Before.Relocatable:
		return &schemavalidation.Diagnostic{Code: schemavalidation.InvalidSchema, Kind: string(pgextension.ChangeKind), Object: extension,
			Message: fmt.Sprintf("cannot move PostgreSQL extension %q from schema %q to schema %q; the extension is not relocatable "+
				"and the server answers \"does not support SET SCHEMA\"", extension, change.Before.Schema, change.Schema)}
	default:
		return nil
	}
}

func strategy(change *pgextension.Change) string {
	switch {
	case change.Before == nil:
		return "install the extension before the objects that may use what it supplies"
	case change.After == nil:
		return "drop the extension after the objects that may use what it supplies"
	default:
		return "update the extension's version, then move it, then set its comment, before the objects that may use what it supplies"
	}
}

// isVersionDowngrade reports whether the declared version is demonstrably
// behind the installed one.
//
// An extension version is any string its control file chooses, so this
// claims a downgrade only when both sides are dot-separated numbers and the
// declared one is lower. Anything else -- "1.0-beta", "unstable" -- is left to
// the server, which answers "has no update path" on its own.
func isVersionDowngrade(from, to string) bool {
	fromParts, ok := numericVersion(from)
	if !ok {
		return false
	}
	toParts, ok := numericVersion(to)
	if !ok {
		return false
	}
	for i := 0; i < len(fromParts) && i < len(toParts); i++ {
		if toParts[i] != fromParts[i] {
			return toParts[i] < fromParts[i]
		}
	}
	return len(toParts) < len(fromParts)
}

// numericVersion splits a dot-separated numeric version, reporting whether it
// is one at all.
func numericVersion(version string) ([]int, bool) {
	fields := strings.Split(strings.TrimSpace(version), ".")
	parts := make([]int, 0, len(fields))
	for _, field := range fields {
		value, err := strconv.Atoi(field)
		if err != nil {
			return nil, false
		}
		parts = append(parts, value)
	}
	return parts, len(parts) > 0
}
