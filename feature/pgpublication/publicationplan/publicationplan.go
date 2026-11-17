// Package publicationplan plans PostgreSQL publication changes into operations
// for a host's dependency graph, for the owner of package pgpublication. It
// performs no I/O.
package publicationplan

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/feature/pgpublication"
	"ptah.run/feature/pgpublication/publicationcompare"
	"ptah.run/feature/pgpublication/publicationrender"
	"ptah.run/internal/objectowner"
	"ptah.run/internal/pgeffects"
)

// Service plans publication changes and declarations. Its zero value is ready
// for concurrent use.
//
// A migration changes publications in [featureplan.PhaseDependent]: after it
// creates and changes tables and roles, so a publication names tables that
// exist and is given to a role that exists, and before it removes any, so a
// table leaves the publication before the table is dropped. Each step reads
// the tables, schemas and role it names, which orders it against the host's
// steps on them. A whole-schema render creates each publication after the
// tables it names.
//
// Within one change the steps run drop, create, add, drop members, options,
// owner. A change between FOR ALL TABLES and a list of members drops the
// publication and creates it again, which a subscriber of the publication
// notices: it is reported as behavioral.
//
// A plan that would write a row filter or a column list is refused, since
// Ptah writes neither and publishing the whole table would publish more than
// the publication does, and so is one that writes TABLES IN SCHEMA or
// publish_generated_columns = stored on a target without the capability. A
// declaration of publish_generated_columns = none on such a target writes
// nothing: the server publishes no generated column. The owner a declaration
// names is checked as every declared owner is (see package objectowner).
type Service struct{}

// PlanFeatures returns one receipt per change in input order, or a completed
// refusal with no usable prefix. Publications belong to no table, so a
// request that names parent kinds is refused.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if !request.Policy.Statements.Valid() {
		return featureplan.Result{}, fmt.Errorf("%w: unknown statement style %d", schemaext.ErrInvalidValue, request.Policy.Statements)
	}
	if len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: a publication belongs to no table", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: pgpublication.Owner}
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		change, ok := record.Value.(*pgpublication.Change)
		if !ok {
			return featureplan.Result{}, fmt.Errorf("%w: unexpected publication change %T", schemaext.ErrInvalidValue, record.Value)
		}
		planner := planner{target: request.Target, caps: request.Capabilities, semantics: request.Identifiers, phase: featureplan.PhaseDependent,
			style: request.Policy.Statements, common: request.CommonSteps}
		receipt, refusal, err := planner.plan(fmt.Sprintf("publication/%06d", index), record.Subject, change, &contribution)
		if err != nil {
			return featureplan.Result{}, err
		}
		if refusal != nil {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: *refusal, Change: new(index)}}}, nil
		}
		result.Changes[index] = receipt
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// PlanDeclarations creates each declared publication for a whole-schema
// render, after the tables it names.
func (Service) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: pgpublication.Owner}
	defaultSchema := publicationcompare.DefaultSchema(request.Identifiers)
	for index, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		declared, ok := object.Value.(*pgpublication.DesiredPublication)
		if !ok {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a declared publication, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		planner := planner{target: request.Target, caps: request.Capabilities, semantics: request.Identifiers, phase: featureplan.PhaseDefault,
			style: featureplan.StatementStyleAnnotated, common: request.CommonSteps}
		change := &pgpublication.Change{After: publicationcompare.Qualify(declared, defaultSchema)}
		receipt, refusal, err := planner.plan(fmt.Sprintf("declared/%06d", index), object.Ref, change, &contribution)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		if refusal != nil {
			// A declaration's refusal names the declared model, which is what
			// the render was handed, not the change it was planned as.
			refusal.Kind = string(pgpublication.PublicationKind)
			return featureplan.DeclarationResult{Complete: true, Diagnostics: []featureplan.DeclarationDiagnostic{{Problem: *refusal, Object: new(index)}}}, nil
		}
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref, Strategy: receipt.Strategy, Steps: receipt.Steps})
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

func validateRequest(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: publication planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if platform.NormalizeDialect(target) != platform.Postgres {
		return fmt.Errorf("%w: PostgreSQL publication planning on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

// planner is what one change is planned with.
type planner struct {
	target    string
	caps      capability.Capabilities
	semantics identifier.Semantics
	phase     featureplan.Phase
	style     featureplan.StatementStyle
	common    []featureplan.CommonStep
}

// plan plans one change as steps name/..., adding them to contribution, or
// returns the refusal of a change Ptah cannot write.
func (p planner) plan(name string, subject objectidentity.ID, change *pgpublication.Change,
	contribution *plangraph.Contribution[featureplan.Operation],
) (featureplan.ChangePlan, *schemavalidation.Diagnostic, error) {
	if err := change.Validate(); err != nil {
		return featureplan.ChangePlan{}, nil, err
	}
	if err := pgpublication.ValidateRef(subject); err != nil {
		return featureplan.ChangePlan{}, nil, err
	}
	publication := pgpublication.Name(subject)
	if refusal := p.refuse(publication, change); refusal != nil {
		return featureplan.ChangePlan{}, refusal, nil
	}
	operations := p.operations(publication, change)
	for _, operation := range operations {
		if err := operation.Validate(); err != nil {
			return featureplan.ChangePlan{}, nil, err
		}
		if err := publicationrender.RefuseUnsupported(p.target, p.caps, operation); err != nil {
			return featureplan.ChangePlan{}, &schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(pgpublication.ChangeKind),
				Object: publication, Feature: capabilityFeature(operation), Message: err.Error()}, nil
		}
	}
	receipt := featureplan.ChangePlan{Subject: subject, Kind: pgpublication.ChangeKind, Strategy: strategy(change)}
	builder := objectidentity.NewBuilder(p.semantics)
	for index, operation := range operations {
		step := plangraph.Step[featureplan.Operation]{ID: plangraph.StepID{Owner: pgpublication.Owner, Name: fmt.Sprintf("%s/%s", name, operation.Action)},
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Phase: p.phase, Payload: operation, Notes: p.notes(publication, operation)},
			Effects:     append([]plangraph.Effect{{Subject: subject, Action: action(operation.Action)}}, reads(builder, operation)...),
			Transaction: plangraph.TransactionAllowed, Impact: operation.Effect()}
		if index != 0 {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: receipt.Steps[index-1], After: step.ID})
		}
		addDependencies(step, p.common, contribution)
		receipt.Steps = append(receipt.Steps, step.ID)
		contribution.Steps = append(contribution.Steps, step)
	}
	return receipt, nil, nil
}

// refuse returns the refusal of a change that would write a row filter or a
// column list, or give the publication to a role no declaration can name. A
// declared filter is refused whatever the change does with it: a creation, a
// rebuild and a member added in place would each write the table without it.
func (p planner) refuse(publication string, change *pgpublication.Change) *schemavalidation.Diagnostic {
	if err := pgpublication.RefuseFiltered(publication, change.After); err != nil {
		return &schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(pgpublication.ChangeKind), Object: publication,
			Feature: "publication row filters and column lists", Message: err.Error()}
	}
	if len(change.Refiltered) != 0 {
		table := change.Refiltered[0].Qualified()
		return &schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(pgpublication.ChangeKind), Object: publication,
			Feature: "publication row filters and column lists",
			Message: fmt.Sprintf("publication %q publishes table %s with another row filter or column list than the desired schema declares; "+
				"Ptah does not write a publication's row filters or column lists, so it cannot change one without publishing more of the "+
				"table than the publication does; a desired schema cannot declare a row filter or a column list, so manage this publication by "+
				"hand, or leave it to a desired schema that declares no publication", publication, table)}
	}
	owner := ""
	if change.After != nil {
		owner = change.After.Owner
	}
	if err := objectowner.ValidateDeclaredOwner(p.target, objectowner.Publication, publication, owner); err != nil {
		return &schemavalidation.Diagnostic{Code: schemavalidation.InvalidSchema, Kind: string(pgpublication.ChangeKind), Object: publication,
			Message: err.Error()}
	}
	return nil
}

// operations are the statements of one change, in the order they run.
func (p planner) operations(publication string, change *pgpublication.Change) []*pgpublication.Operation {
	var operations []*pgpublication.Operation
	if change.After == nil {
		return append(operations, &pgpublication.Operation{Name: publication, Action: pgpublication.Drop, IfExists: p.style != featureplan.StatementStyleExact})
	}
	desired := change.After
	if change.Before == nil || change.Rebuild {
		operations = append(operations, p.creation(publication, change)...)
	} else {
		operations = append(operations, alterations(publication, change)...)
	}
	if change.Owner != "" || (change.Before == nil && desired.Owner != "") {
		operations = append(operations, &pgpublication.Operation{Name: publication, Action: pgpublication.SetOwner, Owner: desired.Owner})
	}
	return operations
}

// generatedColumns is the publish_generated_columns value an operation
// writes: the declared one, and nothing for none on a target without the
// setting, which publishes no generated column.
func (p planner) generatedColumns(declared string) string {
	if declared == pgpublication.GeneratedColumnsNone && !p.caps.Has(capability.PublicationGeneratedColumns) {
		return ""
	}
	return declared
}

// notes describe a step for a reader of the plan; an exact plan leaves them
// out.
func (p planner) notes(publication string, operation *pgpublication.Operation) []string {
	if p.style == featureplan.StatementStyleExact {
		return nil
	}
	switch operation.Action {
	case pgpublication.Drop:
		return []string{fmt.Sprintf("WARNING: Dropping publication '%s' stops logical replication to every subscription that reads it", publication)}
	case pgpublication.DropMembers:
		return []string{fmt.Sprintf("Publication '%s' stops publishing the tables it drops; its subscribers receive no further change to them", publication)}
	default:
		return nil
	}
}

func action(action pgpublication.Action) plangraph.Action {
	switch action {
	case pgpublication.Create:
		return plangraph.Create
	case pgpublication.Drop:
		return plangraph.Drop
	default:
		return plangraph.Alter
	}
}

// creation creates the publication the change declares, after dropping the
// one a rebuild replaces.
func (p planner) creation(publication string, change *pgpublication.Change) []*pgpublication.Operation {
	var operations []*pgpublication.Operation
	if change.Rebuild {
		operations = append(operations, &pgpublication.Operation{Name: publication, Action: pgpublication.Drop})
	}
	desired := change.After
	return append(operations, &pgpublication.Operation{Name: publication, Action: pgpublication.Create, AllTables: desired.AllTables,
		Tables: bare(desired.Tables), Schemas: sorted(desired.Schemas), Publish: canonical(desired),
		ViaPartitionRoot: desired.ViaPartitionRoot, GeneratedColumns: p.generatedColumns(desired.GeneratedColumns)})
}

// alterations change a surviving publication in place: the members it adds,
// then the ones it drops, then its options.
func alterations(publication string, change *pgpublication.Change) []*pgpublication.Operation {
	var operations []*pgpublication.Operation
	if len(change.AddTables) != 0 || len(change.AddSchemas) != 0 {
		operations = append(operations, &pgpublication.Operation{Name: publication, Action: pgpublication.AddMembers,
			Tables: bare(change.AddTables), Schemas: sorted(change.AddSchemas)})
	}
	if len(change.DropTables) != 0 || len(change.DropSchemas) != 0 {
		operations = append(operations, &pgpublication.Operation{Name: publication, Action: pgpublication.DropMembers,
			Tables: bare(change.DropTables), Schemas: sorted(change.DropSchemas)})
	}
	if !change.Options {
		return operations
	}
	// publish_generated_columns is written only where it changes, so a change
	// of another option names no setting the target may lack.
	desired, generated := change.After, ""
	if desired.EffectiveGeneratedColumns() != change.Before.EffectiveGeneratedColumns() {
		generated = desired.EffectiveGeneratedColumns()
	}
	return append(operations, &pgpublication.Operation{Name: publication, Action: pgpublication.SetOptions,
		Publish: desired.EffectivePublish(), ViaPartitionRoot: desired.ViaPartitionRoot, GeneratedColumns: generated})
}

// reads are the objects an operation names besides its publication: its
// tables, its schemas and its owner.
func reads(builder objectidentity.Builder, operation *pgpublication.Operation) []plangraph.Effect {
	var effects []plangraph.Effect
	for _, member := range operation.Tables {
		effects = append(effects, plangraph.Effect{Subject: builder.TableParts(member.Schema, member.Table), Action: plangraph.Read})
	}
	for _, schema := range operation.Schemas {
		effects = append(effects, plangraph.Effect{Subject: pgeffects.Schema(builder, schema), Action: plangraph.Read})
	}
	if operation.Owner != "" {
		effects = append(effects, plangraph.Effect{Subject: builder.Role(operation.Owner), Action: plangraph.Read})
	}
	return effects
}

// addDependencies orders step after a host step that creates or changes what
// it reads, and before one that drops it.
func addDependencies(step plangraph.Step[featureplan.Operation], common []featureplan.CommonStep, contribution *plangraph.Contribution[featureplan.Operation]) {
	for _, shared := range common {
		for _, effect := range shared.Effects {
			for _, read := range step.Effects {
				if read.Action != plangraph.Read || read.Subject.Key() != effect.Subject.Key() {
					continue
				}
				switch effect.Action {
				case plangraph.Create, plangraph.Alter:
					contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: shared.ID, After: step.ID})
				case plangraph.Drop:
					contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: step.ID, After: shared.ID})
				}
			}
		}
	}
}

// capabilityFeature names the clause a capability refusal is about.
func capabilityFeature(operation *pgpublication.Operation) string {
	if len(operation.Schemas) != 0 {
		return "TABLES IN SCHEMA"
	}
	return "publish_generated_columns"
}

func strategy(change *pgpublication.Change) string {
	switch {
	case change.Before == nil:
		return "create the publication after the tables and the role it names"
	case change.After == nil:
		return "drop the publication before the tables it names are removed"
	case change.Rebuild:
		return "drop the publication and create it again in its new membership form"
	default:
		return "change the publication's members, options and owner in place, after the tables and the role they name"
	}
}

// bare returns the members by schema and table alone, sorted.
func bare(members []pgpublication.Member) []pgpublication.Member {
	if len(members) == 0 {
		return nil
	}
	result := make([]pgpublication.Member, 0, len(members))
	for _, member := range members {
		result = append(result, pgpublication.Member{Schema: member.Schema, Table: member.Table})
	}
	return pgpublication.SortMembers(result)
}

func sorted(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	return pgpublication.SortNames(names)
}

// canonical is the publish set a creation writes: nil for the server's
// default, so the statement leaves the option out.
func canonical(desired *pgpublication.DesiredPublication) []string {
	if desired.Publish == nil {
		return nil
	}
	return desired.EffectivePublish()
}
