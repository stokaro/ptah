package generator

// One planned migration before it is written -- what it renders in each
// direction, and how a bidirectional plan is split into subplans.

import (
	"fmt"
	"strings"
	"time"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/txrequire"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/safety"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/shadow"
)

type generatedMigrationSpec struct {
	Version       int64
	Name          string
	UpSQL         string
	DownSQL       string
	Assessments   []safety.StatementAssessment
	NoTransaction bool
}

func planGeneratedMigrationSpecs(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	info catalog.ServerInfo,
	version int64,
	migrationName string,
	policy DiffPolicy,
	qualifier atlasmigrate.Qualifier,
) ([]generatedMigrationSpec, []safety.StatementAssessment, error) {
	// Apply the diff policy once, up front, BEFORE any concurrent-index split.
	// The split separates an index redefinition's added and removed entries into
	// different sub-diffs; if the skip filter ran per sub-diff after the split,
	// it would mistake the orphaned removal for a genuine standalone drop and
	// skip it, silently discarding the redefinition. Filtering here keeps the
	// added/removed pair together, and downstream planning runs with an empty
	// planner-level skip. The omitted changes are surfaced as leading comments.
	var skipped []diffpolicy.SkippedChange
	if skipSet := diffpolicy.NewSkipSet(policy.SkipChangeKinds...); !skipSet.Empty() {
		diff, skipped = diffpolicy.ApplyForDialect(diff, skipSet, info.Dialect)
	}

	bidirectional, err := PlanBidirectionalSchemaDiff(BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: dbSchema,
		Dialect:       info.Dialect,
		Capabilities:  info.Capabilities,
		Policy:        bidirectionalPlanPolicy(policy),
	})
	if err != nil {
		return nil, nil, err
	}
	upNodes := bidirectional.Forward.Nodes
	if len(upNodes) == 0 {
		return nil, nil, nil
	}
	requiresNoTransaction := bidirectional.Forward.RequiresNoTransaction
	if !requiresNoTransaction {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Plan:      bidirectional,
			Qualifier: qualifier,
			Version:   version,
			Name:      migrationName,
		})
		if err != nil || spec.UpSQL == "" {
			return nil, assessments, err
		}
		return withSkipComments([]generatedMigrationSpec{spec}, skipped), assessments, nil
	}

	nodeGroups := splitNoTransactionNodes(info.Dialect, upNodes)
	if len(nodeGroups.transactional) == 0 {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Plan:      bidirectional,
			Qualifier: qualifier,
			Version:   version,
			Name:      migrationName,
		})
		if err != nil || spec.UpSQL == "" {
			return nil, assessments, err
		}
		return withSkipComments([]generatedMigrationSpec{spec}, skipped), assessments, nil
	}
	if containsUnsplittableNoTransactionNode(nodeGroups.noTransaction) {
		return nil, nil, txrequire.UnsplittableMixError(nodeGroups.noTransaction)
	}

	// Two splits, composed, and the order of the files they produce is the
	// order the statements have to run in. Enum values are pulled out first and
	// LEAD, because `ALTER TYPE ... ADD VALUE` must be committed before any
	// statement that uses the value -- PostgreSQL answers 55P04 otherwise. The
	// concurrent indexes come out of what is left and FOLLOW, because an index
	// is built after the table it indexes.
	//
	// A plan with no enum additions produces exactly the two files it produced
	// before: the leading group has no changes and is skipped.
	enumGroups := splitEnumValueAdditionDiff(diff)
	indexGroups := splitConcurrentIndexDiff(
		enumGroups.transactional,
		bidirectional.Forward.ConcurrentIndexRefs,
		bidirectional.Forward.ConcurrentIndexDropRefs,
	)
	ordered := []struct {
		diff   *difftypes.SchemaDiff
		suffix string
	}{
		{enumGroups.noTransaction, "_enum_values"},
		{indexGroups.transactional, "_transactional"},
		{indexGroups.noTransaction, "_concurrent_indexes"},
	}

	specs := make([]generatedMigrationSpec, 0, len(ordered))
	allAssessments := make([]safety.StatementAssessment, 0)
	for _, group := range ordered {
		if !group.diff.HasChanges() {
			continue
		}
		plan, err := bidirectionalSubplan(bidirectional, group.diff)
		if err != nil {
			return nil, nil, err
		}
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Plan:      plan,
			Qualifier: qualifier,
			Version:   version,
			Name:      migrationName + group.suffix,
		})
		if err != nil {
			return nil, nil, err
		}
		if spec.UpSQL == "" {
			continue
		}
		specs = append(specs, spec)
		allAssessments = append(allAssessments, assessments...)
		version++
	}
	return withSkipComments(specs, skipped), allAssessments, nil
}

func bidirectionalPlanPolicy(policy DiffPolicy) BidirectionalPlanPolicy {
	createMode := ConcurrentIndexAutomatic
	if policy.ConcurrentIndex {
		createMode = ConcurrentIndexAll
	}
	dropMode := ConcurrentIndexDisabled
	if policy.ConcurrentIndexDrop {
		dropMode = ConcurrentIndexAll
	}
	return BidirectionalPlanPolicy{Create: createMode, Drop: dropMode}
}

// withSkipComments prepends the diff-policy omission comments to the first
// generated spec so the audit trail is visible in the migration. When every
// change was skipped there is no spec to attach to and the comments are dropped
// along with the (empty) migration.
func withSkipComments(specs []generatedMigrationSpec, skipped []diffpolicy.SkippedChange) []generatedMigrationSpec {
	if len(specs) == 0 || len(skipped) == 0 {
		return specs
	}
	var block strings.Builder
	for _, change := range skipped {
		block.WriteString("-- ")
		block.WriteString(change.Comment())
		block.WriteByte('\n')
	}
	specs[0].UpSQL = block.String() + specs[0].UpSQL
	return specs
}

type generatedMigrationSpecOptions struct {
	Plan      *BidirectionalSchemaPlan
	Version   int64
	Name      string
	Qualifier atlasmigrate.Qualifier
}

func buildGeneratedMigrationSpec(opts generatedMigrationSpecOptions) (generatedMigrationSpec, []safety.StatementAssessment, error) {
	if opts.Plan == nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("bidirectional migration plan is nil")
	}
	upNodes := opts.Plan.Forward.Nodes
	if err := opts.Qualifier.ApplyToPlan(opts.Plan.Dialect, opts.Plan.DesiredSchema, upNodes); err != nil {
		return generatedMigrationSpec{}, nil, err
	}
	assessments, err := safety.AssessRenderedWithCapabilities(
		upNodes,
		opts.Plan.Dialect,
		opts.Plan.Capabilities,
	)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error assessing migration safety: %w", err)
	}
	upDirectiveOpts := generatedDirectiveOptions{skipTimeouts: opts.Plan.Forward.RequiresNoTransaction}
	upSQL, err := renderGeneratedMigrationSQL(
		upNodes,
		opts.Plan.Dialect,
		opts.Plan.Capabilities,
		"UP",
		upDirectiveOpts,
	)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating up migration SQL: %w", err)
	}
	if upSQL == "" {
		return generatedMigrationSpec{}, assessments, nil
	}
	if opts.Plan.Forward.RequiresNoTransaction {
		upSQL = withNoTransactionDirective(upSQL)
	}

	downSQL, err := renderGeneratedDownMigrationSQL(opts.Plan, opts.Qualifier)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	if opts.Plan.Reverse.RequiresNoTransaction {
		downSQL = withNoTransactionDirective(downSQL)
	}

	return generatedMigrationSpec{
		Version:       opts.Version,
		Name:          opts.Name,
		UpSQL:         upSQL,
		DownSQL:       downSQL,
		Assessments:   assessments,
		NoTransaction: opts.Plan.Forward.RequiresNoTransaction || opts.Plan.Reverse.RequiresNoTransaction,
	}, assessments, nil
}

func bidirectionalSubplan(
	full *BidirectionalSchemaPlan,
	diff *difftypes.SchemaDiff,
) (*BidirectionalSchemaPlan, error) {
	createRefs := selectIndexRefOccurrences(
		diff.IndexAdditions(),
		indexRefSet(full.Forward.ConcurrentIndexRefs),
	)
	dropRefs := selectIndexRefOccurrences(
		diff.IndexRemovals(),
		indexRefSet(full.Forward.ConcurrentIndexDropRefs),
	)
	return planBidirectionalSchemaDiffWithRefs(BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: full.DesiredSchema,
		CurrentSchema: full.CurrentSchema,
		Dialect:       full.Dialect,
		Capabilities:  full.Capabilities,
		Policy:        full.Policy,
	}, full.Dialect, full.Capabilities, createRefs, dropRefs)
}

func renderGeneratedDownMigrationSQL(
	plan *BidirectionalSchemaPlan,
	qualifier atlasmigrate.Qualifier,
) (string, error) {
	priorSchema := dbschematogo.ConvertDBSchemaToGoSchema(plan.CurrentSchema)
	nodes := plan.Reverse.Nodes
	if err := qualifier.ApplyToPlan(plan.Dialect, priorSchema, nodes); err != nil {
		return "", err
	}
	output, err := renderer.RenderSQLWithCapabilities(plan.Dialect, plan.Capabilities, nodes...)
	if err != nil {
		return "", err
	}
	statements := sqlutil.SplitSQLStatementsForDialect(output, plan.Dialect)
	if len(statements) == 0 {
		return fmt.Sprintf(
			"-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n-- No rollback operations needed\n",
			time.Now().Format(time.RFC3339),
		), nil
	}
	header := fmt.Sprintf(
		"-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n",
		time.Now().Format(time.RFC3339),
	)
	directives := generatedDirectiveOptions{skipTimeouts: plan.Reverse.RequiresNoTransaction}
	return withGeneratedTimeoutDirectivesForOptions(
		header+strings.Join(statements, ";\n")+";",
		plan.Dialect,
		directives,
	), nil
}

func renderGeneratedMigrationSQL(
	nodes []ast.Node,
	dialect string,
	caps capability.Capabilities,
	direction string,
	directiveOpts generatedDirectiveOptions,
) (string, error) {
	rawSQL, err := renderer.RenderSQLWithCapabilities(dialect, caps, nodes...)
	if err != nil {
		return "", err
	}
	statements := sqlutil.SplitSQLStatementsForDialect(rawSQL, dialect)
	if len(statements) == 0 || !hasActualSQLStatements(statements) {
		return "", nil
	}
	header := fmt.Sprintf("-- Migration generated from schema differences\n-- Generated on: %s\n-- Direction: %s\n\n",
		time.Now().Format(time.RFC3339), direction)
	return withGeneratedTimeoutDirectivesForOptions(header+strings.Join(statements, ";\n")+";", dialect, directiveOpts), nil
}

func shadowCandidatesFromSpecs(specs []generatedMigrationSpec) []shadow.Candidate {
	candidates := make([]shadow.Candidate, 0, len(specs))
	for _, spec := range specs {
		candidates = append(candidates, shadow.Candidate{
			Version: spec.Version,
			Name:    spec.Name,
			UpSQL:   spec.UpSQL,
			DownSQL: spec.DownSQL,
		})
	}
	return candidates
}

func migrationFilesFromPairs(pairs []MigrationFilePair) *MigrationFiles {
	if len(pairs) == 0 {
		return nil
	}
	return &MigrationFiles{
		Files: pairs,
	}
}
