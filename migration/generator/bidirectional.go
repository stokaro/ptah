package generator

import (
	"fmt"
	"maps"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/concurrentindex"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/migration/internal/identifiervalidation"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// ConcurrentIndexMode selects which index changes one direction plans with
// PostgreSQL's CONCURRENTLY modifier.
type ConcurrentIndexMode uint8

const (
	// ConcurrentIndexAutomatic selects concurrent index builds on populated
	// tables and ordinary index drops. It is the native generator's default.
	ConcurrentIndexAutomatic ConcurrentIndexMode = iota
	// ConcurrentIndexDisabled selects no concurrent index operations.
	ConcurrentIndexDisabled
	// ConcurrentIndexAll selects every eligible index operation concurrently.
	// The target must expose the matching capability, and a partitioned parent
	// is refused rather than silently downgraded to a blocking operation.
	ConcurrentIndexAll
)

// BidirectionalPlanPolicy selects concurrent-index behavior for the forward
// migration. The reverse keeps the exact table-qualified index identities but
// selects each concurrent modifier independently from the capability available
// for that reverse operation. A valid rollback therefore falls back to an
// ordinary statement when only the counterpart concurrent operation is absent.
type BidirectionalPlanPolicy struct {
	Create ConcurrentIndexMode
	Drop   ConcurrentIndexMode
}

// SchemaDirectionPlan is one half of a bidirectional schema migration plan.
// Its slices and diff are planning inputs and must be treated as read-only.
type SchemaDirectionPlan struct {
	Diff                    *difftypes.SchemaDiff
	Nodes                   []ast.Node
	ConcurrentIndexRefs     []difftypes.IndexRef
	ConcurrentIndexDropRefs []difftypes.IndexRef
	RequiresNoTransaction   bool
}

// BidirectionalSchemaPlan is one validated forward and reverse schema plan.
//
// DesiredSchema and CurrentSchema are the exact inputs the two directions were
// planned against. They are retained so adapters can apply the same qualifier
// or rendering policy without reconstructing either side. Treat them and both
// direction plans as read-only.
type BidirectionalSchemaPlan struct {
	Dialect       string
	Capabilities  capability.Capabilities
	DesiredSchema *schemamodel.Database
	CurrentSchema *catalog.Database
	Policy        BidirectionalPlanPolicy
	Forward       SchemaDirectionPlan
	Reverse       SchemaDirectionPlan
}

// BidirectionalSchemaPlanOptions contains the complete state needed to plan a
// schema change and the rollback that restores its pre-change state.
type BidirectionalSchemaPlanOptions struct {
	Diff          *difftypes.SchemaDiff
	DesiredSchema *schemamodel.Database
	CurrentSchema *catalog.Database
	Dialect       string
	Capabilities  capability.Capabilities
	Policy        BidirectionalPlanPolicy
}

// PlanBidirectionalSchemaDiff plans a forward schema diff and its exact
// rollback through the same dialect, capabilities, and concurrent-index
// policy.
//
// The reverse direction restores CurrentSchema rather than merely swapping
// structural additions and removals. This preserves prior column and
// constraint definitions, removes MySQL/MariaDB foreign-key backing indexes
// created by the forward migration, and keeps any prior or same-run index whose
// leading key columns cover the foreign key.
// Concurrent index references are table-qualified and correlated exactly
// between directions. A concurrent forward create selects the matching reverse
// drop concurrently when the target supports it and otherwise leaves that
// reverse statement blocking; the same rule applies to a reverse create after
// a concurrent forward drop.
//
// Planning fails before a caller can publish artifacts when an explicitly
// requested concurrent operation is unsupported, targets a PostgreSQL
// partitioned parent, would remove every MySQL/MariaDB foreign-key covering
// index, or cannot be expressed safely by the reverse direction.
func PlanBidirectionalSchemaDiff(
	opts BidirectionalSchemaPlanOptions,
) (*BidirectionalSchemaPlan, error) {
	if opts.Diff == nil {
		return nil, fmt.Errorf("schema diff is required")
	}
	if opts.DesiredSchema == nil {
		return nil, fmt.Errorf("desired schema is required")
	}
	if opts.CurrentSchema == nil {
		return nil, fmt.Errorf("current schema is required")
	}
	dialect := platform.NormalizeDialect(opts.Dialect)
	if dialect == "" {
		return nil, fmt.Errorf("dialect is required")
	}
	caps := opts.Capabilities
	if caps == nil {
		caps = capability.ForDialect(dialect)
	}
	if err := caps.Validate(); err != nil {
		return nil, fmt.Errorf("invalid capabilities for %s: %w", dialect, err)
	}

	createRefs, err := concurrentIndexCreateRefs(
		opts.Diff,
		opts.DesiredSchema,
		opts.CurrentSchema,
		catalog.ServerInfo{Dialect: dialect, Capabilities: caps},
		opts.Policy.Create,
	)
	if err != nil {
		return nil, err
	}
	dropRefs, err := concurrentIndexRemovalRefs(
		opts.Diff,
		opts.CurrentSchema,
		catalog.ServerInfo{Dialect: dialect, Capabilities: caps},
		opts.Policy.Drop,
	)
	if err != nil {
		return nil, err
	}

	return planBidirectionalSchemaDiffWithRefs(opts, dialect, caps, createRefs, dropRefs)
}

func planBidirectionalSchemaDiffWithRefs(
	opts BidirectionalSchemaPlanOptions,
	dialect string,
	caps capability.Capabilities,
	forwardCreateRefs,
	forwardDropRefs []difftypes.IndexRef,
) (*BidirectionalSchemaPlan, error) {
	if err := validateSelectedForwardConcurrentCapabilities(dialect, caps, forwardCreateRefs, forwardDropRefs); err != nil {
		return nil, err
	}
	reverseDiff := reverseSchemaDiffWithSchemaForDialect(
		opts.Diff,
		opts.DesiredSchema,
		opts.CurrentSchema,
		dialect,
	)

	forwardOpts := planner.Options{
		Capabilities:            caps,
		ConcurrentIndexRefs:     forwardCreateRefs,
		ConcurrentIndexDropRefs: forwardDropRefs,
	}
	forwardNodes, err := planner.GenerateSchemaDiffASTWithOptions(opts.Diff, dialect, forwardOpts)
	if err != nil {
		return nil, fmt.Errorf("error planning forward migration: %w", err)
	}
	if err := addMySQLFamilyForeignKeyBackingIndexRemovals(
		reverseDiff,
		opts.Diff,
		opts.CurrentSchema,
		dialect,
		forwardNodes,
	); err != nil {
		return nil, err
	}

	// Re-materialize the sets through the directional diff so duplicate
	// occurrences and deterministic diff order survive the swap. A plain map
	// would collapse equal refs and a bare-name set would cross-match indexes on
	// different MySQL/MariaDB tables. The reverse modifier is capability-selected
	// independently: lack of a counterpart concurrent operation does not make
	// the ordinary reverse statement invalid.
	var reverseCreate, reverseDrop []difftypes.IndexRef
	if caps.Has(capability.CreateIndexConcurrently) {
		reverseCreate = selectIndexRefOccurrences(
			reverseDiff.IndexAdditions(),
			indexRefSet(forwardDropRefs),
		)
	}
	if caps.Has(capability.DropIndexConcurrently) {
		reverseDrop = selectIndexRefOccurrences(
			reverseDiff.IndexRemovals(),
			indexRefSet(forwardCreateRefs),
		)
	}
	// The rollback half of the SQLite virtual-table guard, asked here because
	// this is where the reverse diff exists and is final. The forward direction
	// was gated by sqlitevirtual.ValidatePlannedChanges inside the comparison,
	// and that gate deliberately exempts a table whose only change is added
	// columns -- SQLite performs those in place. Reversal is what breaks the
	// exemption: an added column comes back as a removed one, which SQLite
	// converges by rebuilding the table, and on a database holding a module this
	// build cannot load that rebuild is aimed at storage Ptah cannot tell from
	// an ordinary table (stokaro/ptah#1028).
	//
	// It is asked of both production callers at once. `ptah migrations generate`
	// and ptah-compat `migrate diff` both reach a reverse plan only through this
	// function, and both hand it a diff their diff policy has already filtered.
	if err := sqlitevirtual.ValidatePlannedRollback(
		dialect, opts.CurrentSchema, opts.Diff, reverseDiff,
	); err != nil {
		return nil, err
	}

	reverseOpts := planner.Options{
		Capabilities:            caps,
		ConcurrentIndexRefs:     reverseCreate,
		ConcurrentIndexDropRefs: reverseDrop,
	}
	// The rollback's target is the schema the database currently holds, and it
	// is validated here because this is where that schema exists. The forward
	// direction's target is validated by the comparison that produced the
	// forward diff; a reversal has no comparison of its own, so the assertion
	// would otherwise be made for one direction only (stokaro/ptah#2315).
	if err := validateRollbackTarget(
		dbschematogo.ConvertDBSchemaToGoSchema(opts.CurrentSchema, dialect), reverseDiff, dialect, caps,
	); err != nil {
		return nil, fmt.Errorf("error planning reverse migration: %w", err)
	}
	reverseNodes, err := planner.GenerateSchemaDiffASTWithOptions(reverseDiff, dialect, reverseOpts)
	if err != nil {
		return nil, fmt.Errorf("error planning reverse migration: %w", err)
	}

	return &BidirectionalSchemaPlan{
		Dialect:       dialect,
		Capabilities:  maps.Clone(caps),
		DesiredSchema: opts.DesiredSchema,
		CurrentSchema: opts.CurrentSchema,
		Policy:        opts.Policy,
		Forward: SchemaDirectionPlan{
			Diff:                    opts.Diff,
			Nodes:                   forwardNodes,
			ConcurrentIndexRefs:     append([]difftypes.IndexRef(nil), forwardCreateRefs...),
			ConcurrentIndexDropRefs: append([]difftypes.IndexRef(nil), forwardDropRefs...),
			RequiresNoTransaction:   planner.RequiresNoTransaction(dialect, forwardNodes),
		},
		Reverse: SchemaDirectionPlan{
			Diff:                    reverseDiff,
			Nodes:                   reverseNodes,
			ConcurrentIndexRefs:     reverseCreate,
			ConcurrentIndexDropRefs: reverseDrop,
			RequiresNoTransaction:   planner.RequiresNoTransaction(dialect, reverseNodes),
		},
	}, nil
}

func concurrentIndexCreateRefs(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	current *catalog.Database,
	info catalog.ServerInfo,
	mode ConcurrentIndexMode,
) ([]difftypes.IndexRef, error) {
	switch mode {
	case ConcurrentIndexAutomatic:
		return concurrentindex.MergeRefs(
			concurrentIndexRefsForPopulatedTables(diff, current, info),
			concurrentindex.DeclaredRefs(diff, desired, current, info),
		), nil
	case ConcurrentIndexDisabled:
		return nil, nil
	case ConcurrentIndexAll:
		if !info.Capabilities.Has(capability.CreateIndexConcurrently) {
			return nil, requireConcurrentIndexCapability(
				info.Dialect,
				info.Capabilities,
				capability.CreateIndexConcurrently,
				concurrentIndexCreatePolicy,
			)
		}
		refs := diff.IndexAdditions()
		if err := refusePartitionedConcurrentIndexRefs(refs, current, concurrentIndexCreatePolicy); err != nil {
			return nil, err
		}
		return refs, nil
	default:
		return nil, fmt.Errorf("unknown concurrent index create mode %d", mode)
	}
}

func concurrentIndexRemovalRefs(
	diff *difftypes.SchemaDiff,
	current *catalog.Database,
	info catalog.ServerInfo,
	mode ConcurrentIndexMode,
) ([]difftypes.IndexRef, error) {
	switch mode {
	case ConcurrentIndexAutomatic, ConcurrentIndexDisabled:
		return nil, nil
	case ConcurrentIndexAll:
		if !info.Capabilities.Has(capability.DropIndexConcurrently) {
			return nil, requireConcurrentIndexCapability(
				info.Dialect,
				info.Capabilities,
				capability.DropIndexConcurrently,
				concurrentIndexDropPolicy,
			)
		}
		return concurrentIndexDropRefsForPolicy(
			diff,
			current,
			info,
			DiffPolicy{ConcurrentIndexDrop: true},
		)
	default:
		return nil, fmt.Errorf("unknown concurrent index drop mode %d", mode)
	}
}

func requireConcurrentIndexCapability(
	dialect string,
	caps capability.Capabilities,
	required capability.Capability,
	kind concurrentIndexPolicyKind,
) error {
	if platform.IsPostgresFamily(dialect) && caps.Has(required) {
		return nil
	}
	return fmt.Errorf(
		"%s requested by %s cannot be generated for dialect %q: target capability %s is unavailable",
		kind.statement,
		kind.configKey,
		dialect,
		required,
	)
}

func validateSelectedForwardConcurrentCapabilities(
	dialect string,
	caps capability.Capabilities,
	forwardCreateRefs,
	forwardDropRefs []difftypes.IndexRef,
) error {
	if len(forwardCreateRefs) > 0 {
		if err := requireConcurrentIndexCapability(
			dialect,
			caps,
			capability.CreateIndexConcurrently,
			concurrentIndexCreatePolicy,
		); err != nil {
			return err
		}
	}
	if len(forwardDropRefs) > 0 {
		if err := requireConcurrentIndexCapability(
			dialect,
			caps,
			capability.DropIndexConcurrently,
			concurrentIndexDropPolicy,
		); err != nil {
			return err
		}
	}
	return nil
}

func selectIndexRefOccurrences(
	refs []difftypes.IndexRef,
	selected map[difftypes.IndexRef]struct{},
) []difftypes.IndexRef {
	out := make([]difftypes.IndexRef, 0, len(selected))
	for _, ref := range refs {
		if _, ok := selected[ref]; ok {
			out = append(out, ref)
		}
	}
	return out
}

// validateRollbackTarget refuses a prior schema a rollback could not be written
// against, such as one whose constraint namespace holds a name twice.
//
// prior is the database's own schema in declaration form, which is what a
// rollback restores. Both halves of the target validation are asked, for the
// reason the duplicate-name case shows: a name collision the renderer reports
// is not an identifier question, and asking only the identifier half let a
// rollback be written against a schema that cannot be rendered.
func validateRollbackTarget(
	prior *schemamodel.Database,
	reverseDiff *difftypes.SchemaDiff,
	dialect string,
	caps capability.Capabilities,
) error {
	prepared := schemaprep.QualifyDeclaredUserTypes(
		schemaprep.AssignDefaultForeignKeyNames(prior, dialect),
		dialect,
	)
	if prepared == nil {
		return nil
	}
	if err := identifiervalidation.ValidateTarget(
		prepared,
		dialect,
		reverseDiff.EffectiveIdentifierSemantics(dialect),
	); err != nil {
		return err
	}
	return renderer.ValidateSchemaWithCapabilities(prepared, dialect, caps)
}
