package generator

// The transaction split. A CREATE INDEX CONCURRENTLY cannot run inside one, so
// the statements that need to be outside are separated from the statements
// that must stay in.

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/concurrentindex"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/txrequire"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

type splitMigrationNodes struct {
	transactional []ast.Node
	noTransaction []ast.Node
}

func splitNoTransactionNodes(dialect string, nodes []ast.Node) splitMigrationNodes {
	txNodes := make([]ast.Node, 0, len(nodes))
	noTxNodes := make([]ast.Node, 0)
	for _, node := range nodes {
		if planner.NodeRequiresNoTransaction(dialect, node) {
			noTxNodes = append(noTxNodes, node)
			continue
		}
		txNodes = append(txNodes, node)
	}
	return splitMigrationNodes{transactional: txNodes, noTransaction: noTxNodes}
}

// containsUnsplittableNoTransactionNode reports whether any statement that must
// leave the transactional file is one the generator has no ordered place for.
func containsUnsplittableNoTransactionNode(nodes []ast.Node) bool {
	for _, node := range nodes {
		if txrequire.Kind(node) == txrequire.KindUnsplittable {
			return true
		}
	}
	return false
}

// concurrentIndexRefsForPolicy resolves which newly added indexes are built
// concurrently. When the diff policy requests it, every newly added index is
// concurrent (still gated on dialect and the CreateIndexConcurrently
// capability); otherwise the populated-table heuristic applies.
//
// A partitioned parent is refused rather than published: PostgreSQL rejects
// CREATE INDEX CONCURRENTLY on relkind 'p' with SQLSTATE 0A000, so the request
// cannot be honored and silently downgrading it would give a project that asked
// for a non-blocking build a blocking one without saying so. The heuristic path
// downgrades instead -- see [concurrentIndexRefsForPopulatedTables].
func concurrentIndexRefsForPolicy(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
	info catalog.ServerInfo,
	policy DiffPolicy,
) ([]difftypes.IndexRef, error) {
	if !policy.ConcurrentIndex {
		return concurrentIndexRefsForPopulatedTables(diff, dbSchema, info), nil
	}
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.CreateIndexConcurrently) {
		return nil, nil
	}
	refs := diff.IndexAdditions()
	if err := refusePartitionedConcurrentIndexRefs(refs, dbSchema, concurrentIndexCreatePolicy); err != nil {
		return nil, err
	}
	return refs, nil
}

// concurrentIndexPolicyKind names the half of the concurrent-index policy a
// refusal came from, so the diagnostic can quote the configuration key the
// operator would change.
type concurrentIndexPolicyKind struct {
	statement string
	configKey string
}

var (
	concurrentIndexCreatePolicy = concurrentIndexPolicyKind{
		statement: "CREATE INDEX CONCURRENTLY",
		configKey: "diff.concurrent_index.create",
	}
	concurrentIndexDropPolicy = concurrentIndexPolicyKind{
		statement: "DROP INDEX CONCURRENTLY",
		configKey: "diff.concurrent_index.drop",
	}
)

// refusePartitionedConcurrentIndexRefs fails generation before any migration
// file is written when an explicitly requested concurrent index statement names
// a PostgreSQL partitioned parent.
//
// PostgreSQL answers both statements with SQLSTATE 0A000 on relkind 'p'
// ("cannot create index on partitioned table ... concurrently", "cannot drop
// partitioned index ... concurrently"), and it answers at execution time -- so
// without this the plan is written, hashed, and committed, and the failure
// arrives against a production database instead of against the developer who
// generated it.
func refusePartitionedConcurrentIndexRefs(
	refs []difftypes.IndexRef,
	dbSchema *catalog.Database,
	kind concurrentIndexPolicyKind,
) error {
	tables := concurrentindex.IndexTableFacts(dbSchema)
	var offending []string
	for _, ref := range refs {
		facts, known := tables.Lookup(ref.TableName)
		if !known || !facts.Partitioned {
			continue
		}
		offending = append(offending, fmt.Sprintf("%q on %q", ref.Name, ref.TableName))
	}
	if len(offending) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%s requested by %s cannot be generated for partitioned table(s): %s; "+
			"PostgreSQL refuses a concurrent index statement on a partitioned parent (SQLSTATE 0A000). "+
			"Unset %s to generate the plain, transactional statement, or manage the index per partition "+
			"(CREATE INDEX ... ON ONLY the parent, CREATE INDEX CONCURRENTLY on each partition, then ALTER INDEX ... ATTACH PARTITION)",
		kind.statement,
		kind.configKey,
		strings.Join(offending, ", "),
		kind.configKey,
	)
}

// concurrentIndexDropRefsForPolicy resolves which index removals are dropped
// concurrently in the UP direction. Unlike builds there is no populated-table
// heuristic: a concurrent drop happens only when the project asks for one, so
// the default output is byte-identical to before this policy existed.
//
// A removal that is also an addition under the same identity is a redefinition
// whose drop the planner pairs with the rebuild; it is excluded here so the
// pair is never split across a transactional and a non-transactional file.
//
// A UNIQUE constraint's backing index is excluded for a different reason: it is
// not dropped as an index at all (the planner spells it
// ALTER TABLE ... DROP CONSTRAINT), and PostgreSQL has no concurrent form of
// that statement. Routing it into the no-transaction file would also strand the
// marker, which the no-transaction diff does not carry.
func concurrentIndexDropRefsForPolicy(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
	info catalog.ServerInfo,
	policy DiffPolicy,
) ([]difftypes.IndexRef, error) {
	if !policy.ConcurrentIndexDrop {
		return nil, nil
	}
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.DropIndexConcurrently) {
		return nil, nil
	}
	// Match the planner's own redefinition test (indexscope conflict semantics),
	// not plain struct equality: two refs differing only in identifier case are
	// the same index on a case-insensitive target, and treating them as distinct
	// here would route a rebuild's drop into the wrong migration file.
	additions := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(info.Dialect),
		diff.IndexAdditions(),
	)
	constraintBacked := diff.ConstraintBackedIndexRemovalSet()
	var refs []difftypes.IndexRef
	for _, ref := range diff.IndexRemovals() {
		if additions.Contains(ref) {
			continue
		}
		if _, ownedByConstraint := constraintBacked[ref]; ownedByConstraint {
			continue
		}
		refs = append(refs, ref)
	}
	if err := refusePartitionedConcurrentIndexRefs(refs, dbSchema, concurrentIndexDropPolicy); err != nil {
		return nil, err
	}
	return refs, nil
}

// concurrentIndexRefsForPopulatedTables is the default heuristic: build an
// index concurrently when the table it targets already holds rows.
//
// A partitioned parent is excluded rather than refused. Nothing asked for a
// concurrent build here, PostgreSQL supports no concurrent form for relkind
// 'p', and the plain CREATE INDEX the exclusion selects is legal SQL that says
// what it does -- lint reports it as PG101 exactly like any other blocking
// build. Refusing instead would leave a project with a partitioned table unable
// to generate an index migration at all.
func concurrentIndexRefsForPopulatedTables(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
	info catalog.ServerInfo,
) []difftypes.IndexRef {
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.CreateIndexConcurrently) {
		return nil
	}
	tables := concurrentindex.IndexTableFacts(dbSchema)
	var refs []difftypes.IndexRef
	for _, ref := range diff.IndexAdditions() {
		facts, known := tables.Lookup(ref.TableName)
		if !known || facts.Partitioned || !facts.Populated {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

type splitSchemaDiffs struct {
	transactional *difftypes.SchemaDiff
	noTransaction *difftypes.SchemaDiff
}

func splitConcurrentIndexDiff(
	diff *difftypes.SchemaDiff,
	concurrentIndexRefs,
	concurrentIndexDropRefs []difftypes.IndexRef,
) splitSchemaDiffs {
	txDiff := cloneSchemaDiff(diff)
	noTxDiff := &difftypes.SchemaDiff{
		IdentifierSemantics: cloneIdentifierSemantics(diff.IdentifierSemantics),
	}
	addTx, addNoTx := partitionIndexChanges(diff.IndexesAdded, concurrentIndexRefs)
	txDiff.SetIndexAdditions(addTx)
	noTxDiff.SetIndexAdditions(addNoTx)

	// Only rewrite the removal lists when something actually moves. SetIndexRemovals
	// re-sorts, so calling it unconditionally would reorder the drops in every
	// existing split migration for no reason.
	if len(concurrentIndexDropRefs) > 0 {
		dropTx, dropNoTx := partitionIndexRefs(diff.IndexRemovals(), concurrentIndexDropRefs)
		txDiff.SetIndexRemovals(dropTx)
		noTxDiff.SetIndexRemovals(dropNoTx)
	}
	return splitSchemaDiffs{transactional: txDiff, noTransaction: noTxDiff}
}

// partitionIndexRefs splits refs into the ones that stay in the transactional
// migration and the ones that move to the no_transaction migration, preserving
// the input order within each group so file contents stay deterministic.
func partitionIndexRefs(refs, selected []difftypes.IndexRef) (transactional, noTransaction []difftypes.IndexRef) {
	set := indexRefSet(selected)
	for _, ref := range refs {
		if _, ok := set[ref]; ok {
			noTransaction = append(noTransaction, ref)
			continue
		}
		transactional = append(transactional, ref)
	}
	return transactional, noTransaction
}

func indexRefSet(values []difftypes.IndexRef) map[difftypes.IndexRef]struct{} {
	out := make(map[difftypes.IndexRef]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

// partitionIndexChanges splits index ADDITIONS the way partitionIndexRefs
// splits references, keeping each one's declaration with it: the two halves are
// planned by separate migrations and each still has to render a CREATE INDEX
// (stokaro/ptah#2315).
func partitionIndexChanges(
	changes difftypes.IndexChanges,
	selected []difftypes.IndexRef,
) (unselected, matched difftypes.IndexChanges) {
	chosen := make(map[difftypes.IndexRef]struct{}, len(selected))
	for _, ref := range selected {
		chosen[ref] = struct{}{}
	}
	for _, change := range changes {
		ref := difftypes.IndexRef{Name: change.Index.Name, TableName: change.TableName}
		if _, ok := chosen[ref]; ok {
			matched = append(matched, change)
			continue
		}
		unselected = append(unselected, change)
	}
	return unselected, matched
}
