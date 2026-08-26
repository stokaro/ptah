// Package concurrentindex decides which index additions a PostgreSQL-family
// target should build with CREATE INDEX CONCURRENTLY.
//
// It exists because that decision is needed in two places that cannot share a
// caller. The versioned generator writes migration files; the declarative
// surfaces (`schema apply`, `schema diff`, and the plan simulation) render
// statements directly. Both have the same three inputs -- the comparison, the
// desired description, and the live catalog -- and a second implementation of
// the rule is a second answer that can disagree with the first.
//
// The rule cannot live in the planner, which is the layer that looks like it
// should own it. Two of the gates below are facts the planner does not have:
// whether a table is a partitioned parent is a CATALOG fact, and whether the
// operator turned concurrent builds off is a MODE the planner cannot see. A
// planner-level shortcut was built and the generator's own tests refused it on
// exactly those two rows (stokaro/ptah#2019).
package concurrentindex

import (
	"slices"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TableFacts is what the live catalog says about one table, reduced to the two
// answers a concurrency decision needs.
type TableFacts struct {
	// Partitioned reports a PostgreSQL partitioned parent (relkind 'p'), which
	// has no concurrent index form at all.
	Partitioned bool
	// Populated reports a table that holds rows, or one whose row statistics
	// could not be read -- the unknown case counts as populated, because
	// guessing "empty" is the guess that takes the lock.
	Populated bool
}

// TableIndex answers table facts by either spelling an index ref can use.
type TableIndex struct {
	qualified map[string]TableFacts
	bare      map[string]TableFacts
}

// Lookup answers for the qualified spelling first, then the bare one. The
// second return distinguishes "this table is not partitioned" from "the catalog
// does not describe this table", which are different answers to a caller that
// must not build concurrently on a parent it cannot see.
func (idx TableIndex) Lookup(tableName string) (TableFacts, bool) {
	if facts, ok := idx.qualified[tableName]; ok {
		return facts, true
	}
	facts, ok := idx.bare[tableName]
	return facts, ok
}

// IndexTableFacts indexes a read schema by both spellings an index ref can use.
func IndexTableFacts(dbSchema *dbschematypes.DBSchema) TableIndex {
	index := TableIndex{
		qualified: make(map[string]TableFacts),
		bare:      make(map[string]TableFacts),
	}
	if dbSchema == nil {
		return index
	}
	for _, table := range dbSchema.Tables {
		facts := TableFacts{
			Partitioned: table.Partitioned,
			Populated:   table.RowStatsUnknown || table.EstimatedRows > 0,
		}
		mergeTableFacts(index.qualified, table.QualifiedName(), facts)
		if table.Schema != "" {
			mergeTableFacts(index.bare, table.Name, facts)
		}
	}
	return index
}

// mergeTableFacts folds one more candidate into a spelling that several tables
// answer to.
//
// The two halves fold in OPPOSITE directions on purpose. A spelling is treated
// as partitioned only if every table answering to it is, because excluding a
// table that is not a parent costs a lock that was avoidable; and as populated
// if any is, because building concurrently on an empty table costs nothing while
// the reverse takes the lock this exists to avoid.
func mergeTableFacts(into map[string]TableFacts, key string, facts TableFacts) {
	previous, seen := into[key]
	if !seen {
		into[key] = facts
		return
	}
	into[key] = TableFacts{
		Partitioned: previous.Partitioned && facts.Partitioned,
		Populated:   previous.Populated || facts.Populated,
	}
}

// DeclaredRefs is the index additions the DESCRIPTION asked to build
// concurrently.
//
// `CREATE INDEX CONCURRENTLY` survives parsing into
// [go.5x5.cz/ptah/core/goschema.Index.Concurrently], and until this existed
// nothing carried the answer to the planner: a `.sql` desired state asking for
// the non-locking build was planned as a locking one, silently, which on a table
// large enough for the request to be worth making is the difference between a
// migration and an outage (stokaro/ptah#2019).
//
// Two gates apply, and they are the ones the generator's tests already proved
// it needs:
//
//   - a target outside the PostgreSQL family, or one without
//     [capability.CreateIndexConcurrently], keeps the plain build;
//   - a partitioned parent is EXCLUDED rather than refused, because PostgreSQL
//     supports no concurrent form for relkind 'p' and refusing would leave a
//     project with a partitioned table unable to plan an index change at all.
//
// A caller that has turned concurrent builds off must not call this. Turning
// them off is an operator's instruction, and a description does not overrule it.
//
// The "table already holds rows" filter that the generator's heuristic applies
// is deliberately NOT applied here. That heuristic guesses what the operator
// would have wanted; a declaration is not guessing, so an index declared
// concurrent on an empty table is still built concurrently.
func DeclaredRefs(
	diff *difftypes.SchemaDiff,
	desired *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
) []difftypes.IndexRef {
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.CreateIndexConcurrently) {
		return nil
	}
	declared := declaredIdentities(desired, identifier.ForDialect(info.Dialect))
	if len(declared) == 0 {
		return nil
	}
	tables := IndexTableFacts(dbSchema)
	var refs []difftypes.IndexRef
	for _, ref := range diff.IndexAdditions() {
		identity := indexscope.IdentityKeyWithSemantics(identifier.ForDialect(info.Dialect), ref)
		if _, asked := declared[identity]; !asked {
			continue
		}
		if facts, known := tables.Lookup(ref.TableName); known && facts.Partitioned {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

// declaredIdentities is the identity of every index the description asked to
// build concurrently, in the key space index refs use.
//
// The lookup is built here rather than through [indexscope.NewResolver] because
// that constructor VALIDATES the whole diff, and an addition it cannot attribute
// to the target schema is its error to report at planning time, in its own
// words. Reporting it from a policy decision three steps earlier replaces a
// planner's diagnostic with one about concurrency, for a diff that has nothing
// to do with it.
func declaredIdentities(
	desired *goschema.Database,
	semantics identifier.Semantics,
) map[difftypes.IndexRef]struct{} {
	if desired == nil {
		return nil
	}
	owners := goschema.ResolveIndexOwners(desired.Indexes, desired.Tables, desired.MaterializedViews)
	identities := make(map[difftypes.IndexRef]struct{})
	for position, index := range desired.Indexes {
		if !index.Concurrently {
			continue
		}
		identities[indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{
			Name:      index.Name,
			TableName: owners[position],
		})] = struct{}{}
	}
	return identities
}

// MergeRefs is the union of two ref lists, in the order the first names them and
// then the second, without repeating one both name.
func MergeRefs(first, second []difftypes.IndexRef) []difftypes.IndexRef {
	if len(second) == 0 {
		return first
	}
	seen := make(map[difftypes.IndexRef]struct{}, len(first)+len(second))
	merged := make([]difftypes.IndexRef, 0, len(first)+len(second))
	for _, ref := range slices.Concat(first, second) {
		if _, repeated := seen[ref]; repeated {
			continue
		}
		seen[ref] = struct{}{}
		merged = append(merged, ref)
	}
	return merged
}
