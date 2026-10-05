package ydb

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbpartition"
	"ptah.run/migration/schemadiff/difftypes"
)

// How a YDB plan changes a table's settings: how it splits into partitions,
// its read replicas and its key bloom filter.
//
// `ALTER TABLE t SET (...)` changes them in place, one statement per table. A
// setting the declaration leaves out keeps what the table holds, so the
// statement sets what the declaration names, and names the held value of
// every other setting of each group it touches: setting one resets others
// (see [ydbpartition.TableClause]). The settings depend on no column, so the
// statement goes with the table's other in-place changes.
//
// A starting layout (UNIFORM_PARTITIONS, PARTITION_AT_KEYS) is taken only by
// CREATE TABLE; see [ydbpartition.TableChangeRefusal]. The plan refuses one a
// table was not created with, or rebuilds the table when asked, which writes
// the declared settings, and the held value of every other one, into the new
// table's CREATE TABLE.

// partitioningOperation is the operation that takes a table's settings from
// change.Current to change.Desired, or nil when they do not change.
func partitioningOperation(change *difftypes.YDBTablePartitioningChange) ast.AlterOperation {
	if change == nil {
		return nil
	}
	return &ast.SetYDBTablePartitioningOperation{Partitioning: change.Desired.Clone(), Previous: change.Current.Clone()}
}

// refusePartitioningChange refuses, before anything is emitted, a change of a
// table's settings this target cannot make in place: one needing a capability
// key the target lacks, a declaration YDB refuses, and a starting layout only
// a rebuild gives, whose refusal says how to ask for one.
func (p *Planner) refusePartitioningChange(tableDiff difftypes.TableDiff) error {
	change := tableDiff.YDBPartitioningChange
	if change == nil {
		return nil
	}
	subject := fmt.Sprintf("table %q", tableDiff.TableName)
	desired, current, err := p.resolvePartitioning(subject, change)
	if err != nil {
		return err
	}
	if reason := ydbpartition.TableChangeRefusal(change.Desired, desired, current); reason != "" {
		return p.rebuildableFact(subject, reason)
	}
	return nil
}

// refuseRebuiltPartitioning refuses the settings a rebuilt table cannot take:
// the new table is created with them, so only a key the target lacks and a
// declaration YDB refuses stop it.
func (p *Planner) refuseRebuiltPartitioning(tableDiff difftypes.TableDiff) error {
	if tableDiff.YDBPartitioningChange == nil {
		return nil
	}
	_, _, err := p.resolvePartitioning(fmt.Sprintf("table %q", tableDiff.TableName), tableDiff.YDBPartitioningChange)
	return err
}

// resolvePartitioning reads both sides of a change, refusing a setting the
// target has no key for and a declaration YDB refuses: current as the table
// holds it, and desired over it.
func (p *Planner) resolvePartitioning(
	subject string,
	change *difftypes.YDBTablePartitioningChange,
) (desired, current ydbpartition.TableSettings, _ error) {
	for _, spec := range []*ast.YDBTablePartitioningSpec{change.Desired, change.Current} {
		for _, requirement := range ydbpartition.Requirements(spec) {
			if !p.caps.Has(requirement.Key) {
				return desired, current, refuseKey(requirement.Key, "changing the "+requirement.Settings+" of "+subject)
			}
		}
	}
	current, err := ydbpartition.HeldTable(change.Current)
	if err != nil {
		return desired, current, refuseFact(subject, "the settings it holds: "+err.Error())
	}
	desired, err = ydbpartition.ResolveTable(change.Desired, current)
	if err != nil {
		return desired, current, refuseFact(subject, err.Error())
	}
	return desired, current, nil
}

// partitioningNeedsRebuild reports whether a table's settings change in a way
// only a rebuild makes. A side that does not resolve is refused elsewhere.
func partitioningNeedsRebuild(change *difftypes.YDBTablePartitioningChange) bool {
	if change == nil {
		return false
	}
	current, currentErr := ydbpartition.HeldTable(change.Current)
	desired, desiredErr := ydbpartition.ResolveTable(change.Desired, current)
	return desiredErr == nil && currentErr == nil && ydbpartition.TableChangeRefusal(change.Desired, desired, current) != ""
}
