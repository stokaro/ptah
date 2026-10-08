// Package ydb plans a YDB migration from a schema difference.
//
// It is a planner of its own rather than another dialect's under a different
// name. YDB changes a table in place far less than the SQL engines do -- no key
// change, no type change, no SET NOT NULL, and on the older lines no default
// change and no column added with a default -- and the statements it does take
// are not atomic together: a query of several DDL statements keeps the ones
// that ran before a failure. So the planner refuses what YDB cannot do before
// it emits anything, decides every refusal with a capability key rather than
// with the dialect's name, and orders what it emits so that no statement needs
// one that has not run yet:
//
//  1. DROP TRANSFER for every transfer the plan removes, then DROP ASYNC
//     REPLICATION for every replication it removes, before anything a
//     transfer reads or writes goes and before a table is created at a path a
//     replication held;
//  2. DROP VIEW for every view the plan removes or replaces, dependents first,
//     so no table goes while a view the plan touches still reads it;
//     External tables are dropped before their data sources.
//  3. DROP TOPIC and DROP SECRET for removed topics and secrets, then the coordination nodes the
//     plan drops, so a table created under one's path finds the path free.
//     YQL has no statement for a coordination node, so the plan carries Ptah's
//     own, which Ptah's YDB connection runs through the coordination service;
//  4. CREATE TABLE for every added table, with the indexes it gains written
//     inside the statement, because YDB has no CREATE INDEX
//     ([capability.CreateIndexStatement]);
//  5. DROP INDEX for every index the plan removes, before any column it names
//     is dropped (measured: `Impossible drop column because table has an index
//     with that column`, and the same for a covered column);
//  6. RENAME INDEX for every index the plan renames, one per statement
//     (`RENAME INDEX TO can not be used together with another table action`),
//     then ALTER INDEX ... SET for every index whose partitioning changes in
//     place, under the name it has once renamed;
//  7. per table, ADD COLUMN, then the in-place column changes, then SET
//     (TTL = ...) or RESET (TTL), then one ALTER TABLE for its column
//     families, then SET (...) for its partitioning, read replicas and key
//     bloom filter, then DROP COLUMN and comment changes: a TTL may read a column the plan adds,
//     a column the plan adds may move into a family, and YDB refuses to drop
//     the column a TTL reads;
//  8. ADD INDEX for every index added to a table that already exists, one per
//     statement (`Only one index can be added by one operation`), after the
//     columns it names exist, then comment changes for kept, renamed or dropped indexes;
//  9. per table, DROP CHANGEFEED, then ADD CHANGEFEED with the consumers of
//     its topic, then ALTER TOPIC for a retention or a consumer changed in
//     place; a new table's changefeeds follow its CREATE TABLE instead, since
//     YDB adds one only to a table that exists;
//  10. DROP TABLE for every removed table, which drops its changefeeds;
//  11. CREATE TOPIC for every added topic and ALTER TOPIC for every changed
//     one, then CREATE SECRET and the requested ALTER SECRET rotations,
//     then the coordination nodes the plan creates and changes, after
//     the tables are dropped, so an object created under a dropped table's
//     path finds the path free;
//     External data sources and external tables follow their secrets.
//  12. CREATE ASYNC REPLICATION and ALTER ASYNC REPLICATION, then CREATE
//     TRANSFER and ALTER TRANSFER, once the tables, changefeeds, topics and
//     consumers a transfer uses exist and the paths a replication creates its
//     replica tables at are free;
//  13. CREATE VIEW for every view the plan adds or replaces, last, a view after
//     the views it reads: YDB checks a view's query against the schema when
//     the view is created, so the tables and columns it reads exist by then;
//     then the comment of each view the plan keeps whose comment changed.
//
// A comment is a user attribute of its table or view, written by Ptah's own
// COMMENT ON statement (internal/ydbcomment). An object the plan creates
// writes its comment after the statement that creates it.
//
// An index a plan creates, in CREATE TABLE or by ADD INDEX, takes its declared
// partitioning from an ALTER INDEX the renderer writes after it, because no
// statement that creates an index takes the settings.
//
// Users, groups, memberships and permissions are planned around these phases:
// revokes, removed memberships and new or changed principals before them, new
// memberships and grants after them, and dropped principals last. Resource
// pools and their classifiers come between the grants and the dropped
// principals, since a classifier names a user or a group; see
// [Planner.planResourcePools] for their own order.
//
// Each node renders as statements of its own, and the executor runs one per
// query: YDB compiles a query against the schema as it stood before the query,
// so a statement that needs another's effect fails when the two share one.
package ydb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/featureplan"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/modelast"
	"ptah.run/internal/planner/columnchange"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbtype"
	"ptah.run/migration/schemadiff/difftypes"
)

// Planner plans YDB migrations for one capability set.
type Planner struct {
	caps capability.Capabilities
	// rebuild plans a change YDB cannot make in place as a table rebuild;
	// see [Planner.WithTableRebuild].
	rebuild bool
	// rebuildRequest is how the caller asks for a rebuild, as a refusal
	// names it; see [Planner.WithTableRebuildRequest].
	rebuildRequest string
}

// New returns a planner for the newest YDB line Ptah measured.
func New() *Planner { return NewWithCapabilities(capability.YDB262()) }

// NewWithCapabilities returns a planner for a concrete server capability set.
// The set is cloned, so a caller mutating it later does not change a plan.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return &Planner{caps: caps.Clone()}
}

// WithTableRebuild returns a copy of the planner that, when allow is set,
// plans a change YDB cannot make in place -- a changed primary key, a column
// type change, a column made NOT NULL -- as a rebuild of the table: a new
// table, a copy of the rows, and a swap. The steps are not atomic. Without it
// such a change is refused, and the refusal names the flag that asks for it.
func (p *Planner) WithTableRebuild(allow bool) *Planner {
	copied := *p
	copied.rebuild = allow
	return &copied
}

// WithTableRebuildRequest returns a copy of the planner whose refusal of a
// change only a rebuild can make names request as the way to ask for one. A
// surface that reads the request from somewhere other than the native flag
// passes its own spelling; an empty request names [TableRebuildFlag].
func (p *Planner) WithTableRebuildRequest(request string) *Planner {
	copied := *p
	copied.rebuildRequest = request
	return &copied
}

// GenerateMigrationAST returns the nodes that take a YDB database from the
// state diff describes to the declared one, in the order the package
// documentation gives. A change YDB cannot make is refused before any node is
// returned, with an error satisfying errors.Is(err, ptaherr.ErrUnsupportedFeature)
// that names the capability key the target lacks.
func (p *Planner) GenerateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) (plannedNodes []ast.Node, planErr error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	defer func() {
		if err := ctx.Err(); err != nil {
			plannedNodes, planErr = nil, err
		}
	}()

	return p.generateMigrationAST(ctx, runtime, diff)
}

func (p *Planner) generateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	if diff == nil {
		return nil, fmt.Errorf("%w: schema diff is nil", ptaherr.ErrInvalidSchemaDiff)
	}
	if err := schemaprecondition.RefuseServerSchemas(platform.YDB, diff); err != nil {
		return nil, err
	}
	semantics := diff.EffectiveIdentifierSemantics(platform.YDB)
	if err := indexscope.ValidateDiffWithSemantics(platform.YDB, semantics, diff); err != nil {
		return nil, err
	}
	removedTables := tableSet(diff.TablesRemoved.Names(), semantics)
	addedTables := make(map[string]bool, len(diff.TablesAdded))
	for _, creation := range diff.TablesAdded {
		addedTables[semantics.TableIdentityKey(creation.Name)] = true
	}
	rebuilds, err := p.planRebuilds(diff, removedTables, addedTables, semantics)
	if err != nil {
		return nil, err
	}
	scoped := withoutKeysOfRebuiltTables(withoutKeysOfDroppedTables(diff, removedTables, semantics), rebuilds, semantics)
	if err := p.refuseDeclaredObjectChanges(scoped); err != nil {
		return nil, err
	}

	for _, tableDiff := range diff.TablesModified {
		if err := p.refuseModification(tableDiff, rebuilds, semantics, diff.CurrentNotDescribed); err != nil {
			return nil, err
		}
	}
	// The tables whose indexes go inside their CREATE TABLE: every added
	// table, on a target without a CREATE INDEX statement. The render path
	// decides the same question through the same predicate, so `schema
	// render` and a plan write a new table's indexes in the same place.
	inlineIndexes := make(map[string]bool)
	if schemaprep.DeclaresIndexesInCreateTable(p.caps) {
		inlineIndexes = maps.Clone(addedTables)
	}
	// A rebuilt table's indexes are written into its new CREATE TABLE, which
	// the rebuild writes from the whole declaration, so none of them is added
	// on its own.
	ownIndexes := maps.Clone(inlineIndexes)
	for key := range rebuilds {
		ownIndexes[key] = true
	}
	if err := p.refuseUnplannableObjectChanges(diff, ownIndexes, semantics); err != nil {
		return nil, err
	}
	streamBefore, streamAfter, err := p.streamingQueries(diff)
	if err != nil {
		return nil, err
	}
	external, err := p.planExternal(diff)
	if err != nil {
		return nil, err
	}
	columnTTL, err := p.planColumnTTL(diff)
	if err != nil {
		return nil, err
	}
	sequences, err := p.planSerialSequences(diff, rebuilds, semantics)
	if err != nil {
		return nil, err
	}

	access, err := p.planAccess(diff, removedTables, addedTables, slices.Sorted(maps.Keys(rebuilds)), semantics)
	if err != nil {
		return nil, err
	}
	pools, err := p.planResourcePools(diff)
	if err != nil {
		return nil, err
	}

	var result []ast.Node
	result = append(result, streamBefore...)
	result = append(result, dropReplications(diff)...)
	result = append(result, p.dropViews(diff)...)
	result = append(result, access.before...)
	result = append(result, columnTTL.before...)
	result = append(result, removedTablesBeforeSources(diff, external)...)
	result = append(result, external.drops...)
	result = append(result, dropTopics(diff)...)
	nodeChanges, nodeDrops := coordinationNodes(diff)
	result = append(result, nodeDrops...)
	result = append(result, dropSecrets(diff)...)
	earlyTables, lateTables := splitColumnTTLCreations(p.createTables(diff, inlineIndexes, sequences.created, semantics))
	result = append(result, earlyTables...)
	result = append(result, dropIndexes(diff.IndexRemovals(), removedTables, rebuilds, semantics)...)
	result = append(result, renameIndexes(diff.IndexesRenamed, rebuilds, semantics)...)
	result = append(result, changeIndexPartitioning(diff.IndexPartitioningChanged, rebuilds, semantics)...)
	rebuiltNodes, err := p.changeTables(diff, rebuilds, semantics)
	if err != nil {
		return nil, err
	}
	result = append(result, rebuiltNodes...)
	result = append(result, sequences.changed...)
	result = append(result, addIndexes(diff.IndexesAdded, ownIndexes, semantics)...)
	result = append(result, indexComments(diff, removedTables, rebuilds, semantics)...)
	changefeeds, err := p.planFeatureChanges(ctx, runtime, diff, rebuilds, semantics)
	if err != nil {
		return nil, err
	}
	beforeChangefeeds := result
	result = nil
	result = append(result, removedTablesAfterSources(diff, external)...)
	result = append(result, changeTopics(diff)...)
	result = append(result, nodeChanges...)
	result = append(result, changeSecrets(diff)...)
	result = append(result, external.creations...)
	result = append(result, lateTables...)
	result = append(result, columnTTL.after...)
	result = append(result, changeReplications(diff)...)
	result = append(result, p.createViews(diff)...)
	result = append(result, viewComments(diff)...)
	result = append(result, access.after...)
	result = append(result, pools.nodes...)
	result = append(result, streamAfter...)
	result = append(result, access.last...)
	return scheduleChangefeeds(ctx, beforeChangefeeds, result, changefeeds, diff)
}

// refuseUnplannableObjectChanges refuses every index addition, in-place index
// change, changefeed change, topic change and replication or transfer change
// this planner does not plan, in the order [Planner.GenerateMigrationAST]
// reports them.
func (p *Planner) refuseUnplannableObjectChanges(
	diff *difftypes.SchemaDiff,
	ownIndexes map[string]bool,
	semantics identifier.Semantics,
) error {
	if err := p.refuseIndexAdditions(diff, ownIndexes, semantics); err != nil {
		return err
	}
	if err := p.refuseIndexChangesInPlace(diff); err != nil {
		return err
	}
	if err := p.refuseTopicsAndSecrets(diff); err != nil {
		return err
	}
	return p.refuseReplications(diff)
}

// refuseModification refuses what one table's modification asks that the
// plan cannot make: in place, or through the rebuild the plan makes of the
// table, which writes the declared TTL into the new table.
func (p *Planner) refuseModification(
	tableDiff difftypes.TableDiff,
	rebuilds map[string]*tableRebuild,
	semantics identifier.Semantics,
	notDescribed coverage.Set,
) error {
	if err := refuseColumnTTLShape(tableDiff.Desired); err != nil {
		return err
	}
	if err := p.refuseColumnTableChange(tableDiff); err != nil {
		return err
	}
	if _, rebuilt := rebuilds[semantics.TableIdentityKey(tableDiff.TableName)]; rebuilt {
		if err := p.refuseRebuiltTableChanges(tableDiff); err != nil {
			return err
		}
		return p.refuseTTLKey(tableDiff)
	}
	if err := p.refuseTableChanges(tableDiff); err != nil {
		return err
	}
	return p.refuseTTLChange(tableDiff, notDescribed)
}

// changeTables writes each modified table's changes, in place or as a
// rebuild, and then the rebuilds of the tables whose key is their only change.
func (p *Planner) changeTables(
	diff *difftypes.SchemaDiff,
	rebuilds map[string]*tableRebuild,
	semantics identifier.Semantics,
) ([]ast.Node, error) {
	var nodes []ast.Node
	written := make(map[string]bool, len(rebuilds))
	writeRebuild := func(key string) error {
		rebuilt, err := p.rebuildNodes(rebuilds[key])
		if err != nil {
			return err
		}
		nodes = append(nodes, rebuilt...)
		written[key] = true
		return nil
	}
	for _, tableDiff := range diff.TablesModified {
		key := semantics.TableIdentityKey(tableDiff.TableName)
		if _, rebuilt := rebuilds[key]; !rebuilt {
			nodes = append(nodes, p.changeTable(tableDiff, diff.DeclaredUserTypes.Enums)...)
			continue
		}
		if err := writeRebuild(key); err != nil {
			return nil, err
		}
	}
	for _, key := range slices.Sorted(maps.Keys(rebuilds)) {
		if written[key] {
			continue
		}
		if err := writeRebuild(key); err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

// createTables writes each added table, with the indexes the plan gives it
// when inlineIndexes names the table, and after it the ALTER SEQUENCE
// statements sequences holds for it: the sequence exists once the table does,
// and every statement runs as a query of its own.
func (p *Planner) createTables(
	diff *difftypes.SchemaDiff,
	inlineIndexes map[string]bool,
	sequences map[string][]*ast.AlterSerialSequenceNode,
	semantics identifier.Semantics,
) []ast.Node {
	creations := diff.TablesAdded.Qualified(diff.DeclaredUserTypes, platform.YDB).InDependencyOrder()
	nodes := make([]ast.Node, 0, len(creations))
	for _, creation := range creations {
		table := modelast.FromTableWithConstraints(creation.Table, creation.Fields, creation.Enums, platform.YDB, creation.Constraints)
		table.OwnedObjects = creation.OwnedObjects
		key := semantics.TableIdentityKey(creation.Name)
		withoutPlannedSequenceSettings(table, sequences[key])
		if inlineIndexes[key] {
			for _, change := range diff.IndexesAdded {
				if semantics.TableIdentityKey(change.TableName) != key {
					continue
				}
				index := change.Index
				index.TableName = change.TableName
				table.AddIndex(modelast.FromIndex(index))
			}
		}
		nodes = append(nodes, table)
		for _, sequence := range sequences[key] {
			nodes = append(nodes, sequence)
		}
	}
	return nodes
}

// dropIndexes drops each removed index through its table. An index of a table
// the plan drops or rebuilds goes with the table.
func dropIndexes(
	refs []difftypes.IndexRef,
	removedTables map[string]bool,
	rebuilds map[string]*tableRebuild,
	semantics identifier.Semantics,
) []ast.Node {
	var nodes []ast.Node
	for _, ref := range refs {
		key := semantics.TableIdentityKey(ref.TableName)
		if _, rebuilt := rebuilds[key]; rebuilt || removedTables[key] {
			continue
		}
		nodes = append(nodes, ast.NewDropIndex(ref.Name).SetTable(ref.TableName))
	}
	return nodes
}

// renameIndexes renames each index the comparison paired, one per statement.
// An index of a table the plan rebuilds takes its declared name in the new
// table's CREATE TABLE, so it is not renamed on its own.
func renameIndexes(renames []difftypes.IndexRename, rebuilds map[string]*tableRebuild, semantics identifier.Semantics) []ast.Node {
	nodes := make([]ast.Node, 0, len(renames))
	for _, rename := range renames {
		if _, rebuilt := rebuilds[semantics.TableIdentityKey(rename.TableName)]; rebuilt {
			continue
		}
		nodes = append(nodes, &ast.AlterTableNode{
			Name:       rename.TableName,
			Operations: []ast.AlterOperation{&ast.RenameIndexOperation{From: rename.From, To: rename.To}},
		})
	}
	return nodes
}

// changeIndexPartitioning changes each index's partitioning in place, carrying
// the settings it holds so the renderer can keep each one the declaration
// leaves out. An index of a table the plan rebuilds takes its settings in the
// new table, so it is not changed on its own.
func changeIndexPartitioning(
	changes []difftypes.IndexPartitioningChange,
	rebuilds map[string]*tableRebuild,
	semantics identifier.Semantics,
) []ast.Node {
	nodes := make([]ast.Node, 0, len(changes))
	for _, change := range changes {
		if _, rebuilt := rebuilds[semantics.TableIdentityKey(change.TableName)]; rebuilt {
			continue
		}
		nodes = append(nodes, &ast.AlterTableNode{
			Name: change.TableName,
			Operations: []ast.AlterOperation{&ast.SetIndexPartitioningOperation{
				IndexName:    change.Name,
				Partitioning: change.Partitioning.Clone(),
				Previous:     change.Previous.Clone(),
			}},
		})
	}
	return nodes
}

// refuseIndexChangesInPlace refuses, before anything is emitted, a rename or a
// change of partitioning this target cannot make: by
// [capability.IndexRename] and [capability.IndexPartitioning], and a
// declaration YDB refuses whatever the target, which the renderer would
// otherwise refuse after the statements before it were planned.
func (p *Planner) refuseIndexChangesInPlace(diff *difftypes.SchemaDiff) error {
	if len(diff.IndexesRenamed) > 0 && !p.caps.Has(capability.IndexRename) {
		rename := diff.IndexesRenamed[0]
		return refuseKey(capability.IndexRename, fmt.Sprintf("renaming index %q of table %q to %q",
			rename.From, rename.TableName, rename.To))
	}
	for _, change := range diff.IndexPartitioningChanged {
		subject := fmt.Sprintf("index %q of table %q", change.Name, change.TableName)
		if !p.caps.Has(capability.IndexPartitioning) {
			return refuseKey(capability.IndexPartitioning, "changing the partitioning of "+subject)
		}
		previous, err := ydbindex.Held(change.Previous)
		if err != nil {
			return refuseFact(subject, "the settings it holds: "+err.Error())
		}
		if _, err := ydbindex.Resolve(change.Partitioning, previous); err != nil {
			return refuseFact(subject, err.Error())
		}
	}
	return nil
}

// addIndexes adds each index the plan gives a table, one per statement, except
// on the tables inlineIndexes names, whose CREATE TABLE carries them.
func addIndexes(changes difftypes.IndexChanges, inlineIndexes map[string]bool, semantics identifier.Semantics) []ast.Node {
	var nodes []ast.Node
	for _, change := range changes {
		if inlineIndexes[semantics.TableIdentityKey(change.TableName)] {
			continue
		}
		index := change.Index
		index.TableName = change.TableName
		node := modelast.FromIndex(index)
		// YDB's ADD INDEX has no IF NOT EXISTS, and the plan adds an index the
		// comparison found missing, so none is asked for.
		node.IfNotExists = false
		nodes = append(nodes, node)
	}
	return nodes
}

// changeTable writes one table's changes: added columns, then in-place
// changes, then the TTL, then the column families, then the partitioning,
// read replicas and key bloom filter, then dropped columns. The
// drops come after the index drops the plan emitted before it, so an indexed
// or covered column is free by then, and after the TTL, so the column the TTL
// read is free too; the TTL and the families come after the additions, so a
// column the TTL reads exists, and so does a column that moves into a family.
func (p *Planner) changeTable(tableDiff difftypes.TableDiff, enums []schemamodel.Enum) []ast.Node {
	var nodes []ast.Node
	alter := func(operation ast.AlterOperation) {
		nodes = append(nodes, &ast.AlterTableNode{Name: tableDiff.TableName, Operations: []ast.AlterOperation{operation}})
	}
	for _, column := range tableDiff.ColumnsAdded {
		alter(&ast.AddColumnOperation{Column: modelast.FromField(column, enums, platform.YDB)})
	}
	for _, colDiff := range tableDiff.ColumnsModified {
		changed := columnchange.Properties(colDiff)
		if !changed.Any() {
			continue
		}
		alter(&ast.ModifyColumnOperation{
			Column:     modelast.FromField(colDiff.Desired, enums, platform.YDB),
			Changed:    changed,
			HasChanged: true,
		})
	}
	if operation := ttlOperation(tableDiff.RowDeletionPolicyChange); operation != nil {
		alter(operation)
	}
	if operation := familyOperation(tableDiff); operation != nil {
		alter(operation)
	}
	if operation := partitioningOperation(tableDiff.YDBPartitioningChange); operation != nil {
		alter(operation)
	}
	for _, column := range tableDiff.ColumnsRemoved {
		alter(&ast.DropColumnOperation{ColumnName: column.Name})
	}
	for _, operation := range tableComments(tableDiff) {
		alter(operation)
	}
	return nodes
}

// tableSet keys table names by the target's identity rule.
// withoutKeysOfDroppedTables is diff without the primary keys of the tables it
// drops. The reader reports a table's key as a constraint, so a dropped table
// reaches the planner with its key's removal too; DROP TABLE removes the key
// with the table, and refusing that removal as a key change would refuse every
// plan that drops a table.
func withoutKeysOfDroppedTables(
	diff *difftypes.SchemaDiff,
	removedTables map[string]bool,
	semantics identifier.Semantics,
) *difftypes.SchemaDiff {
	scoped := *diff
	scoped.ConstraintsRemoved = slices.DeleteFunc(slices.Clone(diff.ConstraintsRemoved),
		func(removal difftypes.ConstraintRemovalInfo) bool {
			return strings.EqualFold(strings.TrimSpace(removal.Type), "PRIMARY KEY") &&
				removedTables[semantics.TableIdentityKey(removal.TableName)]
		})
	return &scoped
}

func tableSet(names []string, semantics identifier.Semantics) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[semantics.TableIdentityKey(name)] = true
	}
	return set
}

// refuseIndexAdditions refuses, before anything is emitted, an index added
// through ALTER TABLE which YDB would refuse when the plan reaches it: an index
// on a table that exists, or on a new table that inlineIndexes does not name.
// A unique one needs [capability.UniqueIndexOnExistingTable]: measured on every
// line, `Adding a unique index to an existing table is disabled`, even on an
// empty table. Where the plan carries the table's declaration, because it
// changes the table too, the index is held to [ydbindex.ShapeRefusal] the way
// a new table's is. An index added to a table the plan does not otherwise
// change comes with no declaration; schema validation renders that table
// whole, its indexes inside it, and refuses the same shapes before a plan is
// made.
func (p *Planner) refuseIndexAdditions(
	diff *difftypes.SchemaDiff,
	inlineIndexes map[string]bool,
	semantics identifier.Semantics,
) error {
	declarations := make(map[string]schemacapture.TableDeclaration, len(diff.TablesModified))
	for _, tableDiff := range diff.TablesModified {
		if tableDiff.Desired.HasTable() {
			declarations[semantics.TableIdentityKey(tableDiff.TableName)] = tableDiff.Desired
		}
	}
	for _, change := range diff.IndexesAdded {
		table := semantics.TableIdentityKey(change.TableName)
		if inlineIndexes[table] {
			continue
		}
		if change.Index.Unique && !p.caps.Has(capability.UniqueIndexOnExistingTable) {
			return refuseKey(capability.UniqueIndexOnExistingTable, fmt.Sprintf(
				"adding unique index %q to table %q, which exists already", change.Index.Name, change.TableName))
		}
		declaration, declared := declarations[table]
		if !declared {
			continue
		}
		if reason := p.indexShapeRefusal(change.Index, declaration); reason != "" {
			return refuseFact(fmt.Sprintf("adding index %q to table %q", change.Index.Name, change.TableName), reason)
		}
	}
	return nil
}

// indexShapeRefusal asks [ydbindex.ShapeRefusal] about an index on a declared
// table: its key from the table or from its key fields, and each column's
// type through the map the renderer writes with. A column whose type the map
// refuses is answered as orderable, because that refusal is the column's and
// is reported where the column is written. An index whose kind does not read
// is left to the renderer, which refuses it by name.
func (p *Planner) indexShapeRefusal(index schemamodel.Index, declaration schemacapture.TableDeclaration) string {
	kind, err := ydbindex.KindOf(index.Type)
	if err != nil {
		return ""
	}
	columns := make(map[string]ydbindex.Column, len(declaration.Fields))
	var fieldKey []string
	for _, field := range declaration.Fields {
		mapping, err := ydbtype.Map(field.Type, p.caps)
		if err != nil {
			mapping = ydbtype.Mapping{}
		}
		columns[field.Name] = ydbindex.Column{Type: mapping.Type, Dimension: mapping.Dimension}
		if field.Primary {
			fieldKey = append(fieldKey, field.Name)
		}
	}
	key := declaration.Table.PrimaryKey
	if len(key) == 0 {
		key = fieldKey
	}
	column := func(name string) (ydbindex.Column, bool) {
		declared, ok := columns[name]
		return declared, ok
	}
	if kind.IsLocal() != (declaration.Table.YDBColumnTable != nil) {
		return "LOCAL indexes require column storage and GLOBAL indexes require row storage"
	}
	shape := ydbindex.Shape{Kind: kind, Columns: indexKeyColumns(index), Cover: index.IncludeColumns}
	if index.Vector != nil {
		shape.Dimension = index.Vector.Dimension
	}
	return ydbindex.ShapeRefusal(shape, key, column)
}

// indexKeyColumns are the columns an index is keyed on, from its structured
// parts where it has them.
func indexKeyColumns(index schemamodel.Index) []string {
	if len(index.Parts) == 0 {
		return index.Fields
	}
	columns := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		columns = append(columns, part.Name)
	}
	return columns
}

// refuseTableChanges refuses the column changes YDB cannot make in place on
// this target.
func (p *Planner) refuseTableChanges(tableDiff difftypes.TableDiff) error {
	subject := fmt.Sprintf("table %q", tableDiff.TableName)
	if err := p.refuseColumnTableChange(tableDiff); err != nil {
		return err
	}
	if err := p.refuseTableSettings(tableDiff, subject); err != nil {
		return err
	}
	if err := p.refuseFamilyChange(tableDiff); err != nil {
		return err
	}
	if err := p.refusePartitioningChange(tableDiff); err != nil {
		return err
	}
	if tableDiff.Desired.HasTable() && !declaresKey(tableDiff.Desired) {
		return refuseKey(capability.PrimaryKeyRequired, fmt.Sprintf("table %q declares no primary key", tableDiff.TableName))
	}
	for _, column := range tableDiff.ColumnsAdded {
		if err := p.refuseColumnAddition(tableDiff.TableName, column); err != nil {
			return err
		}
	}
	for _, colDiff := range tableDiff.ColumnsModified {
		if err := p.refuseColumnChange(tableDiff.TableName, colDiff); err != nil {
			return err
		}
	}
	return nil
}

// refuseTableSettings refuses the table-level changes a YDB plan makes neither
// in place nor through a rebuild.
func (p *Planner) refuseTableSettings(tableDiff difftypes.TableDiff, subject string) error {
	switch {
	case tableDiff.RowTTLChange != nil:
		return p.keyed(capability.RowLevelTTL, "row-level TTL", "the row-level TTL of "+subject)
	case len(tableDiff.ConstraintsAdded)+len(tableDiff.ConstraintsRemoved) > 0:
		return refuseFact(subject, "YDB has no constraint but the key, and the key never changes")
	}
	return nil
}

// declaresKey reports whether a desired table names a key, on itself or on a
// column.
func declaresKey(declaration schemacapture.TableDeclaration) bool {
	if len(declaration.Table.PrimaryKey) > 0 {
		return true
	}
	return slices.ContainsFunc(declaration.Fields, func(field schemamodel.Field) bool {
		return field.Primary && field.StructName == declaration.Table.StructName
	})
}

// refuseColumnAddition refuses the ADD COLUMN shapes YDB refuses on an existing
// table; the renderer holds the same rules, and stating them here refuses the
// plan before any of it is emitted.
func (p *Planner) refuseColumnAddition(table string, column schemamodel.Field) error {
	subject := fmt.Sprintf("adding column %q to table %q", column.Name, table)
	mapping, err := ydbtype.Map(column.Type, p.caps)
	hasDefault := column.DefaultSet || column.Default != "" || column.DefaultExpr != ""
	switch {
	case column.Primary:
		return p.rebuildable(capability.PrimaryKeyAlterable, "key change", subject+" as part of the key")
	case err == nil && mapping.Serial, column.AutoInc, column.IdentityGeneration != "":
		return refuseFact(subject, "YDB adds no Serial column to an existing table (`Column addition with serial data type is unsupported`)")
	case hasDefault && !p.caps.Has(capability.AddColumnWithDefault):
		return refuseKey(capability.AddColumnWithDefault, subject+" with a default")
	case !hasDefault && !column.Nullable:
		return refuseFact(subject, "YDB adds a NOT NULL column only with a default (`Cannot add not null column without default value`)")
	}
	return nil
}

// refuseColumnChange refuses a modification YDB cannot make in place. The
// change keys are the comparator's; a key this planner does not know is
// refused rather than skipped, because skipping it would report the column
// synced while it is not.
func (p *Planner) refuseColumnChange(table string, colDiff difftypes.ColumnDiff) error {
	subject := fmt.Sprintf("column %q of table %q", colDiff.ColumnName, table)
	if colDiff.NotNullConstraintNameChange != nil {
		return p.keyed(capability.NamedNotNullConstraints, "named NOT NULL constraint", "the NOT NULL name of "+subject)
	}
	for _, key := range sortedKeys(colDiff.Changes) {
		if err := p.refuseChangeKey(subject, key, colDiff); err != nil {
			return err
		}
	}
	return nil
}

// refuseChangeKey refuses one recorded property change of a column.
func (p *Planner) refuseChangeKey(subject, key string, colDiff difftypes.ColumnDiff) error {
	change := colDiff.Changes[key]
	switch key {
	case "primary_key":
		return p.rebuildable(capability.PrimaryKeyAlterable, "key change", "changing whether "+subject+" is part of the key ("+change+")")
	case "type":
		return p.rebuildable(capability.AlterColumnType, "column type change", "changing the type of "+subject+" ("+change+")")
	case "unique":
		return p.keyed(capability.UniqueConstraints, "UNIQUE constraint", "changing whether "+subject+" is UNIQUE ("+change+")")
	case "generated":
		return p.keyed(capability.GeneratedColumns, "generated column", "changing the generation of "+subject)
	case "identity_start", "identity_increment":
		// The sequence's own statement; planSerialSequences refuses what it
		// cannot write.
		return nil
	case "nullable":
		if strings.HasSuffix(change, "-> false") && !p.caps.Has(capability.AlterColumnSetNotNull) {
			return p.rebuildable(capability.AlterColumnSetNotNull, "SET NOT NULL", "making "+subject+" NOT NULL")
		}
		if strings.HasSuffix(change, "-> true") && !p.caps.Has(capability.AlterColumnDropNotNull) {
			return refuseKey(capability.AlterColumnDropNotNull, "making "+subject+" nullable")
		}
		return nil
	case "default", "default_expr":
		if !p.caps.Has(capability.AlterColumnDefault) {
			return refuseKey(capability.AlterColumnDefault, "changing the default of "+subject+" ("+change+")")
		}
		if colDiff.Desired.DefaultExpr != "" && !p.caps.Has(capability.ExpressionDefaults) {
			return refuseKey(capability.ExpressionDefaults, subject+" defaults to the expression "+colDiff.Desired.DefaultExpr)
		}
		return nil
	default:
		return refuseFact(subject, fmt.Sprintf("YDB cannot change %s of a column in place (%s)", key, change))
	}
}

// rebuildable refuses a change YDB can make only by rebuilding the table, when
// the plan was not asked to rebuild it. The refusal is the one [Planner.keyed]
// gives, and it says how to ask for the rebuild.
func (p *Planner) rebuildable(key capability.Capability, feature, subject string) error {
	err := p.keyed(key, feature, subject)
	refusal, ok := errors.AsType[*schemavalidation.RefusalError](err)
	if !ok || p.caps.Has(key) {
		return err
	}
	request := p.rebuildRequest
	if request == "" {
		request = TableRebuildFlag
	}
	diagnostics := refusal.Diagnostics()
	diagnostics[0].Message += "; YDB makes it by rebuilding the table, which Ptah plans when asked with " + request
	return (schemavalidation.Result{Complete: true, Diagnostics: diagnostics}).Err(platform.YDB)
}

// rebuildableFact refuses a change YDB makes only by rebuilding the table and
// no capability key decides, saying reason and how to ask for the rebuild.
func (p *Planner) rebuildableFact(subject, reason string) error {
	request := p.rebuildRequest
	if request == "" {
		request = TableRebuildFlag
	}
	return refuseFact(subject, reason+"; YDB makes it by rebuilding the table, which Ptah plans when asked with "+request)
}

// sortedKeys returns the keys of changes in order, so a refusal names the same
// change every time.
func sortedKeys(changes map[string]string) []string {
	return slices.Sorted(maps.Keys(changes))
}

// keyed refuses through refuseKey when the target lacks key and through
// refuseUnplanned when it holds it.
func (p *Planner) keyed(key capability.Capability, feature, subject string) error {
	if !p.caps.Has(key) {
		return refuseKey(key, subject)
	}
	return refuseUnplanned(feature, subject)
}

func refuseKey(key capability.Capability, subject string) error {
	return planningRefusal(string(key), fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target",
		subject, key, platform.YDB))
}

func refuseUnplanned(feature, subject string) error {
	return planningRefusal(feature, fmt.Sprintf("%s: the %s planner plans no %s", subject, platform.YDB, feature))
}

func refuseFact(subject, reason string) error {
	return planningRefusal(subject, fmt.Sprintf("%s: %s", subject, reason))
}

func planningRefusal(feature, message string) error {
	return (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
		Code: schemavalidation.UnsupportedFeature, Kind: "schema", Feature: feature, Message: message,
	}}}).Err(platform.YDB)
}
