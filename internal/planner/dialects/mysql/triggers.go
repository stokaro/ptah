package mysql

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/modelast"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
	"ptah.run/migration/schemadiff/difftypes"
)

// triggerStatement is one trigger change of a plan, with the table it runs
// against and what it does to that table's existing triggers.
type triggerStatement struct {
	node ast.Node
	// table is the trigger's table as the diff spells it.
	table string
	// statements is how many statements the node renders to.
	statements int
	// retires is true when the node removes or replaces a trigger the
	// table already has.
	retires bool
	// installs is true when the node creates or replaces a trigger.
	installs bool
}

// planTriggers emits every trigger change of the plan as one block.
//
// GenerateMigrationAST places the block after columns are added and modified
// and holds column drops back until after it. A trigger body may read its own
// table through NEW and OLD and may write to any other table. The server checks
// only the NEW and OLD references when the trigger is created. So a trigger
// created before a column it writes to is accepted, and every write to its
// table then fails with ERROR 1054 until the column exists. A column dropped
// before the trigger that reads it is replaced breaks writes the same way.
// Measured on MySQL 8.4.11, MySQL 26.7 and MariaDB 11.8.9 (stokaro/ptah#4014).
// Removed triggers belong to the block for that reason: one dropped later than
// the columns it reads leaves the same window.
//
// Inside the block a new trigger is created before an old one is dropped, so a
// body the server refuses leaves the old trigger in place. A trigger whose name
// a removal frees is created last, because MySQL trigger names are unique per
// schema and a trigger that moves to another table has to be dropped first.
//
// On MySQL and MariaDB the block can run under LOCK TABLES; see
// [Planner.triggerSwapLock].
func (p *Planner) planTriggers(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	freed := make(map[string]bool, len(diff.TriggersRemoved))
	for _, ref := range diff.TriggersRemoved {
		freed[triggerNameKey(ref.TriggerName)] = true
	}

	// MariaDB replaces a trigger with CREATE OR REPLACE TRIGGER; MySQL has no
	// such statement and the renderer writes DROP TRIGGER and CREATE TRIGGER.
	replaceStatements := 2
	if p.capabilities().Has(capability.CreateOrReplaceTrigger) {
		replaceStatements = 1
	}

	// The definition travels WITH the entry (stokaro/ptah#2315).
	var created, replaced, dropped, moved []triggerStatement
	for _, ref := range diff.TriggersAdded {
		if ref.Desired.Name == "" {
			continue
		}
		statement := triggerStatement{
			node:       modelast.FromTrigger(ref.Desired),
			table:      ref.TableName,
			statements: 1,
			installs:   true,
		}
		if freed[triggerNameKey(ref.Desired.Name)] {
			moved = append(moved, statement)
			continue
		}
		created = append(created, statement)
	}
	for _, triggerDiff := range diff.TriggersModified {
		if triggerDiff.Desired.Name == "" {
			continue
		}
		replaced = append(replaced, triggerStatement{
			node:       modelast.FromTrigger(triggerDiff.Desired).SetReplace(),
			table:      triggerDiff.TableName,
			statements: replaceStatements,
			retires:    true,
			installs:   true,
		})
	}
	for _, ref := range diff.TriggersRemoved {
		dropped = append(dropped, triggerStatement{
			node:       ast.NewDropTrigger(ref.TriggerName, ref.TableName).SetIfExists(),
			table:      ref.TableName,
			statements: 1,
			retires:    true,
		})
	}

	block := slices.Concat(created, replaced, dropped, moved)
	if len(block) == 0 {
		return result
	}
	lock := p.triggerSwapLock(block, diff)
	if lock != nil {
		result = append(result,
			ast.NewComment("Writes to these tables wait until their triggers are replaced"),
			lock,
		)
	}
	for _, statement := range block {
		result = append(result, statement.node)
	}
	if lock != nil {
		result = append(result, ast.NewRawSQL("UNLOCK TABLES"))
	}
	return result
}

// triggerSwapLock returns the LOCK TABLES statement the trigger block runs
// under, or nil when it needs none.
//
// A table needs the lock when the block runs two or more statements against it
// that both retire a trigger the table already has and install one. Between
// those statements a write sees either no trigger or both of them.
// Measured on MySQL 8.4.11: a row inserted between DROP and CREATE of a
// same-name replacement got no audit row, and a row inserted between CREATE of
// a renamed trigger and DROP of the old one got two. Under LOCK TABLES ...
// WRITE the same insert waits for the metadata lock and fires only the new
// trigger.
//
// Once one table needs the lock, every table the block touches is locked,
// because trigger DDL on a table the session did not lock fails with ERROR 1100
// while the lock is held. Each table is named once. A table spelled bare in one
// entry and qualified with the connection's database in another would
// otherwise appear twice, and LOCK TABLES refuses that with ERROR 1066.
//
// A single changed trigger needs no lock on MariaDB: CREATE OR REPLACE TRIGGER
// is one statement, and it keeps the old trigger when the server refuses the
// new body. Triggers that are only added, or only removed, take no lock
// either: no write sees an old trigger and a new one at once, and a migration
// that only drops triggers can still run inside a transaction file, where the
// migrator refuses LOCK TABLES. SQL Server and Oracle share this planner and
// have no LOCK TABLES.
func (p *Planner) triggerSwapLock(block []triggerStatement, diff *difftypes.SchemaDiff) ast.Node {
	switch p.targetDialect() {
	case platform.MySQL, platform.MariaDB:
	default:
		return nil
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())

	type tableUse struct {
		spelling   string
		statements int
		retires    bool
		installs   bool
	}
	uses := make(map[string]*tableUse, len(block))
	for _, statement := range block {
		key := semantics.QualifiedTableIdentityKey(statement.table)
		use := uses[key]
		if use == nil {
			use = &tableUse{spelling: statement.table}
			uses[key] = use
		}
		use.statements += statement.statements
		use.retires = use.retires || statement.retires
		use.installs = use.installs || statement.installs
	}

	needed := false
	for _, use := range uses {
		if use.statements >= 2 && use.retires && use.installs {
			needed = true
		}
	}
	if !needed {
		return nil
	}
	tables := make([]string, 0, len(uses))
	for _, key := range slices.Sorted(maps.Keys(uses)) {
		tables = append(tables, lockTableName(uses[key].spelling)+" WRITE")
	}
	return ast.NewRawSQL("LOCK TABLES " + strings.Join(tables, ", "))
}

// lockTableName quotes a table the diff spells bare or qualified with its
// database.
func lockTableName(table string) string {
	ref, ok := tableref.Parse(table)
	if !ok {
		return sqlident.Quote(platform.MySQL, table)
	}
	return sqlident.Qualified(platform.MySQL, ref.Schema, ref.Name)
}

// triggerNameKey folds a trigger name for the one question asked of it here:
// could a trigger being created collide with one being dropped? A false match
// only moves a creation after the drops, which is always a valid order, so the
// fold errs toward matching.
func triggerNameKey(name string) string {
	return strings.ToLower(name)
}
