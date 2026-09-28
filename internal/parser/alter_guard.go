package parser

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// An existence guard inside ALTER TABLE.
//
// MySQL takes none. Measured on MySQL 8.4.11 and 26.7.0 and on MariaDB 11.8.9
// and 12.3.3, after `CREATE TABLE c (id int, x int, z int, w int, KEY ix (z),
// CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id), CONSTRAINT ck CHECK (z >
// 0))`, each clause below as `ALTER TABLE c <clause>`:
//
//	clause                          MySQL 8.4.11, 26.7.0  MariaDB 11.8.9, 12.3.3
//	DROP INDEX IF EXISTS ix         ERROR 1064            accepted
//	DROP KEY IF EXISTS ix           ERROR 1064            accepted
//	DROP FOREIGN KEY IF EXISTS fk   ERROR 1064            accepted
//	DROP CONSTRAINT IF EXISTS ck    ERROR 1064            accepted
//	DROP CHECK IF EXISTS ck         ERROR 1064            ERROR 1064
//	DROP COLUMN IF EXISTS w         ERROR 1064            accepted
//	DROP IF EXISTS w                ERROR 1064            accepted
//	ADD COLUMN IF NOT EXISTS y int  ERROR 1064            accepted
//
// Without the refusal the guard is read, and it changes the desired schema of
// a file the server refuses to run: against a MySQL database, `ptah-compat
// schema apply` plans `c` without `ix`, where Atlas CE v1.3.0 refuses the file
// with the server's ERROR 1064 (stokaro/ptah#3877). MariaDB has no DROP CHECK
// spelling with a guard or without one (stokaro/ptah#3894).
//
// The index and constraint guards are the ones the renderer writes, and it
// decides whether to write each by the capability keys DropIndexIfExists and
// DropConstraintIfExists. The parser asks the same keys, of its capability set
// where that set answers them and of the dialect's default where it does not,
// so a guard Ptah writes for a target is one it reads for that target. No key
// names the column guards, which no MySQL line takes and no MySQL-family
// renderer writes, so the dialect decides those.

// refuseAlterGuard refuses the existence guard written at position in an ALTER
// TABLE clause on a MySQL-family target that does not take it, and answers nil
// everywhere else. key is the capability that says whether the target takes
// the guard, or empty for a column guard, which MySQL refuses and MariaDB takes.
func (p *Parser) refuseAlterGuard(guard, clause string, key capability.Capability, position int) error {
	if !isMySQLFamilyDialect(p.dialect) || p.takesAlterGuard(key) {
		return nil
	}
	return fmt.Errorf(
		"%s at position %d in ALTER TABLE ... %s: %s takes no %s on %s, and answers ERROR 1064 (42000) "+
			"to one; write the clause without it",
		guard, position, clause, p.dialect, guard, clause)
}

// takesAlterGuard answers whether this parser's MySQL-family target takes the
// existence guard key names, or a column guard when key is empty.
func (p *Parser) takesAlterGuard(key capability.Capability) bool {
	if key == "" {
		return p.dialect == platform.MariaDB
	}
	if p.capabilities.Established(key) {
		return p.capabilities.Has(key)
	}
	return capability.ForDialect(p.dialect).Has(key)
}
