package parser

import (
	"fmt"

	"ptah.run/core/platform/capability"
)

// refuseDropCheck refuses ALTER TABLE ... DROP CHECK, written at position, on a
// MySQL-family target that has no such spelling, and answers nil everywhere
// else.
//
// MariaDB has none. Measured on MariaDB 11.8.9 and 12.3.3, after `CREATE TABLE
// c (id int, z int, CONSTRAINT ck CHECK (z > 0))`, `ALTER TABLE c DROP CHECK
// ck` and `DROP CHECK IF EXISTS ck` answer ERROR 1064 (42000), and `DROP
// CONSTRAINT ck` drops the check. Without the refusal the clause is read, and
// `schema apply` plans `c` without `ck` from a MariaDB schema file the server
// refuses to run, where Atlas CE v1.3.0 refuses the file with the server's
// error (stokaro/ptah#3894).
//
// The renderer asks capability.DropCheckClause before it writes the spelling,
// and writes DROP CONSTRAINT where the target lacks it. The parser asks the
// same key, so the spelling Ptah writes for a target is the one it reads.
func (p *Parser) refuseDropCheck(position int) error {
	if !isMySQLFamilyDialect(p.dialect) || p.targetHas(capability.DropCheckClause) {
		return nil
	}
	return fmt.Errorf(
		"DROP CHECK at position %d in ALTER TABLE: %s has no DROP CHECK, and answers ERROR 1064 (42000) "+
			"to one; drop the check with DROP CONSTRAINT",
		position, p.dialect)
}
