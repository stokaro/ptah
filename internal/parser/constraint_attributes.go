package parser

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// Deferral timings as the model spells them.
const (
	initiallyDeferred  = "deferred"
	initiallyImmediate = "immediate"
)

// deferral is what the `[NOT] DEFERRABLE` and `INITIALLY DEFERRED | IMMEDIATE`
// clauses after one constraint said. The zero value is a constraint that
// wrote neither.
type deferral struct {
	// deferrable is DEFERRABLE, written or implied: PostgreSQL makes a key
	// that is INITIALLY DEFERRED deferrable without the word.
	deferrable bool
	// initially is the timing written, empty when none was.
	initially string
	// start is where the first clause began, for the messages below.
	start int
	// notDeferrable is NOT DEFERRABLE, written.
	notDeferrable bool
	// written is whether any clause was.
	written bool
	// dialect is the grammar the clauses were read with.
	dialect string
}

// defersChecks reports whether the constraint may have its check put off to
// the end of the transaction. NOT DEFERRABLE and INITIALLY IMMEDIATE only say
// what a constraint is without them.
func (d deferral) defersChecks() bool {
	return d.deferrable
}

// parseDeferral reads the deferral clauses at the cursor, in any order, the way
// PostgreSQL 18.6 reads them: it refuses DEFERRABLE beside NOT DEFERRABLE,
// INITIALLY DEFERRED beside INITIALLY IMMEDIATE, and INITIALLY DEFERRED beside
// NOT DEFERRABLE, and reads INITIALLY DEFERRED alone as deferrable. It leaves
// the cursor where it is when no clause is there.
//
// MySQL 8.4 and MariaDB 11.8 answer ERROR 1064 to every one of the clauses,
// NOT DEFERRABLE included, CockroachDB v26.3.2 answers `unimplemented: this
// syntax` or a syntax error to each of them, and SQLite 3.51 refuses an
// INITIALLY that no DEFERRABLE comes before, so those are refused here too.
func (p *Parser) parseDeferral() (deferral, error) {
	result := deferral{dialect: platform.NormalizeDialect(p.dialect)}
	for {
		p.skipWhitespace()
		start := p.current.Start
		switch {
		case p.current.MatchIdentifierValue("DEFERRABLE"):
			p.advance()
			result.deferrable = true
		case p.current.MatchIdentifierValue("NOT") && p.nextIsDeferrable():
			p.advance()
			p.skipWhitespace()
			p.advance()
			result.notDeferrable = true
		case p.current.MatchIdentifierValue("INITIALLY"):
			timing, err := p.parseInitially(result)
			if err != nil {
				return deferral{}, err
			}
			result.initially = timing
		default:
			if !result.written {
				return result, nil
			}
			return result, result.validate()
		}
		if !result.written {
			result.written, result.start = true, start
		}
		if isMySQLFamilyDialect(result.dialect) {
			return deferral{}, fmt.Errorf(
				"deferral clause at position %d: MySQL and MariaDB have no deferrable constraints, "+
					"and MySQL 8.4 and MariaDB 11.8 answer ERROR 1064 to the clause",
				result.start,
			)
		}
		if result.dialect == platform.CockroachDB {
			return deferral{}, fmt.Errorf(
				"deferral clause at position %d: CockroachDB has no deferrable constraints, "+
					"and v26.3.2 refuses the clause, NOT DEFERRABLE included",
				result.start,
			)
		}
	}
}

// nextIsDeferrable reports whether the word after the cursor is DEFERRABLE.
func (p *Parser) nextIsDeferrable() bool {
	next := p.peekSignificantToken()
	return next.Type == lexer.TokenIdentifier && strings.EqualFold(next.Value, "DEFERRABLE")
}

// parseInitially reads `INITIALLY DEFERRED | IMMEDIATE` and answers the timing.
func (p *Parser) parseInitially(sofar deferral) (string, error) {
	start := p.current.Start
	if sofar.dialect == platform.SQLite && !sofar.written {
		return "", fmt.Errorf("INITIALLY at position %d: SQLite takes it only after [NOT] DEFERRABLE", start)
	}
	p.advance()
	p.skipWhitespace()
	var timing string
	switch {
	case p.current.MatchIdentifierValue("DEFERRED"):
		timing = initiallyDeferred
	case p.current.MatchIdentifierValue("IMMEDIATE"):
		timing = initiallyImmediate
	default:
		return "", fmt.Errorf("expected DEFERRED or IMMEDIATE after INITIALLY at position %d", p.current.Start)
	}
	p.advance()
	if sofar.initially != "" && sofar.initially != timing {
		return "", fmt.Errorf("INITIALLY at position %d: conflicting constraint properties", start)
	}
	return timing, nil
}

// validate refuses the combinations PostgreSQL refuses, makes INITIALLY
// DEFERRED deferrable, and drops the timing of a constraint that is not: a
// constraint that cannot defer checks at once, and a renderer reads a timing
// as a deferrable constraint, so `NOT DEFERRABLE INITIALLY IMMEDIATE` kept as a
// timing renders as `DEFERRABLE INITIALLY IMMEDIATE`.
func (d *deferral) validate() error {
	if d.deferrable && d.notDeferrable {
		return fmt.Errorf("deferral clause at position %d: conflicting constraint properties", d.start)
	}
	if d.initially == initiallyDeferred {
		if d.notDeferrable {
			return fmt.Errorf(
				"deferral clause at position %d: constraint declared INITIALLY DEFERRED must be DEFERRABLE", d.start,
			)
		}
		d.deferrable = true
	}
	if !d.deferrable {
		d.initially = ""
	}
	return nil
}

// applyToForeignKey carries the clauses onto the reference they follow.
func (d deferral) applyToForeignKey(reference *ast.ForeignKeyRef) {
	if !d.written || reference == nil {
		return
	}
	reference.Deferrable = d.deferrable
	reference.Initially = d.initially
}

// applyToKey carries the clauses onto the PRIMARY KEY, UNIQUE or EXCLUDE of
// kind they follow, and refuses them where the dialect takes none there:
// SQLite takes a deferral clause after a foreign key alone.
func (d deferral) applyToKey(constraint *ast.ConstraintNode, kind string) error {
	switch {
	case !d.written:
		return nil
	case d.dialect == platform.SQLite:
		return fmt.Errorf("deferral clause at position %d: SQLite takes one only after a foreign key, not after %s", d.start, kind)
	default:
		constraint.Deferrable = d.deferrable
		constraint.Initially = d.initially
		return nil
	}
}

// refuseOnCheck refuses a deferral clause after a CHECK where the server
// does. PostgreSQL 18.6 answers `CHECK constraints cannot be marked
// DEFERRABLE` and takes NOT DEFERRABLE and INITIALLY IMMEDIATE, which change
// nothing.
func (d deferral) refuseOnCheck() error {
	switch {
	case !d.written:
		return nil
	case d.dialect == platform.SQLite:
		return fmt.Errorf("deferral clause at position %d: SQLite takes one only after a foreign key, not after CHECK", d.start)
	case d.defersChecks():
		return fmt.Errorf("deferral clause at position %d: CHECK constraints cannot be marked DEFERRABLE", d.start)
	default:
		return nil
	}
}

// The kinds of table element a clause after one is judged by. A table
// constraint is named by its keyword; indexElement is an element read as an
// index, which takes none of these clauses.
const (
	indexElement      = "index"
	primaryKeyElement = "PRIMARY KEY"
	uniqueElement     = "UNIQUE"
	excludeElement    = "EXCLUDE"
	foreignKeyElement = "FOREIGN KEY"
	checkElement      = "CHECK"
)

// tableElementKind names a table constraint by its keyword.
func tableElementKind(constraint *ast.ConstraintNode) string {
	switch constraint.Type {
	case ast.PrimaryKeyConstraint:
		return primaryKeyElement
	case ast.UniqueConstraint:
		return uniqueElement
	case ast.ExcludeConstraint:
		return excludeElement
	case ast.ForeignKeyConstraint:
		return foreignKeyElement
	case ast.CheckConstraint:
		return checkElement
	default:
		return ""
	}
}

// readConstraintClauses reads what may follow a table constraint's key or
// expression, in PostgreSQL's order: INCLUDE, the clauses of the index behind
// a key, REFERENCES, the CHECK expression, the EXCLUDE predicate, and the
// deferral clauses with `[NOT] ENFORCED` and `NOT VALID` on either side of
// them.
func (p *Parser) readConstraintClauses(constraint *ast.ConstraintNode, kind string) error {
	if err := p.handleTableConstraintInclude(constraint); err != nil {
		return err
	}
	if err := p.refuseKeyIndexClause(kind); err != nil {
		return err
	}
	if err := p.handleTableForeignKey(constraint); err != nil {
		return err
	}
	if err := p.handleTableCheck(constraint); err != nil {
		return err
	}
	if err := p.handleTableExcludeWhere(constraint); err != nil {
		return err
	}
	var enforced enforcement
	if err := p.readConstraintAttributes(kind, &enforced); err != nil {
		return err
	}
	clauses, err := p.parseDeferral()
	if err != nil {
		return err
	}
	if err := clauses.applyToTableElement(constraint, kind); err != nil {
		return err
	}
	if err := p.readConstraintAttributes(kind, &enforced); err != nil {
		return err
	}
	switch {
	case kind == checkElement:
		constraint.NotEnforced = enforced.notEnforced
	case kind == foreignKeyElement && constraint.Reference != nil:
		constraint.Reference.NotEnforced = enforced.notEnforced
	}
	return nil
}

// applyToTableElement carries the clauses after a table element of kind onto
// it, or refuses them.
func (d deferral) applyToTableElement(constraint *ast.ConstraintNode, kind string) error {
	if !d.written {
		return nil
	}
	switch kind {
	case foreignKeyElement:
		d.applyToForeignKey(constraint.Reference)
		return nil
	case checkElement:
		return d.refuseOnCheck()
	case primaryKeyElement, uniqueElement, excludeElement:
		return d.applyToKey(constraint, kind)
	default:
		return fmt.Errorf("deferral clause at position %d: an index takes none", d.start)
	}
}

// columnClause is the constraint a column definition read last, which a
// deferral clause after it belongs to. The zero value is none that takes one.
type columnClause struct {
	foreignKey bool
	// key is the kind of the constraint for a key or a CHECK the column moved
	// onto the table, empty otherwise.
	key string
	// onTable is whether the key was read as the last table constraint, as a
	// named key and a UNIQUE with a NULLS clause are.
	onTable bool
}

// columnClauses is what a column definition held before one clause was read.
type columnClauses struct {
	foreignKey  *ast.ForeignKeyRef
	unique      bool
	primary     bool
	constraints int
}

// snapshotColumnClauses records what column holds. table is nil where the
// column is read outside a CREATE TABLE, as in ALTER TABLE ... ADD COLUMN.
func snapshotColumnClauses(table *ast.CreateTableNode, column *ast.ColumnNode) columnClauses {
	return columnClauses{
		foreignKey:  column.ForeignKey,
		unique:      column.Unique,
		primary:     column.Primary,
		constraints: tableConstraintCount(table),
	}
}

func tableConstraintCount(table *ast.CreateTableNode) int {
	if table == nil {
		return 0
	}
	return len(table.Constraints)
}

// clauseRead answers which constraint the clause read after b was. A
// named key and a second CHECK the column declares go onto the table, so a new
// table constraint is the one the clause read, of its own kind.
func (b columnClauses) clauseRead(table *ast.CreateTableNode, column *ast.ColumnNode) columnClause {
	switch {
	case column.ForeignKey != b.foreignKey:
		return columnClause{foreignKey: true}
	case column.Primary && !b.primary:
		return columnClause{key: primaryKeyElement}
	case column.Unique && !b.unique:
		return columnClause{key: uniqueElement}
	case tableConstraintCount(table) > b.constraints:
		return columnClause{key: tableElementKind(table.Constraints[len(table.Constraints)-1]), onTable: true}
	default:
		return columnClause{}
	}
}

// parseColumnDeferral reads the deferral clauses in a column definition and
// gives them to the constraint written just before them. PostgreSQL 18.6
// answers `misplaced DEFERRABLE clause` where no key or foreign key comes
// first, as in `a int DEFERRABLE` or `a int NOT NULL DEFERRABLE`.
func (p *Parser) parseColumnDeferral(table *ast.CreateTableNode, column *ast.ColumnNode, last columnClause) error {
	clauses, err := p.parseDeferral()
	if err != nil {
		return err
	}
	// A column's CHECK takes no deferral clause at all: PostgreSQL 18.6 answers
	// `misplaced NOT DEFERRABLE clause` even where a table's CHECK takes one,
	// so the one a column moved onto the table is refused as misplaced too.
	switch {
	case last.foreignKey:
		clauses.applyToForeignKey(column.ForeignKey)
		return nil
	case last.onTable && last.key != checkElement:
		return clauses.applyToTableElement(table.Constraints[len(table.Constraints)-1], last.key)
	case !last.onTable && last.key != "":
		return clauses.liftColumnKey(table, column, last.key)
	default:
		return fmt.Errorf(
			"misplaced deferral clause at position %d: it has to follow the column's PRIMARY KEY, UNIQUE or REFERENCES",
			clauses.start,
		)
	}
}

// refuseKeyIndexClause refuses the clauses PostgreSQL takes for the index
// behind a key, `WITH (storage_parameter = value)` and `USING INDEX TABLESPACE
// name`, by name. The model keeps neither on a key, and PostgreSQL 18.6
// builds the index with them, so reading on would describe another index. An
// kind of any other element is left alone.
func (p *Parser) refuseKeyIndexClause(kind string) error {
	if kind != primaryKeyElement && kind != uniqueElement && kind != excludeElement {
		return nil
	}
	p.skipWhitespace()
	next := p.peekSignificantToken()
	switch {
	case p.current.MatchIdentifierValue("WITH") && next.MatchOperatorValue("("):
		return fmt.Errorf(
			"%s WITH (...) at position %d: storage parameters of the index behind a key are not read; "+
				"set them with ALTER INDEX ... SET in a migration", kind, p.current.Start,
		)
	case p.current.MatchIdentifierValue("USING") && next.MatchIdentifierValue("INDEX"):
		return fmt.Errorf(
			"%s USING INDEX TABLESPACE at position %d: the tablespace of the index behind a key is not read; "+
				"move it with ALTER INDEX ... SET TABLESPACE in a migration", kind, p.current.Start,
		)
	default:
		return nil
	}
}

// handleColumnUnique reads a column's UNIQUE. With a NULLS [NOT] DISTINCT
// clause it is the table's UNIQUE over the column, which is where the model
// keeps the clause: measured on PostgreSQL 18.6, `a int UNIQUE NULLS NOT
// DISTINCT` and `UNIQUE NULLS NOT DISTINCT (a)` on n1 and n2 both build
// `<table>_a_key` as `UNIQUE NULLS NOT DISTINCT (a)`.
func (p *Parser) handleColumnUnique(table *ast.CreateTableNode, column *ast.ColumnNode) error {
	start := p.current.Start
	p.advance()
	p.skipWhitespace()
	nullsDistinct, err := p.parseNullsDistinctClause()
	if err != nil {
		return err
	}
	if nullsDistinct == nil {
		column.SetUnique()
		return nil
	}
	if table == nil {
		return fmt.Errorf(
			"UNIQUE NULLS at position %d: a column's UNIQUE with a NULLS clause is read as the table's UNIQUE over "+
				"the column, and this column has no table to attach it to; add it with ALTER TABLE ... ADD UNIQUE",
			start,
		)
	}
	table.AddConstraint(&ast.ConstraintNode{
		Type:          ast.UniqueConstraint,
		Columns:       []string{column.Name},
		NullsDistinct: nullsDistinct,
	})
	return nil
}

// liftColumnKey gives the clauses to a column's own PRIMARY KEY or UNIQUE of
// kind. A key that defers its check becomes the table's key over the column,
// which is where the model keeps the clause: measured on PostgreSQL 18.6,
// `x int UNIQUE DEFERRABLE` and `UNIQUE (x) DEFERRABLE` both build
// `<table>_x_key` as `UNIQUE (x) DEFERRABLE`, and `x int PRIMARY KEY
// DEFERRABLE` builds `<table>_pkey` as `PRIMARY KEY (x) DEFERRABLE`. A clause
// that says what the key is without it changes nothing, and the key stays on
// the column.
func (d deferral) liftColumnKey(table *ast.CreateTableNode, column *ast.ColumnNode, kind string) error {
	constraint := &ast.ConstraintNode{Columns: []string{column.Name}}
	if err := d.applyToKey(constraint, kind); err != nil || !d.defersChecks() {
		return err
	}
	if table == nil {
		return fmt.Errorf(
			"DEFERRABLE %s at position %d: a column's deferrable key is read as the table's key over the column, "+
				"and this column has no table to attach it to; add it with ALTER TABLE ... ADD %s",
			kind, d.start, kind,
		)
	}
	switch kind {
	case primaryKeyElement:
		constraint.Type = ast.PrimaryKeyConstraint
		column.Primary = false
	default:
		constraint.Type = ast.UniqueConstraint
		column.Unique = false
	}
	table.AddConstraint(constraint)
	return nil
}
