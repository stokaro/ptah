package parser

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// unmodeledClauses is the issue that records the clauses the reader refuses
// because the model has no field for what they declare.
const unmodeledClauses = "stokaro/ptah#3853"

// readConstraintAttributes reads `ENFORCED`, `NOT ENFORCED` and `NOT VALID`
// after a table constraint of kind, and leaves the cursor on anything else. It
// is read before the deferral clauses and again after them, because PostgreSQL
// takes the two families in any order.
//
// PostgreSQL 18.6 takes each of them after a CHECK or a FOREIGN KEY, in any
// order and beside the deferral clauses, and answers `<KIND> constraints cannot
// be marked ENFORCED` after a key. MySQL 8.4 takes `[NOT] ENFORCED` after a
// CHECK alone, and MariaDB 11.8 takes none of them. What is read changes
// nothing: `ENFORCED` is what a constraint is without it, and `NOT VALID` in a
// CREATE TABLE leaves the constraint validated, as `pg_constraint.convalidated`
// reports. `NOT ENFORCED` keeps a constraint that checks nothing, and `NOT
// VALID` in ALTER TABLE keeps existing rows unchecked; the model has a field for
// neither, so both are refused by name.
func (p *Parser) readConstraintAttributes(kind string) error {
	for {
		p.skipWhitespace()
		start := p.current.Start
		var err error
		switch {
		case p.current.MatchIdentifierValue("ENFORCED"):
			p.advance()
			err = refuseEnforcement(p.dialect, kind, "ENFORCED", start)
		case p.current.MatchIdentifierValue("NOT") && p.nextIsWord("ENFORCED"):
			p.advance()
			p.skipWhitespace()
			p.advance()
			err = refuseNotEnforced(p.dialect, kind, start)
		case p.current.MatchIdentifierValue("NOT") && p.nextIsWord("VALID"):
			p.advance()
			p.skipWhitespace()
			p.advance()
			err = p.readNotValid(kind, start)
		default:
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// nextIsWord reports whether the word after the cursor is word.
func (p *Parser) nextIsWord(word string) bool {
	next := p.peekSignificantToken()
	return next.MatchIdentifierValue(word)
}

// takesEnforced reports whether the dialect takes `[NOT] ENFORCED` after a
// constraint of kind. A document read with no dialect takes what any of them
// does.
func takesEnforced(dialect, kind string) bool {
	switch platform.NormalizeDialect(dialect) {
	case "", platform.Postgres:
		return kind == checkElement || kind == foreignKeyElement
	case platform.MySQL:
		return kind == checkElement
	default:
		return false
	}
}

// refuseNotEnforced refuses `NOT ENFORCED`: where the dialect takes it, the
// model has no field for a constraint that checks nothing.
func refuseNotEnforced(dialect, kind string, start int) error {
	if err := refuseEnforcement(dialect, kind, "NOT ENFORCED", start); err != nil {
		return err
	}
	return fmt.Errorf(
		"NOT ENFORCED at position %d: a %s that is not enforced is not modeled (%s); "+
			"declare it without the clause, or drop the constraint",
		start, kind, unmodeledClauses,
	)
}

// refuseEnforcement refuses clause after a constraint of kind where the
// dialect does not take it. The key message is PostgreSQL's own.
func refuseEnforcement(dialect, kind, clause string, start int) error {
	switch {
	case takesEnforced(dialect, kind):
		return nil
	case kind == primaryKeyElement || kind == uniqueElement || kind == excludeElement:
		return fmt.Errorf("%s at position %d: %s constraints cannot be marked %s", clause, start, kind, clause)
	case kind == indexElement:
		return fmt.Errorf("%s at position %d: an index takes no %s clause", clause, start, clause)
	default:
		return fmt.Errorf("%s at position %d: the %s dialect takes no %s clause after a %s",
			clause, start, dialectName(dialect), clause, kind)
	}
}

// readNotValid reads `NOT VALID` after a table constraint of kind.
func (p *Parser) readNotValid(kind string, start int) error {
	dialect := platform.NormalizeDialect(p.dialect)
	switch {
	case kind == primaryKeyElement || kind == uniqueElement || kind == excludeElement:
		return fmt.Errorf("NOT VALID at position %d: %s constraints cannot be marked NOT VALID", start, kind)
	case kind != checkElement && kind != foreignKeyElement:
		return fmt.Errorf("NOT VALID at position %d: an index takes no NOT VALID clause", start)
	case dialect != "" && dialect != platform.Postgres:
		return fmt.Errorf("NOT VALID at position %d: the %s dialect takes no NOT VALID clause", start, dialectName(p.dialect))
	case p.addingConstraint:
		return fmt.Errorf(
			"NOT VALID at position %d: a %s added without checking the rows already in the table is not modeled (%s); "+
				"write the constraint without NOT VALID, and add it NOT VALID in a migration",
			start, kind, unmodeledClauses,
		)
	default:
		return nil
	}
}

// dialectName names the dialect a document is read as.
func dialectName(dialect string) string {
	if dialect == "" {
		return "chosen"
	}
	return dialect
}

// columnClauseKind is the constraint the clause read last in a column
// definition was, for the attributes that follow it: a CHECK, a foreign key,
// a key, or none.
func columnClauseKind(last columnClause, check bool) string {
	switch {
	case check:
		return checkElement
	case last.foreignKey:
		return foreignKeyElement
	default:
		return last.key
	}
}

// columnCheckMark is what a column definition's CHECKs are so far, so that a
// change across one clause says the clause was a CHECK. A second CHECK on a
// column goes onto the table, and p.columnChecks counts it.
type columnCheckMark struct {
	check  string
	checks int
}

func (p *Parser) markColumnChecks(column *ast.ColumnNode) columnCheckMark {
	return columnCheckMark{check: column.Check, checks: len(p.columnChecks)}
}

// parseColumnAttribute reads `ENFORCED`, `NOT ENFORCED` or `NOT VALID` in a
// column definition, after the clause of kind. PostgreSQL 18.6 attaches the
// first two to the CHECK or REFERENCES written just before them and answers
// `misplaced ENFORCED clause` after anything else, a NOT NULL included, and
// takes `NOT VALID` after a table constraint alone.
func (p *Parser) parseColumnAttribute(kind string) error {
	start := p.current.Start
	switch {
	case p.current.MatchIdentifierValue("NOT") && p.nextIsWord("VALID"):
		return fmt.Errorf("NOT VALID at position %d: it follows a table constraint, not a column", start)
	case kind != checkElement && kind != foreignKeyElement:
		return fmt.Errorf(
			"misplaced ENFORCED clause at position %d: it has to follow the column's CHECK or REFERENCES", start,
		)
	}
	return p.readConstraintAttributes(kind)
}

// isColumnAttribute reports whether the word at the cursor opens `ENFORCED`,
// `NOT ENFORCED` or `NOT VALID`.
func (p *Parser) isColumnAttribute(keyword string) bool {
	return keyword == "ENFORCED" || (keyword == "NOT" && (p.nextIsWord("ENFORCED") || p.nextIsWord("VALID")))
}

// readMatchType reads `MATCH SIMPLE | FULL | PARTIAL` after a reference's
// column list. SIMPLE is what a key is without the clause on every server
// that takes it, and is read. The model keeps no match type, so FULL and
// PARTIAL are refused by name. Measured on PostgreSQL 18.6, MySQL 8.4 and
// MariaDB 11.8: PostgreSQL enforces FULL and answers `MATCH PARTIAL not yet
// implemented`; MySQL records FULL and PARTIAL in MATCH_OPTION and enforces
// neither; MariaDB takes all three and records NONE.
func (p *Parser) readMatchType() error {
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("MATCH") {
		return nil
	}
	start := p.current.Start
	if !takesMatch(p.dialect) {
		return fmt.Errorf("MATCH at position %d: the %s dialect takes no MATCH clause", start, dialectName(p.dialect))
	}
	p.advance()
	p.skipWhitespace()
	matchType := strings.ToUpper(p.current.Value)
	switch matchType {
	case "SIMPLE":
		p.advance()
		return nil
	case "FULL", "PARTIAL":
		return fmt.Errorf(
			"MATCH %s at position %d: the match type of a foreign key is not modeled (%s), so the key would be "+
				"built as MATCH SIMPLE; declare it without the clause",
			matchType, start, unmodeledClauses,
		)
	default:
		return fmt.Errorf("expected SIMPLE, FULL or PARTIAL after MATCH at position %d", p.current.Start)
	}
}

// takesMatch reports whether the dialect takes a MATCH clause after
// REFERENCES.
func takesMatch(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case "", platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite:
		return true
	default:
		return false
	}
}

// readKeyOptions reads the MySQL family's index options after a key's parts,
// in any order, and refuses by name the ones the model cannot keep. kind is
// the element the options follow and prefix its FULLTEXT or SPATIAL prefix,
// empty for none.
//
// `USING BTREE | HASH` is the access method `KEY k USING HASH (a)` asks for
// before the parts, and the later clause wins: MariaDB 11.8 builds `KEY k USING
// BTREE (a) USING HASH` as HASH. `VISIBLE` and MariaDB's `NOT IGNORED` are what
// an index is without them. The rest declare what the model has no field for;
// see [refuseKeyOption]. Other dialects take none of these, and a document
// read as one of them leaves them to the end of the table element.
func (p *Parser) readKeyOptions(kind, prefix string) error {
	if !p.readsKeyOptions() || (kind != primaryKeyElement && kind != uniqueElement && kind != indexElement) {
		return nil
	}
	for {
		p.skipWhitespace()
		start := p.current.Start
		keyword := strings.ToUpper(p.current.Value)
		if p.current.Type != lexer.TokenIdentifier {
			return nil
		}
		switch {
		case keyword == "USING" && !p.nextIsWord("INDEX"):
			if err := p.readKeyMethod(kind, prefix, start); err != nil {
				return err
			}
		case keyword == "VISIBLE":
			p.advance()
		case keyword == "NOT" && p.nextIsWord("IGNORED"):
			if platform.NormalizeDialect(p.dialect) == platform.MySQL {
				return fmt.Errorf("NOT IGNORED at position %d: it is MariaDB's clause, and MySQL 8.4 answers ERROR 1064", start)
			}
			p.advance()
			p.skipWhitespace()
			p.advance()
		default:
			if err := refuseKeyOption(keyword, start); err != nil {
				return err
			}
			return nil
		}
	}
}

// readsKeyOptions reports whether the document's dialect has the MySQL
// family's index options. A document read with no dialect does.
func (p *Parser) readsKeyOptions() bool {
	return p.dialect == "" || p.isMySQLFamilyDialect()
}

// readKeyMethod reads `USING BTREE | HASH` after a key's parts.
func (p *Parser) readKeyMethod(kind, prefix string, start int) error {
	if prefix != "" {
		return fmt.Errorf("USING at position %d: a %s index takes no access method", start, prefix)
	}
	method := p.readIndexAccessMethod()
	if kind != primaryKeyElement {
		p.indexAccessMethod = method
		return nil
	}
	if method != "" {
		return fmt.Errorf(
			"USING %s after a PRIMARY KEY at position %d: the access method of a primary key is not modeled (%s); "+
				"MariaDB 11.8 builds it, and MySQL 8.4 builds BTREE on InnoDB",
			method, start, unmodeledClauses,
		)
	}
	return nil
}

// refuseKeyOption refuses an index option the model has no field for, by
// name, and answers nil for a word that is not one. Measured on MySQL 8.4 and
// MariaDB 11.8, each is stored with the index: a comment in
// `STATISTICS.INDEX_COMMENT`, which the index model has a field for and the
// MySQL-family renderer writes nowhere; an invisible or ignored index that the
// optimizer skips; a key block size; an engine attribute.
func refuseKeyOption(keyword string, start int) error {
	var what string
	switch keyword {
	case "COMMENT":
		what = "an index comment is not kept on MySQL and MariaDB: Ptah writes none and reads none back"
	case "INVISIBLE", "IGNORED":
		what = "an index the optimizer does not use is not modeled"
	case "KEY_BLOCK_SIZE":
		what = "the key block size of an index is not modeled"
	case "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE":
		what = "an engine attribute of an index is not modeled"
	default:
		return nil
	}
	return fmt.Errorf("%s at position %d: %s (%s); declare the key without it", keyword, start, what, unmodeledClauses)
}
