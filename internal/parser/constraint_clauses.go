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

// enforcement is what `ENFORCED` and `NOT ENFORCED` said about one
// constraint: whether either was written, and whether the last one written was
// `NOT ENFORCED`.
type enforcement struct {
	written     bool
	notEnforced bool
}

// record takes one enforcement clause written at start. PostgreSQL 18.6
// refuses a second one, `ENFORCED NOT ENFORCED` and `NOT ENFORCED NOT
// ENFORCED` alike, and MySQL 8.4 keeps the last: `CHECK (a > 0) NOT ENFORCED
// ENFORCED` reads back ENFORCED `YES`.
func (e *enforcement) record(dialect string, notEnforced bool, start int) error {
	if e.written && takesOneEnforcement(dialect) {
		return fmt.Errorf("NOT ENFORCED or ENFORCED at position %d: a constraint takes one of them, and "+
			"PostgreSQL 18.6 answers `multiple ENFORCED/NOT ENFORCED clauses not allowed`", start)
	}
	e.written = true
	e.notEnforced = notEnforced
	return nil
}

// takesOneEnforcement reports whether the dialect refuses a second
// enforcement clause after one constraint. A document read with no dialect
// takes PostgreSQL's rule, the stricter one.
func takesOneEnforcement(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case "", platform.Postgres:
		return true
	default:
		return false
	}
}

// readConstraintAttributes reads `ENFORCED`, `NOT ENFORCED` and `NOT VALID`
// after a table constraint of kind into enforced, and leaves the cursor on
// anything else. It is read before the deferral clauses and again after them,
// because PostgreSQL takes the two families in any order.
//
// PostgreSQL 18.6 takes each of them after a CHECK or a FOREIGN KEY, in any
// order and beside the deferral clauses, and answers `<KIND> constraints cannot
// be marked ENFORCED` after a key. PostgreSQL 17 answers a syntax error to
// `ENFORCED`, so a render refuses a constraint that does not check its rows
// there. MySQL 8.4 and 9.7 take `[NOT] ENFORCED` after a CHECK alone, and
// MariaDB 11.8 takes none of them. `NOT ENFORCED` keeps a constraint the
// server does not check, which the model carries; `ENFORCED` is what a
// constraint is without it. `NOT VALID` in a CREATE TABLE leaves the
// constraint validated, as `pg_constraint.convalidated` reports, and in ALTER
// TABLE it keeps existing rows unchecked, which the added constraint carries.
func (p *Parser) readConstraintAttributes(kind string, enforced *enforcement) error {
	for {
		p.skipWhitespace()
		start := p.current.Start
		var err error
		switch {
		case p.current.MatchIdentifierValue("ENFORCED"):
			p.advance()
			err = p.readEnforcement(kind, "ENFORCED", start, enforced)
		case p.current.MatchIdentifierValue("NOT") && p.nextIsWord("ENFORCED"):
			p.advance()
			p.skipWhitespace()
			p.advance()
			err = p.readEnforcement(kind, "NOT ENFORCED", start, enforced)
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

// readEnforcement reads clause, `ENFORCED` or `NOT ENFORCED`, after a
// constraint of kind into enforced, and refuses it where the dialect takes
// none there.
func (p *Parser) readEnforcement(kind, clause string, start int, enforced *enforcement) error {
	if err := refuseEnforcement(p.dialect, kind, clause, start); err != nil {
		return err
	}
	return enforced.record(p.dialect, clause == "NOT ENFORCED", start)
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

// readNotValid reads `NOT VALID` after a table constraint of kind. After ALTER
// TABLE ... ADD it marks the constraint added; in a CREATE TABLE it changes
// nothing, because PostgreSQL 18.6, CockroachDB v26.3.2 and YugabyteDB
// 2026.1.2 record such a constraint as validated.
func (p *Parser) readNotValid(kind string, start int) error {
	switch {
	case kind == primaryKeyElement || kind == uniqueElement || kind == excludeElement:
		return fmt.Errorf("NOT VALID at position %d: %s constraints cannot be marked NOT VALID", start, kind)
	case kind != checkElement && kind != foreignKeyElement:
		return fmt.Errorf("NOT VALID at position %d: an index takes no NOT VALID clause", start)
	case !takesNotValid(p.dialect):
		return fmt.Errorf("NOT VALID at position %d: the %s dialect takes no NOT VALID clause", start, dialectName(p.dialect))
	default:
		p.addedNotValid = p.addingConstraint
		return nil
	}
}

// takesNotValid reports whether the dialect takes `NOT VALID` after a CHECK or
// a foreign key and `VALIDATE CONSTRAINT` to complete it. PostgreSQL 18.6,
// CockroachDB v26.3.2 and YugabyteDB 2026.1.2 take both. A document read with
// no dialect takes what any of them does.
func takesNotValid(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case "", platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return true
	default:
		return false
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
// column definition, after the clause of kind, into enforced. PostgreSQL 18.6
// attaches the first two to the CHECK or REFERENCES written just before them
// and answers `misplaced ENFORCED clause` after anything else, a NOT NULL
// included, and takes `NOT VALID` after a table constraint alone.
func (p *Parser) parseColumnAttribute(kind string, enforced *enforcement) error {
	start := p.current.Start
	switch {
	case p.current.MatchIdentifierValue("NOT") && p.nextIsWord("VALID"):
		return fmt.Errorf("NOT VALID at position %d: it follows a table constraint, not a column", start)
	case kind != checkElement && kind != foreignKeyElement:
		return fmt.Errorf(
			"misplaced ENFORCED clause at position %d: it has to follow the column's CHECK or REFERENCES", start,
		)
	}
	return p.readConstraintAttributes(kind, enforced)
}

// applyColumnEnforcement gives what the enforcement clauses in a column
// definition said to the constraint they follow: the column's foreign key, its
// CHECK, or a second CHECK the column moved onto the table, which PostgreSQL
// 18.6 marks alone: `b int CHECK (a > 0) CHECK (b > 0) NOT ENFORCED` leaves
// `a > 0` enforced.
func applyColumnEnforcement(
	table *ast.CreateTableNode, column *ast.ColumnNode, last columnClause, enforced enforcement,
) {
	if !enforced.written {
		return
	}
	switch {
	case last.foreignKey:
		column.ForeignKey.NotEnforced = enforced.notEnforced
	case last.onTable && last.key == checkElement:
		table.Constraints[len(table.Constraints)-1].NotEnforced = enforced.notEnforced
	default:
		column.CheckNotEnforced = enforced.notEnforced
	}
}

// isColumnAttribute reports whether the word at the cursor opens `ENFORCED`,
// `NOT ENFORCED` or `NOT VALID`.
func (p *Parser) isColumnAttribute(keyword string) bool {
	return keyword == "ENFORCED" || (keyword == "NOT" && (p.nextIsWord("ENFORCED") || p.nextIsWord("VALID")))
}

// readMatchType reads `MATCH SIMPLE | FULL | PARTIAL` after a reference's
// column list, and answers FULL or PARTIAL, or empty for SIMPLE, which is what
// a key is without the clause on every server that takes it.
//
// Measured: PostgreSQL 17.11 and 18.6, CockroachDB v26.3.2 and YugabyteDB
// 2026.1.2 record FULL in pg_constraint.confmatchtype, and refuse PARTIAL.
// MySQL 8.4.11 and 9.7.2 record FULL and PARTIAL in
// REFERENTIAL_CONSTRAINTS.MATCH_OPTION and enforce neither. MariaDB 11.8.9 and
// SQLite 3.51 accept both and record NONE, so the key they build is MATCH
// SIMPLE, and the clause is refused there rather than read and dropped.
func (p *Parser) readMatchType() (string, error) {
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("MATCH") {
		return "", nil
	}
	start := p.current.Start
	if !takesMatch(p.dialect) {
		return "", fmt.Errorf("MATCH at position %d: the %s dialect takes no MATCH clause", start, dialectName(p.dialect))
	}
	p.advance()
	p.skipWhitespace()
	matchType := strings.ToUpper(p.current.Value)
	switch matchType {
	case "SIMPLE":
		p.advance()
		return "", nil
	case "FULL", "PARTIAL":
		if err := refuseMatchType(p.dialect, matchType, start); err != nil {
			return "", err
		}
		p.advance()
		return matchType, nil
	default:
		return "", fmt.Errorf("expected SIMPLE, FULL or PARTIAL after MATCH at position %d", p.current.Start)
	}
}

// refuseMatchType refuses a MATCH type the dialect's server does not build,
// and answers nil for one it does. A document read with no dialect takes what
// any of them does.
func refuseMatchType(dialect, matchType string, start int) error {
	switch platform.NormalizeDialect(dialect) {
	case "", platform.MySQL:
		return nil
	case platform.MariaDB:
		return fmt.Errorf("MATCH %s at position %d: MariaDB 11.8.9 accepts the clause and records NONE, so the key "+
			"it builds is MATCH SIMPLE; declare the key without the clause", matchType, start)
	case platform.SQLite:
		return fmt.Errorf("MATCH %s at position %d: SQLite 3.51 accepts the clause and records NONE, so the key "+
			"it builds is MATCH SIMPLE; declare the key without the clause", matchType, start)
	}
	if matchType == "PARTIAL" {
		return fmt.Errorf("MATCH PARTIAL at position %d: the %s dialect's server does not implement it, and "+
			"PostgreSQL 18.6 answers `MATCH PARTIAL not yet implemented`", start, dialectName(dialect))
	}
	return nil
}

// takesMatch reports whether the dialect takes a MATCH clause after
// REFERENCES.
func takesMatch(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case "", platform.Postgres, platform.CockroachDB, platform.YugabyteDB,
		platform.MySQL, platform.MariaDB, platform.SQLite:
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
// BTREE (a) USING HASH` as HASH. A `COMMENT` and the visibility of the index
// are kept on p.keyOptions; see [Parser.readKeyVisibility]. The rest declare
// what the model has no field for; see [refuseKeyOption]. Other dialects take
// none of these, and a document read as one of them leaves them to the end of
// the table element.
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
		var err error
		switch {
		case keyword == "USING" && !p.nextIsWord("INDEX"):
			err = p.readKeyMethod(kind, prefix, start)
		case keyword == "COMMENT":
			err = p.readKeyComment(kind, start)
		case p.atVisibilityWord(keyword):
			err = p.readKeyVisibility(kind, keyword, start)
		default:
			return refuseKeyOption(keyword, start)
		}
		if err != nil {
			return err
		}
	}
}

// keyOptions are the options after a key's parts that the index model keeps.
// They belong to the element being read, which resets them before it reads its
// parts.
type keyOptions struct {
	comment    string
	hasComment bool
	invisible  bool
	// primaryKeyMethod is the `USING HASH` after a primary key's parts, which
	// the table element carries onto its constraint; see
	// [Parser.readTableElementOptions].
	primaryKeyMethod string
}

// carriesIndexOptions reports whether the element asked for something only an
// index can hold.
func (o keyOptions) carriesIndexOptions() bool {
	return o.hasComment || o.invisible
}

// readKeyComment reads `COMMENT 'text'` after a key's parts, which MySQL 8.4.11
// and MariaDB 11.8.9 keep in STATISTICS.INDEX_COMMENT. A primary key's comment
// is not modeled.
func (p *Parser) readKeyComment(kind string, start int) error {
	if kind == primaryKeyElement {
		return fmt.Errorf("COMMENT at position %d: a primary key's comment is not modeled (%s); "+
			"declare the key without it", start, unmodeledClauses)
	}
	p.advance()
	text, err := p.stringConstant("index comment")
	if err != nil {
		return err
	}
	p.keyOptions.comment, p.keyOptions.hasComment = text, true
	return nil
}

// atVisibilityWord reports whether keyword, the current token upper-cased,
// opens a clause that says whether the optimizer uses an index. A key's
// options and ALTER INDEX both ask it, so the two cannot disagree on which
// words hide or show an index.
func (p *Parser) atVisibilityWord(keyword string) bool {
	switch keyword {
	case "VISIBLE", "INVISIBLE", "IGNORED":
		return true
	case "NOT":
		return p.nextIsWord("IGNORED")
	default:
		return false
	}
}

// readKeyVisibility reads whether the optimizer uses the index. MySQL spells
// it `VISIBLE | INVISIBLE` and MariaDB `NOT IGNORED | IGNORED`; each engine
// answers ERROR 1064 to the other's word for the hidden index, and both refuse
// a hidden primary key. A document read with no dialect takes either word.
func (p *Parser) readKeyVisibility(kind, keyword string, start int) error {
	dialect := platform.NormalizeDialect(p.dialect)
	clause := keyword
	if keyword == "NOT" {
		clause = "NOT IGNORED"
	}
	switch {
	case (clause == "IGNORED" || clause == "NOT IGNORED") && dialect == platform.MySQL:
		return fmt.Errorf("%s at position %d: it is MariaDB's clause, and MySQL 8.4 answers ERROR 1064", clause, start)
	case clause == "INVISIBLE" && dialect == platform.MariaDB:
		return fmt.Errorf("INVISIBLE at position %d: it is MySQL's clause, and MariaDB 11.8.9 answers ERROR 1064; "+
			"MariaDB spells it IGNORED", start)
	case clause == "INVISIBLE" && kind == primaryKeyElement:
		return fmt.Errorf("INVISIBLE at position %d: MySQL 8.4.11 answers ERROR 3522, "+
			"a primary key index cannot be invisible", start)
	case clause == "IGNORED" && kind == primaryKeyElement:
		return fmt.Errorf("IGNORED at position %d: MariaDB 11.8.9 answers ERROR 4174, "+
			"a primary key cannot be marked as IGNORE", start)
	}
	p.advance()
	if clause == "NOT IGNORED" {
		p.skipWhitespace()
		p.advance()
	}
	p.keyOptions.invisible = clause == "INVISIBLE" || clause == "IGNORED"
	return nil
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
	// MariaDB 11.8.9 keeps it, INDEX_TYPE HASH, and MySQL 8.4.11 builds BTREE on
	// InnoDB; see [mysqlindex.MethodSatisfiedBy] for how each is compared.
	p.keyOptions.primaryKeyMethod = method
	return nil
}

// refuseKeyOption refuses an index option the model has no field for, by
// name, and answers nil for a word that is not one. Measured on MySQL 8.4 and
// MariaDB 11.8, each is stored with the index: a key block size and an engine
// attribute.
func refuseKeyOption(keyword string, start int) error {
	var what string
	switch keyword {
	case "KEY_BLOCK_SIZE":
		what = "the key block size of an index is not modeled"
	case "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE":
		what = "an engine attribute of an index is not modeled"
	default:
		return nil
	}
	return fmt.Errorf("%s at position %d: %s (%s); declare the key without it", keyword, start, what, unmodeledClauses)
}
