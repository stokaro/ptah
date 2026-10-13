package parser

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/lexer"
)

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
	case kind == primaryKeyElement || kind == uniqueElement:
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
	case kind == primaryKeyElement || kind == uniqueElement:
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

// readKeyOptions reads the options after a key's parts, in any order. kind is
// the element the options follow, name the key's name as the document writes
// it, and prefix its FULLTEXT or SPATIAL prefix, empty for none.
//
// The MySQL family's options the shared grammar reads are listed once, in
// [Parser.keyOptionReader]. `USING BTREE | HASH` is the access method `KEY k
// USING HASH (a)` asks for before the parts, and the later clause wins:
// MariaDB 11.8 builds `KEY k USING BTREE (a) USING HASH` as HASH. A `COMMENT`
// and the visibility of the index are kept on p.keyOptions; see
// [Parser.readKeyVisibility]. An engine attribute is refused by name. An
// owner's option, such as MySQL's KEY_BLOCK_SIZE, is claimed for the owner;
// see [Parser.claimOwnerIndexOption]. A version guard among the options is
// read as the options it holds; see [Parser.readGuardedKeyOptions]. A
// document of another dialect reads only the owners' options here, and
// leaves the rest to the end of the table element.
func (p *Parser) readKeyOptions(kind, name, prefix string) error {
	if kind != primaryKeyElement && kind != uniqueElement && kind != indexElement {
		return nil
	}
	for {
		p.skipSpaceBeforeGuard()
		if p.current.Type == lexer.TokenUnknown {
			if err := p.readGuardedKeyOptions(kind, name, prefix); err != nil {
				return err
			}
			continue
		}
		if p.claimOwnerIndexOption(name, prefix) {
			continue
		}
		read, known := p.keyOptionReader()
		if !known || !p.readsKeyOptions() {
			return nil
		}
		if err := read(kind, prefix, p.current.Start); err != nil {
			return err
		}
	}
}

// keyOptionReader answers the reader of the key option the current token
// opens, of those the shared grammar reads, and whether it opens one. It is
// the one list of them: [Parser.readKeyOptions] dispatches through it, and an
// owner's option ends where it answers true, so the two cannot disagree.
func (p *Parser) keyOptionReader() (func(kind, prefix string, start int) error, bool) {
	if p.current.Type != lexer.TokenIdentifier {
		return nil, false
	}
	keyword := strings.ToUpper(p.current.Value)
	switch {
	case keyword == "USING" && !p.nextIsWord("INDEX"):
		return p.readKeyMethod, true
	case keyword == "COMMENT":
		return func(string, string, int) error { return p.readKeyComment() }, true
	case p.atVisibilityWord(keyword):
		return func(kind, _ string, start int) error { return p.readKeyVisibility(kind, keyword, start) }, true
	case keyword == "ENGINE_ATTRIBUTE" || keyword == "SECONDARY_ENGINE_ATTRIBUTE":
		// Measured on MySQL 8.4 and MariaDB 11.8, an engine attribute is
		// stored with the index, and the model has no field for it.
		return func(_, _ string, start int) error {
			return fmt.Errorf("%s at position %d: an engine attribute of an index is not modeled; declare the key without it",
				keyword, start)
		}, true
	default:
		return nil, false
	}
}

// readGuardedKeyOptions reads the version guard at the cursor, among a key's
// options, as the options its body holds, at their own positions in the
// document. MySQL 8.4's SHOW CREATE TABLE writes two options that way, after
// any other: `FULLTEXT KEY ft (bio) COMMENT 'x' /*!80000 INVISIBLE */
// /*!50100 WITH PARSER ngram */`. So every guard among the options is read,
// not only the last. A guard whose body opens with no key option is stepped
// over, as a guard is anywhere else; one whose body opens with an option
// holds nothing else.
//
// The body is read in place: the parser reads it through a lexer over the
// guard's text and returns to the document's lexer after it, so the options
// and any owner's claim land where they stand.
func (p *Parser) readGuardedKeyOptions(kind, name, prefix string) error {
	guard := p.current
	start, end, ok := executableCommentBody(guard.Value)
	if !ok {
		p.advance()
		return nil
	}
	outer, current, previous := p.lexer, p.current, p.previous
	p.lexer = outer.Range(guard.Start+start, guard.Start+end)
	p.advance()
	p.skipWhitespace()
	var err error
	if p.opensNextOption(sqlext.IndexOption) {
		err = p.readKeyOptions(kind, name, prefix)
		if err == nil && !p.isAtEnd() {
			err = fmt.Errorf("%s at position %d: a version guard among a key's options holds only key options",
				p.current.Value, p.current.Start)
		}
	}
	p.lexer, p.current, p.previous = outer, current, previous
	if err != nil {
		return err
	}
	p.advance()
	return nil
}

// keyOptions are the options after a key's parts that the index model keeps.
// They belong to the element being read, which resets them before it reads its
// parts.
type keyOptions struct {
	comment    string
	hasComment bool
	invisible  bool
	// facets is where the owners' options of the key land, nil when no owner
	// claimed one.
	facets *keyFacets
	// primaryKeyMethod is the `USING HASH` after a primary key's parts, which
	// the table element carries onto its constraint; see
	// [Parser.readTableElementOptions].
	primaryKeyMethod string
}

// carriesIndexOptions reports whether the element asked for something only an
// index can hold: a comment, a visibility, or an owner's option.
func (o keyOptions) carriesIndexOptions() bool {
	return o.hasComment || o.invisible || o.facets != nil
}

// hold points the owners' options of the key at the node the element built,
// the index or the PRIMARY KEY constraint, which label names in an error. An
// element no owner's option was claimed for holds nothing.
func (o keyOptions) hold(node *schemaext.Facets, label string) {
	if o.facets != nil {
		o.facets.node, o.facets.label = node, label
	}
}

// keyFacets is where the owners' options of one key land. The owners read
// their claims once the document is parsed, and the element decides after its
// options whether it builds an index or a PRIMARY KEY constraint, so a claim
// holds this and the element points it at the node it built.
type keyFacets struct {
	node  *schemaext.Facets
	label string
}

// merge adds an owner's facets to the key's node.
func (k *keyFacets) merge(value sqlext.Value) error {
	if k.node == nil {
		// Every element that reads options builds an index or a PRIMARY KEY
		// constraint and holds them, so this is a defect of the reader.
		return fmt.Errorf("%w: an owner's key option reached no key", schemaext.ErrInvalidValue)
	}
	merged, err := k.node.Merge(value.Facets)
	if err != nil {
		return fmt.Errorf("%s: %w", k.label, err)
	}
	*k.node = merged
	return nil
}

// readKeyComment reads `COMMENT 'text'` after a key's parts, which MySQL 8.4.11
// and MariaDB 11.8.9 keep in STATISTICS.INDEX_COMMENT.
func (p *Parser) readKeyComment() error {
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
