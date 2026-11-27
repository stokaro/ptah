package postgresreplay

import (
	"slices"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// wordAt is the keyword at index, in upper case, or "" for a token that is no
// unquoted word: a quoted identifier is a name, never a keyword.
func wordAt(tokens []lexer.Token, index int) string {
	if index < 0 || index >= len(tokens) || tokens[index].Type != lexer.TokenIdentifier {
		return ""
	}
	return replay.NormalizedIdentifier(tokens[index])
}

// foldedIdentifier is the name the identifier at index stands for as the
// server stores it ([storedName]), and false for a token that is no
// identifier.
func foldedIdentifier(tokens []lexer.Token, index int) (string, bool) {
	if index < 0 || index >= len(tokens) {
		return "", false
	}
	token := tokens[index]
	if token.Type != lexer.TokenIdentifier && !quotedIdentifier(token) {
		return "", false
	}
	return storedName(token), true
}

// foldedNameAt reads the dotted name that begins at index as the server
// stores each part, and returns the index after it. Unlike
// [replay.QualifiedIdentifierAt] it folds an unquoted part and keeps a quoted
// one, so `Users` and `"users"` are one relation and `"Users"` is another,
// and it reads no string literal as a name.
func foldedNameAt(tokens []lexer.Token, index int) ([]string, int) {
	part, ok := foldedIdentifier(tokens, index)
	if !ok {
		return nil, index
	}
	parts := []string{part}
	index++
	for index+1 < len(tokens) && tokens[index].MatchOperatorValue(".") {
		part, ok := foldedIdentifier(tokens, index+1)
		if !ok {
			break
		}
		parts = append(parts, part)
		index += 2
	}
	return parts, index
}

// notWriteTarget are the words that follow UPDATE, or a write's ONLY, where
// the word is no write of a relation: ON CONFLICT DO UPDATE SET, FOR UPDATE
// OF, a trigger's UPDATE OF and UPDATE ON, a policy's FOR UPDATE TO and USING,
// a privilege's UPDATE ON, and a foreign key's ON UPDATE action.
var notWriteTarget = []string{
	"CASCADE", "NO", "NOWAIT", "OF", "ON", "OR", "RESTRICT", "SET", "SKIP", "TO", "USING", "WITH",
}

// writeTargetAt returns the folded dotted name of the relation the write whose
// keyword is at index writes, or nil when the word there begins no write:
// INSERT INTO, CockroachDB's UPSERT INTO, MERGE INTO, UPDATE and DELETE FROM,
// each with ONLY or without.
func writeTargetAt(tokens []lexer.Token, index int) []string {
	target := -1
	switch wordAt(tokens, index) {
	case "INSERT", "UPSERT", "MERGE":
		if wordAt(tokens, index+1) == "INTO" {
			target = index + 2
		}
	case "UPDATE":
		target = index + 1
	case "DELETE":
		if wordAt(tokens, index+1) == "FROM" {
			target = index + 2
		}
	}
	if target < 0 {
		return nil
	}
	if wordAt(tokens, target) == "ONLY" {
		target++
	}
	if slices.Contains(notWriteTarget, wordAt(tokens, target)) {
		return nil
	}
	parts, _ := foldedNameAt(tokens, target)
	return parts
}

// writeTargets returns every relation tokens writes the rows of: the target
// of each write the statement holds, in a common table expression, a rule's
// action, an EXPLAIN ANALYZE or a PREPARE among them, and every table a
// TRUNCATE names. COPY is read by [protection.copyTargetNamespace].
func writeTargets(tokens []lexer.Token) [][]string {
	var targets [][]string
	for index := range tokens {
		if target := writeTargetAt(tokens, index); target != nil {
			targets = append(targets, target)
		}
	}
	if wordAt(tokens, 0) != "TRUNCATE" {
		return targets
	}
	index := 1
	if wordAt(tokens, index) == "TABLE" {
		index++
	}
	for index < len(tokens) {
		if wordAt(tokens, index) == "ONLY" {
			index++
		}
		parts, next := foldedNameAt(tokens, index)
		if len(parts) == 0 {
			break
		}
		targets = append(targets, parts)
		if next < len(tokens) && tokens[next].MatchOperatorValue("*") {
			next++
		}
		if next >= len(tokens) || !tokens[next].MatchOperatorValue(",") {
			break
		}
		index = next + 1
	}
	return targets
}

// apply changes the graph as an accepted statement changes the relations of
// the dev database. The caller holds the lock.
func (g *relationGraph) apply(p protection, tokens []lexer.Token) {
	switch wordAt(tokens, 0) {
	case "CREATE":
		g.applyCreate(p, tokens)
	case "ALTER":
		g.applyAlter(p, tokens)
	case "DROP":
		g.applyDrop(p, tokens)
	case "SELECT", "WITH":
		g.applySelectInto(p, replay.LeadingCTEExecutableTokens(tokens))
	}
}

// createKey returns where the server creates the relation the dotted folded
// name parts names: the schema the name is qualified with, the session's
// temporary schema for a temporary relation, or the first schema of the
// search path that exists ([protection.creationNamespace] reads the same
// path for a namespace the server owns). It reports false when the name
// reaches no schema, as when no schema of the path exists and the server
// refuses the CREATE. A guard that read no session keys a name without a
// schema by the name alone.
func (g *relationGraph) createKey(p protection, parts []string, life lifetime) (relationKey, bool) {
	switch len(parts) {
	case 1:
		if life == sessionLifetime {
			return relationKey{schema: "pg_temp", name: parts[0]}, true
		}
		if !g.session {
			return relationKey{name: parts[0]}, true
		}
		for _, schema := range g.searchPath {
			if schema != "pg_temp" && g.schemas[schema] {
				return relationKey{schema: schema, name: parts[0]}, true
			}
		}
	case 2:
		return relationKey{schema: parts[0], name: parts[1]}, true
	case 3:
		if p.database == "" || parts[0] == p.database {
			return relationKey{schema: parts[1], name: parts[2]}, true
		}
	}
	return relationKey{}, false
}

// create records a new relation at key, in place of any the graph held there.
func (g *relationGraph) create(key relationKey) *relationNode {
	if old := g.byKey[key]; old != nil {
		old.dropped = true
	}
	node := &relationNode{key: key}
	g.byKey[key] = node
	return node
}

// rebind moves node to key, as RENAME TO and SET SCHEMA do. What refers to
// the relation stays bound to it.
func (g *relationGraph) rebind(node *relationNode, key relationKey) {
	if g.byKey[node.key] == node {
		delete(g.byKey, node.key)
	}
	if old := g.byKey[key]; old != nil && old != node {
		old.dropped = true
	}
	node.key = key
	g.byKey[key] = node
}

// existing returns the relation a statement that names an existing relation
// refers to: the one the server finds now, or, for a name the guard does not
// know, a new one the graph holds from then on, since the statement would
// fail on the server unless the relation exists. A name without a schema the
// guard does not know is taken to be in the schema a CREATE would put it in
// ([relationGraph.createKey]), the likeliest one; a relation the guard cannot
// place is nil.
func (g *relationGraph) existing(p protection, parts []string) *relationNode {
	if node := g.resolve(p, parts); node != nil {
		return node
	}
	key, ok := g.createKey(p, parts, schemaLifetime)
	if !ok {
		return nil
	}
	if node := g.live(key); node != nil {
		return node
	}
	return g.create(key)
}

// bindRef binds a name a view's query, a rule's action or a foreign key
// writes in the place of a relation, as the server binds it when the object
// is created: to a relation of a namespace the server owns, to the relation
// the guard finds, or, when it finds none, to the name, looked up again each
// time a write is judged.
func (g *relationGraph) bindRef(p protection, parts []string) relationRef {
	if namespace := p.qualifierNamespace(parts); namespace != "" {
		return relationRef{protected: namespace}
	}
	if len(parts) == 1 {
		if namespace := p.unqualifiedNamespace(classRelation, parts[0]); namespace != "" {
			return relationRef{protected: namespace}
		}
	}
	if node := g.existing(p, parts); node != nil {
		return relationRef{node: node}
	}
	return relationRef{name: parts}
}

// bindNames binds every dotted name tokens holds as a name of any class: each
// one a relation of a namespace the server owns
// ([protection.protectedNameReach]), each relation the guard finds for one of
// its prefixes, and the rest as names looked up again each time a write is
// judged. A name followed by a parenthesis is a call, and a name after a dot
// is a part of the name before it.
func (g *relationGraph) bindNames(p protection, tokens []lexer.Token) []relationRef {
	var refs []relationRef
	for index := 0; index < len(tokens); index++ {
		if index > 0 && tokens[index-1].MatchOperatorValue(".") {
			continue
		}
		parts, next := foldedNameAt(tokens, index)
		if len(parts) == 0 {
			continue
		}
		index = next - 1
		if next < len(tokens) && tokens[next].MatchOperatorValue("(") {
			continue
		}
		if namespace := p.protectedNameReach(parts); namespace != "" {
			refs = append(refs, relationRef{protected: namespace})
			continue
		}
		bound := false
		for end := 1; end <= len(parts); end++ {
			if node := g.resolve(p, parts[:end]); node != nil {
				refs = append(refs, relationRef{node: node})
				bound = true
			}
		}
		if !bound {
			refs = append(refs, relationRef{name: parts})
		}
	}
	return refs
}

// viewEdges returns what a write through a view whose query is query reaches:
// the one relation the server writes through when the view is automatically
// updatable ([shapedViewBase]), or every relation the query names when the
// guard cannot read it as one. A view the server cannot write through has no
// write path but its INSTEAD rules and triggers, which the graph holds apart,
// so reading every name there refuses only a write the server would refuse,
// or one an INSTEAD OF trigger function would take.
func (g *relationGraph) viewEdges(p protection, query []lexer.Token) (*relationRef, []relationRef) {
	query = viewQuery(query)
	if base, ok := shapedViewBase(query); ok {
		ref := g.bindRef(p, base)
		return &ref, nil
	}
	return nil, g.bindNames(p, query)
}

// viewQuery is query without the semicolons pg_get_ruledef ends a rule with
// and without a trailing WITH [CASCADED | LOCAL] CHECK OPTION.
func viewQuery(query []lexer.Token) []lexer.Token {
	for len(query) > 0 && query[len(query)-1].MatchOperatorValue(";") {
		query = query[:len(query)-1]
	}
	depth := 0
	for index, token := range query {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case depth == 0 && index > 0 && wordAt(query, index) == "WITH":
			switch wordAt(query, index+1) {
			case "CASCADED", "LOCAL", "CHECK":
				return query[:index]
			}
		}
	}
	return query
}

// shapedViewClauses are the words that, outside parentheses after a view's
// FROM item, make the view one the server does not update automatically, or
// one whose shape the guard does not read: a join, a grouping, a limit, a
// window, a set operation, a sample and a locking clause.
var shapedViewClauses = []string{
	"CROSS", "EXCEPT", "FETCH", "FOR", "FULL", "GROUP", "HAVING", "INNER", "INTERSECT", "JOIN",
	"LATERAL", "LEFT", "LIMIT", "NATURAL", "OFFSET", "RIGHT", "TABLESAMPLE", "UNION", "WINDOW",
}

// shapedViewBase returns the folded name of the relation a view whose query
// is query writes through, and true when the guard reads the query as one the
// server updates automatically: a SELECT, without DISTINCT or a window
// function in its list, whose FROM holds one relation, with ONLY, an alias
// and a column alias list allowed, followed by nothing but WHERE and ORDER BY
// outside parentheses. A subquery in the list or in WHERE is only read, so a
// write through `SELECT * FROM app.docs WHERE owner IN (SELECT rolname FROM
// pg_roles)` writes app.docs. Any other query reports false, and
// [relationGraph.viewEdges] reads every name it holds.
func shapedViewBase(query []lexer.Token) ([]string, bool) {
	from := topLevelFrom(query)
	if from < 0 {
		return nil, false
	}
	base, next := singleFromItem(query, from+1)
	if len(base) == 0 || !filteredAndOrderedOnly(query, next) {
		return nil, false
	}
	return base, true
}

// topLevelFrom returns the index of the FROM of a view query that is a SELECT
// without DISTINCT or a window function in its list, or -1.
func topLevelFrom(query []lexer.Token) int {
	if wordAt(query, 0) != "SELECT" || wordAt(query, 1) == "DISTINCT" {
		return -1
	}
	depth := 0
	for index := 1; index < len(query); index++ {
		switch {
		case query[index].MatchOperatorValue("("):
			depth++
		case query[index].MatchOperatorValue(")"):
			depth--
		case depth == 0 && wordAt(query, index) == "OVER":
			return -1
		case depth == 0 && wordAt(query, index) == "FROM":
			return index
		}
	}
	return -1
}

// singleFromItem reads the FROM item at index, `[ONLY] name [*] [[AS] alias
// [(columns)]]`, and returns the relation's folded name and the index after
// the item. The name is nil when no relation stands at index. A second item
// or a join after it is [filteredAndOrderedOnly]'s to refuse.
func singleFromItem(query []lexer.Token, index int) ([]string, int) {
	if wordAt(query, index) == "ONLY" {
		index++
	}
	base, next := foldedNameAt(query, index)
	if len(base) == 0 {
		return nil, next
	}
	if next < len(query) && query[next].MatchOperatorValue("*") {
		next++
	}
	if wordAt(query, next) == "AS" {
		next++
	}
	if word := wordAt(query, next); word != "" && word != "WHERE" && word != "ORDER" && !slices.Contains(shapedViewClauses, word) {
		next++
	} else if word == "" && next < len(query) && quotedIdentifier(query[next]) {
		next++
	}
	if next < len(query) && query[next].MatchOperatorValue("(") {
		next = closingParenthesis(query, next) + 1
	}
	return base, next
}

// filteredAndOrderedOnly reports whether the view query holds, from index
// on and outside parentheses, no word of [shapedViewClauses] and no comma but
// the ones between the items of an ORDER BY.
func filteredAndOrderedOnly(query []lexer.Token, index int) bool {
	depth := 0
	for at := index; at < len(query); at++ {
		switch {
		case query[at].MatchOperatorValue("("):
			depth++
		case query[at].MatchOperatorValue(")"):
			depth--
		case depth > 0:
		case query[at].MatchOperatorValue(","):
			if !orderByList(query, index, at) {
				return false
			}
		case slices.Contains(shapedViewClauses, wordAt(query, at)):
			return false
		}
	}
	return true
}

// orderByList reports whether the comma at index separates the items of a
// top-level ORDER BY that begins after start.
func orderByList(query []lexer.Token, start, index int) bool {
	depth := 0
	for at := start; at < index; at++ {
		switch {
		case query[at].MatchOperatorValue("("):
			depth++
		case query[at].MatchOperatorValue(")"):
			depth--
		case depth == 0 && wordAt(query, at) == "ORDER":
			return true
		}
	}
	return false
}

// lifetime is how long a relation a CREATE makes lives: as long as the schema
// that holds it, or as long as the session that creates it, in the session's
// temporary schema.
type lifetime int

const (
	schemaLifetime lifetime = iota
	sessionLifetime
)

// createModifiers skips the words between CREATE [OR REPLACE] and the kind of
// object a CREATE makes, and returns the lifetime they give the object.
func createModifiers(tokens []lexer.Token, index int) (int, lifetime) {
	life := schemaLifetime
	for {
		switch wordAt(tokens, index) {
		case "TEMP", "TEMPORARY":
			life = sessionLifetime
		case "GLOBAL", "LOCAL", "UNLOGGED", "RECURSIVE":
		default:
			return index, life
		}
		index++
	}
}

// afterIfNotExists skips IF NOT EXISTS and IF EXISTS at index.
func afterIfNotExists(tokens []lexer.Token, index int) int {
	if wordAt(tokens, index) != "IF" {
		return index
	}
	index++
	if wordAt(tokens, index) == "NOT" {
		index++
	}
	if wordAt(tokens, index) == "EXISTS" {
		index++
	}
	return index
}

func (g *relationGraph) applyCreate(p protection, tokens []lexer.Token) {
	orReplace := replay.TokenSequenceAt(tokens, 1, "OR", "REPLACE")
	kindIndex, life := createModifiers(tokens, postgresSpecialDDLKindIndex(tokens))
	recursive := false
	for index := postgresSpecialDDLKindIndex(tokens); index < kindIndex; index++ {
		recursive = recursive || wordAt(tokens, index) == "RECURSIVE"
	}
	switch wordAt(tokens, kindIndex) {
	case "TABLE":
		g.createTable(p, tokens, kindIndex+1, life)
	case "FOREIGN":
		if wordAt(tokens, kindIndex+1) == "TABLE" {
			g.createTable(p, tokens, kindIndex+2, schemaLifetime)
		}
	case "VIEW":
		g.createView(p, tokens, kindIndex+1, viewCreation{life: life, replace: orReplace, recursive: recursive})
	case "MATERIALIZED":
		if wordAt(tokens, kindIndex+1) == "VIEW" {
			g.createView(p, tokens, kindIndex+2, viewCreation{materialized: true})
		}
	case "RULE":
		g.createRule(p, tokens, kindIndex+1)
	case "CONSTRAINT":
		if wordAt(tokens, kindIndex+1) == "TRIGGER" {
			g.createTrigger(p, tokens, kindIndex+2)
		}
	case "TRIGGER":
		g.createTrigger(p, tokens, kindIndex+1)
	case "SCHEMA":
		g.createSchema(p, tokens, kindIndex+1)
	}
}

// createTable records `CREATE TABLE name ...`, a TABLE AS among them, and the
// tables a write to another table carries on to it: as a partition of
// PARTITION OF, as a child of INHERITS, and through a foreign key with a
// referential action ([referencingParents]).
func (g *relationGraph) createTable(p protection, tokens []lexer.Token, index int, life lifetime) {
	ifNotExists := wordAt(tokens, index) == "IF"
	parts, next := foldedNameAt(tokens, afterIfNotExists(tokens, index))
	key, ok := g.createKey(p, parts, life)
	if !ok {
		return
	}
	node := g.live(key)
	if node == nil || !ifNotExists {
		node = g.create(key)
	}
	for _, parent := range tableParents(tokens, next) {
		if ref := g.bindRef(p, parent); ref.node != nil {
			ref.node.children = append(ref.node.children, relationRef{node: node})
		}
	}
}

// tableParents returns the tables a CREATE TABLE or an ALTER TABLE, from
// index on, makes a write carry on to the table it creates or alters: its
// inheritance parents ([inheritanceParents]), and each table a foreign key
// REFERENCES with an ON DELETE or ON UPDATE action that writes the
// referencing rows ([referentialWrite]). A cascade fires the triggers and the
// rules of the referencing table.
func tableParents(tokens []lexer.Token, index int) [][]string {
	parents := inheritanceParents(tokens, index)
	for at := index; at < len(tokens); at++ {
		if wordAt(tokens, at) != "REFERENCES" {
			continue
		}
		if parts, next := foldedNameAt(tokens, at+1); len(parts) > 0 && referentialWrite(tokens, next) {
			parents = append(parents, parts)
		}
	}
	return parents
}

// inheritanceParents returns the tables a CREATE TABLE or an ALTER TABLE,
// from index on, makes the table it names a child of: the one after
// PARTITION OF, the ones INHERITS lists, and the one INHERIT names. NO
// INHERIT names a parent too: it changes the parent's record of its children
// as INHERIT does, and the graph keeps the child, as a write that reaches
// more is refused where it cannot tell.
func inheritanceParents(tokens []lexer.Token, index int) [][]string {
	var parents [][]string
	for ; index < len(tokens); index++ {
		switch wordAt(tokens, index) {
		case "PARTITION":
			if wordAt(tokens, index+1) == "OF" {
				if parts, _ := foldedNameAt(tokens, index+2); len(parts) > 0 {
					parents = append(parents, parts)
				}
			}
		case "INHERITS":
			if index+1 < len(tokens) && tokens[index+1].MatchOperatorValue("(") {
				parents = append(parents, nameList(tokens, index+2)...)
			}
		case "INHERIT":
			if parts, _ := foldedNameAt(tokens, index+1); len(parts) > 0 {
				parents = append(parents, parts)
			}
		}
	}
	return parents
}

// nameList reads the comma-separated dotted names that begin at index.
func nameList(tokens []lexer.Token, index int) [][]string {
	var names [][]string
	for {
		parts, next := foldedNameAt(tokens, index)
		if len(parts) == 0 {
			return names
		}
		names = append(names, parts)
		if next >= len(tokens) || !tokens[next].MatchOperatorValue(",") {
			return names
		}
		index = next + 1
	}
}

// referentialWrite reports whether the foreign key clause after a REFERENCES
// target, from index to the end of its column or constraint, has an ON DELETE
// or ON UPDATE action that writes the referencing rows: CASCADE, SET NULL or
// SET DEFAULT.
func referentialWrite(tokens []lexer.Token, index int) bool {
	depth := 0
	for ; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			if depth == 0 {
				return false
			}
			depth--
		case depth == 0 && tokens[index].MatchOperatorValue(","):
			return false
		case depth == 0 && wordAt(tokens, index) == "REFERENCES":
			return false
		case depth == 0 && wordAt(tokens, index) == "ON":
			switch wordAt(tokens, index+1) {
			case "DELETE", "UPDATE":
				switch wordAt(tokens, index+2) {
				case "CASCADE", "SET":
					return true
				}
			}
		}
	}
	return false
}

// viewCreation is how a CREATE makes a view.
type viewCreation struct {
	life                             lifetime
	replace, recursive, materialized bool
}

// createView records `CREATE [OR REPLACE] [TEMP] [RECURSIVE | MATERIALIZED]
// VIEW name [(columns)] [WITH (options)] AS query`. OR REPLACE keeps the
// relation, and what refers to it, and replaces its query. A materialized
// view cannot be written through, and a recursive one is read by every name
// its query holds.
func (g *relationGraph) createView(p protection, tokens []lexer.Token, index int, creation viewCreation) {
	parts, next := foldedNameAt(tokens, afterIfNotExists(tokens, index))
	key, ok := g.createKey(p, parts, creation.life)
	if !ok {
		return
	}
	node := g.live(key)
	if node == nil || !creation.replace {
		node = g.create(key)
	}
	node.viewBase, node.viewNames = nil, nil
	if creation.materialized {
		return
	}
	query := viewDefinition(tokens, next)
	if creation.recursive {
		node.viewNames = g.bindNames(p, query)
		return
	}
	node.viewBase, node.viewNames = g.viewEdges(p, query)
}

// viewDefinition returns the query of a CREATE VIEW whose name ends at index:
// the tokens after the AS that follows the column list and the WITH options.
func viewDefinition(tokens []lexer.Token, index int) []lexer.Token {
	for index < len(tokens) {
		switch {
		case tokens[index].MatchOperatorValue("("):
			index = closingParenthesis(tokens, index) + 1
		case wordAt(tokens, index) == "AS":
			return tokens[index+1:]
		default:
			index++
		}
	}
	return nil
}

// createRule records `CREATE [OR REPLACE] RULE name AS ON event TO table
// [WHERE condition] DO [ALSO | INSTEAD] action`, as the replay writes it and
// as pg_get_ruledef writes a rule the dev database holds. A SELECT rule makes
// the table a view of its action's query, as every view's rule is, and as a
// server older than PostgreSQL 16 turns a table into a view. Any other rule
// runs its action when the table is written, so a write to the table reaches
// every relation the action writes, bound when the rule is created.
func (g *relationGraph) createRule(p protection, tokens []lexer.Token, index int) {
	name, ok := foldedIdentifier(tokens, index)
	if !ok {
		return
	}
	on := findPostgresKeyword(tokens, "ON", index+1)
	if on == replay.MutationTargetNotFound {
		return
	}
	to := findPostgresKeyword(tokens, "TO", on+1)
	if to == replay.MutationTargetNotFound {
		return
	}
	table, next := foldedNameAt(tokens, to+1)
	do := findPostgresKeyword(tokens, "DO", next)
	if len(table) == 0 || do == replay.MutationTargetNotFound {
		return
	}
	node := g.existing(p, table)
	if node == nil {
		return
	}
	action := tokens[do+1:]
	if word := wordAt(action, 0); word == "ALSO" || word == "INSTEAD" {
		action = action[1:]
	}
	if wordAt(tokens, on+1) == "SELECT" {
		node.viewBase, node.viewNames = g.viewEdges(p, action)
		return
	}
	var refs []relationRef
	for _, target := range writeTargets(action) {
		refs = append(refs, g.bindRef(p, target))
	}
	if node.rules == nil {
		node.rules = make(map[string][]relationRef)
	}
	node.rules[name] = refs
}

// createTrigger records `CREATE [OR REPLACE] [CONSTRAINT] TRIGGER name ... ON
// table ... EXECUTE {FUNCTION | PROCEDURE} function(arguments)`: a write to
// the table reaches every relation a name in the arguments names when the
// write runs ([triggerArgumentTokens]).
func (g *relationGraph) createTrigger(p protection, tokens []lexer.Token, index int) {
	name, ok := foldedIdentifier(tokens, index)
	if !ok {
		return
	}
	on := findPostgresKeyword(tokens, "ON", index+1)
	execute := findPostgresKeyword(tokens, "EXECUTE", index+1)
	if on == replay.MutationTargetNotFound || execute == replay.MutationTargetNotFound {
		return
	}
	table, _ := foldedNameAt(tokens, on+1)
	node := g.existing(p, table)
	if node == nil {
		return
	}
	_, open := foldedNameAt(tokens, execute+2)
	var refs []relationRef
	for _, argument := range postgresCallArguments(tokens, open) {
		scanned, ok := triggerArgumentTokens(argument)
		if !ok {
			continue
		}
		for _, parts := range dottedNames(scanned) {
			refs = append(refs, relationRef{name: parts})
		}
	}
	if node.triggers == nil {
		node.triggers = make(map[string][]relationRef)
	}
	node.triggers[name] = refs
}

// dottedNames returns every dotted folded name tokens holds that is not
// called: a name followed by a parenthesis is a function.
func dottedNames(tokens []lexer.Token) [][]string {
	var names [][]string
	for index := 0; index < len(tokens); index++ {
		if index > 0 && tokens[index-1].MatchOperatorValue(".") {
			continue
		}
		parts, next := foldedNameAt(tokens, index)
		if len(parts) == 0 {
			continue
		}
		index = next - 1
		if next < len(tokens) && tokens[next].MatchOperatorValue("(") {
			continue
		}
		names = append(names, parts)
	}
	return names
}

// createSchema records `CREATE SCHEMA [IF NOT EXISTS] name [AUTHORIZATION
// role]` and `CREATE SCHEMA [IF NOT EXISTS] AUTHORIZATION role`, which names
// the schema after the role. CockroachDB accepts a schema qualified with its
// database.
func (g *relationGraph) createSchema(p protection, tokens []lexer.Token, index int) {
	index = afterIfNotExists(tokens, index)
	if wordAt(tokens, index) == "AUTHORIZATION" {
		index++
	}
	parts, _ := foldedNameAt(tokens, index)
	switch {
	case len(parts) == 1:
		g.schemas[parts[0]] = true
	case len(parts) == 2 && parts[0] == p.database:
		g.schemas[parts[1]] = true
	}
}

// relationKind returns the index of the name an ALTER or a DROP of a
// relation, a rule or a trigger names, and the kind: TABLE, VIEW, RULE,
// TRIGGER or SCHEMA, with MATERIALIZED VIEW and FOREIGN TABLE read as VIEW
// and TABLE.
func relationKind(tokens []lexer.Token) (string, int) {
	switch kind := wordAt(tokens, 1); kind {
	case "TABLE", "VIEW", "RULE", "TRIGGER", "SCHEMA":
		return kind, afterIfNotExists(tokens, 2)
	case "MATERIALIZED":
		if wordAt(tokens, 2) == "VIEW" {
			return "VIEW", afterIfNotExists(tokens, 3)
		}
	case "FOREIGN":
		if wordAt(tokens, 2) == "TABLE" {
			return "TABLE", afterIfNotExists(tokens, 3)
		}
	}
	return "", 0
}

func (g *relationGraph) applyAlter(p protection, tokens []lexer.Token) {
	kind, index := relationKind(tokens)
	switch kind {
	case "TABLE", "VIEW":
		if wordAt(tokens, index) == "ONLY" {
			index++
		}
		parts, next := foldedNameAt(tokens, index)
		if next < len(tokens) && tokens[next].MatchOperatorValue("*") {
			next++
		}
		node := g.existing(p, parts)
		if node == nil {
			return
		}
		g.alterRelation(p, node, tokens, next)
	case "RULE":
		g.renameMember(p, tokens, index, func(node *relationNode) map[string][]relationRef { return node.rules })
	case "TRIGGER":
		g.renameMember(p, tokens, index, func(node *relationNode) map[string][]relationRef { return node.triggers })
	}
}

// alterRelation records what `ALTER TABLE | VIEW name` changes from index on:
// RENAME TO and SET SCHEMA move the relation, ATTACH PARTITION makes a write
// carry on to the partition, and INHERIT and a foreign key make one carry on
// to this table ([tableParents]).
func (g *relationGraph) alterRelation(p protection, node *relationNode, tokens []lexer.Token, index int) {
	switch {
	case replay.TokenSequenceAt(tokens, index, "RENAME", "TO"):
		if renamed, _ := foldedNameAt(tokens, index+2); len(renamed) == 1 {
			g.rebind(node, relationKey{schema: node.key.schema, name: renamed[0]})
		}
		return
	case replay.TokenSequenceAt(tokens, index, "SET", "SCHEMA"):
		if schema, _ := foldedNameAt(tokens, index+2); len(schema) == 1 {
			g.rebind(node, relationKey{schema: schema[0], name: node.key.name})
		}
		return
	}
	for at := index; at < len(tokens); at++ {
		if replay.TokenSequenceAt(tokens, at, "ATTACH", "PARTITION") {
			if partition, _ := foldedNameAt(tokens, at+2); len(partition) > 0 {
				if child := g.existing(p, partition); child != nil {
					node.children = append(node.children, relationRef{node: child})
				}
			}
		}
	}
	for _, parent := range tableParents(tokens, index) {
		if ref := g.bindRef(p, parent); ref.node != nil {
			ref.node.children = append(ref.node.children, relationRef{node: node})
		}
	}
}

// renameMember records `ALTER RULE | TRIGGER name ON table RENAME TO new`.
func (g *relationGraph) renameMember(
	p protection,
	tokens []lexer.Token,
	index int,
	members func(*relationNode) map[string][]relationRef,
) {
	name, ok := foldedIdentifier(tokens, index)
	if !ok || wordAt(tokens, index+1) != "ON" {
		return
	}
	table, next := foldedNameAt(tokens, index+2)
	renamed, ok := foldedIdentifier(tokens, next+2)
	node := g.resolve(p, table)
	if !ok || node == nil || !replay.TokenSequenceAt(tokens, next, "RENAME", "TO") {
		return
	}
	byName := members(node)
	if refs, found := byName[name]; found {
		delete(byName, name)
		byName[renamed] = refs
	}
}

func (g *relationGraph) applyDrop(p protection, tokens []lexer.Token) {
	kind, index := relationKind(tokens)
	switch kind {
	case "TABLE", "VIEW":
		for _, parts := range nameList(tokens, index) {
			if node := g.resolve(p, parts); node != nil {
				node.dropped = true
				delete(g.byKey, node.key)
			}
		}
	case "RULE", "TRIGGER":
		name, ok := foldedIdentifier(tokens, index)
		if !ok || wordAt(tokens, index+1) != "ON" {
			return
		}
		table, _ := foldedNameAt(tokens, index+2)
		if node := g.resolve(p, table); node != nil {
			delete(node.rules, name)
			if kind == "TRIGGER" {
				delete(node.triggers, name)
			}
		}
	case "SCHEMA":
		for _, parts := range nameList(tokens, index) {
			schema := parts[len(parts)-1]
			delete(g.schemas, schema)
			for key, node := range g.byKey {
				if key.schema == schema {
					node.dropped = true
					delete(g.byKey, key)
				}
			}
		}
	}
}

// applySelectInto records the table `SELECT ... INTO [TEMP] [TABLE] name`
// creates.
func (g *relationGraph) applySelectInto(p protection, tokens []lexer.Token) {
	if wordAt(tokens, 0) != "SELECT" {
		return
	}
	depth := 0
	for index := 1; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
		case depth == 0 && wordAt(tokens, index) == "FROM":
			return
		case depth == 0 && wordAt(tokens, index) == "INTO":
			at, life := createModifiers(tokens, index+1)
			if wordAt(tokens, at) == "TABLE" {
				at++
			}
			parts, _ := foldedNameAt(tokens, at)
			if key, ok := g.createKey(p, parts, life); ok {
				g.create(key)
			}
			return
		}
	}
}
