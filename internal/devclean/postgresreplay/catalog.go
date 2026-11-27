package postgresreplay

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// catalogClass is the catalog the server looks an unqualified name up in, for
// one kind of object: a table and a sequence share pg_class, so `ALTER
// SEQUENCE pg_class` and `UPDATE pg_class` reach the same name, while a
// function named like a table is another object.
type catalogClass string

// The classes are the catalogs whose objects belong to a namespace and can be
// named by a statement the guard reads. Each value is the tag
// [sessionCatalogQuery] gives the catalog's rows.
const (
	// classRelation is pg_class: a table, a view, a materialized view, a
	// foreign table, a sequence or an index.
	classRelation catalogClass = "r"
	// classRoutine is pg_proc: a function, a procedure or an aggregate.
	classRoutine catalogClass = "f"
	// classType is pg_type: a type or a domain.
	classType      catalogClass = "t"
	classCollation catalogClass = "c"
	// classConversion is pg_conversion, an encoding conversion.
	classConversion catalogClass = "v"
	// classOperator is pg_operator. Its names are operator symbols.
	classOperator catalogClass = "o"
	// classOperatorClass is pg_opclass and pg_opfamily: OPERATOR CLASS and
	// OPERATOR FAMILY name one of either.
	classOperatorClass catalogClass = "k"
	// classTextSearch is the four text search catalogs.
	classTextSearch catalogClass = "s"
	// classStatistics is pg_statistic_ext, an extended statistics object.
	classStatistics catalogClass = "x"
)

// sessionCatalogQuery reads, in one row, what [guard.ValidateSession] needs
// from the dev session of dialect: its client_encoding, its
// standard_conforming_strings, the names an unqualified name can reach, the
// schemas a CREATE can put an object in when its name has no schema, the
// rules of the dev database, and the database's name. The names are a JSON
// array of [class, namespace, names] for every class and namespace of the
// session's search path, the schemas the server searches without being told
// included: pg_catalog, the session's temporary schema and, on CockroachDB,
// pg_extension.
// [readCatalogNames] keeps the namespaces the server owns, and the relations
// of the other namespaces for [loadRelationGraph]. The search path is a JSON
// object read by [readSessionPath]: current_schemas(false), the schemas of
// the search path that exist without the implicit ones, which
// [protection.creationNamespace] reads; the search_path setting and the
// session's user, from which [relationGraph] looks up a relation named
// without a schema, schemas created later included; and the schemas of the
// dev database. The rules are read by [loadRelationGraph], and the
// database's name by [protection.qualifierNamespace]. CURRENT_USER is a
// keyword, not a function, and takes no schema.
//
// The names are read from the session rather than declared because they are
// the server's: PostgreSQL 16 holds 264 relations in pg_catalog and 19 holds
// 274, YugabyteDB adds yb_ and gv$ views, and CockroachDB keeps three PostGIS
// tables in pg_extension. Every function is called qualified, so a session
// whose search path puts another schema first is read the same way.
// COALESCE is a keyword, not a function, and takes no schema.
func sessionCatalogQuery(dialect string) string {
	rules := sessionRulesColumn
	if dialect == platform.CockroachDB {
		// CockroachDB has no rules, writes through no view ("v" is not a
		// table) and has no pg_get_ruledef, measured on 26.4.
		rules = `'[]'`
	}
	return sessionCatalogColumns + ",\n\t" + rules + ",\n\tpg_catalog.current_database()"
}

const sessionCatalogColumns = `SELECT pg_catalog.current_setting('client_encoding'),
	pg_catalog.current_setting('standard_conforming_strings'),
	(SELECT COALESCE(pg_catalog.json_agg(pg_catalog.json_build_array(o.class, n.nspname, o.names))::text, '[]')
	FROM (
		SELECT a.class, a.ns, pg_catalog.array_agg(DISTINCT a.name) AS names
		FROM (
			SELECT 'r' AS class, relnamespace AS ns, relname::text AS name FROM pg_catalog.pg_class
			UNION ALL SELECT 'f', pronamespace, proname::text FROM pg_catalog.pg_proc
			UNION ALL SELECT 't', typnamespace, typname::text FROM pg_catalog.pg_type
			UNION ALL SELECT 'c', collnamespace, collname::text FROM pg_catalog.pg_collation
			UNION ALL SELECT 'v', connamespace, conname::text FROM pg_catalog.pg_conversion
			UNION ALL SELECT 'o', oprnamespace, oprname::text FROM pg_catalog.pg_operator
			UNION ALL SELECT 'k', opcnamespace, opcname::text FROM pg_catalog.pg_opclass
			UNION ALL SELECT 'k', opfnamespace, opfname::text FROM pg_catalog.pg_opfamily
			UNION ALL SELECT 's', cfgnamespace, cfgname::text FROM pg_catalog.pg_ts_config
			UNION ALL SELECT 's', dictnamespace, dictname::text FROM pg_catalog.pg_ts_dict
			UNION ALL SELECT 's', prsnamespace, prsname::text FROM pg_catalog.pg_ts_parser
			UNION ALL SELECT 's', tmplnamespace, tmplname::text FROM pg_catalog.pg_ts_template
			UNION ALL SELECT 'x', stxnamespace, stxname::text FROM pg_catalog.pg_statistic_ext
		) AS a
		GROUP BY a.class, a.ns
	) AS o
	JOIN pg_catalog.pg_namespace AS n ON n.oid = o.ns
	WHERE n.nspname = ANY (pg_catalog.current_schemas(true))),
	pg_catalog.json_build_object(
		'existing', pg_catalog.current_schemas(false),
		'setting', pg_catalog.current_setting('search_path'),
		'user', CURRENT_USER,
		'schemas', (SELECT pg_catalog.array_agg(nspname::text) FROM pg_catalog.pg_namespace))::text`

// sessionRulesColumn is a JSON array of [namespace, relation, definition] for
// every rule the dev database holds that initdb did not create: a view's
// SELECT rule, and an INSERT, UPDATE or DELETE rule of a table or a view.
// FirstNormalObjectId, 16384, is the first object identifier a server gives
// an object created after initdb, so the views of pg_catalog and
// information_schema are left out, and an extension's are kept.
const sessionRulesColumn = `(SELECT COALESCE(pg_catalog.json_agg(pg_catalog.json_build_array(n.nspname, c.relname,
		pg_catalog.pg_get_ruledef(w.oid)))::text, '[]')
	FROM pg_catalog.pg_rewrite AS w
	JOIN pg_catalog.pg_class AS c ON c.oid = w.ev_class
	JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
	WHERE w.oid >= 16384)`

// sessionNames is what [guard.ValidateSession] read about how the dev session
// resolves a name written without a schema: where a reference finds it, and
// where a CREATE puts it.
type sessionNames struct {
	catalog catalogNames
	// path is the schemas a CREATE can put an object in when its name has no
	// schema, in the order of the search path, or none when the search path
	// names no schema that exists; see [protection.creationNamespace].
	path []string
	// database is the name of the dev database, or "" for a session the
	// guard did not read; see [protection.qualifierNamespace].
	database string
	// graph is the guard's [relationGraph], which [guard.Validate] sets on
	// the copy it reads a statement with.
	graph *relationGraph
}

// catalogNames is, for each class, the names an unqualified name reaches in a
// namespace the server owns, folded to lower case, and the namespace that
// holds each. A nil or empty value is a guard that read no session; see
// [fallbackCatalogNamespace].
type catalogNames map[catalogClass]map[string]string

// readCatalogNames decodes the third column of [sessionCatalogQuery], and
// returns the names in a namespace the server of dialect owns and the names
// of the relations of each other namespace.
func readCatalogNames(dialect, encoded string) (catalogNames, map[string][]string, error) {
	var rows [][3]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &rows); err != nil {
		return nil, nil, fmt.Errorf("read the dev session's catalog names: %w", err)
	}
	policy := protection{dialect: dialect}
	names := make(catalogNames)
	relations := make(map[string][]string)
	for _, row := range rows {
		var class, namespace string
		var members []string
		if err := json.Unmarshal(row[0], &class); err != nil {
			return nil, nil, fmt.Errorf("read the dev session's catalog names: %w", err)
		}
		if err := json.Unmarshal(row[1], &namespace); err != nil {
			return nil, nil, fmt.Errorf("read the dev session's catalog names: %w", err)
		}
		if err := json.Unmarshal(row[2], &members); err != nil {
			return nil, nil, fmt.Errorf("read the dev session's catalog names: %w", err)
		}
		if !policy.isProtectedPostgresNamespace(namespace) {
			if catalogClass(class) == classRelation {
				relations[namespace] = append(relations[namespace], members...)
			}
			continue
		}
		byName := names[catalogClass(class)]
		if byName == nil {
			byName = make(map[string]string)
			names[catalogClass(class)] = byName
		}
		for _, member := range members {
			byName[strings.ToLower(member)] = namespace
		}
	}
	return names, relations, nil
}

// fallbackCatalogNamespace is the namespace a guard that read no session takes
// an unqualified name of any class to reach, or "". Every production guard
// reads its session first (devclean.NewDevReplayGuardFor), and so does one
// whose read named nothing, which no server answers.
//
// It is the relation names of the supported lines, measured on PostgreSQL 16,
// 18 and 19, YugabyteDB 2026.1 and CockroachDB 26.4: every relation of
// pg_catalog begins with pg_ on PostgreSQL and CockroachDB, YugabyteDB adds
// views that begin with yb_ or gv$, and CockroachDB holds three PostGIS tables
// in pg_extension. The rule misses a routine or a type whose name has no such
// prefix, now() or int4, which only a session's catalog names.
func fallbackCatalogNamespace(dialect, name string) string {
	switch {
	case strings.HasPrefix(name, "pg_"):
		return "pg_catalog"
	case dialect == platform.YugabyteDB && (strings.HasPrefix(name, "yb_") || strings.HasPrefix(name, "gv$")):
		return "pg_catalog"
	case dialect == platform.CockroachDB:
		switch name {
		case "geography_columns", "geometry_columns", "spatial_ref_sys":
			return "pg_extension"
		}
	}
	return ""
}

// unqualifiedNamespace returns the namespace the server owns that it finds an
// object of class named name in, for a name written without a schema, or "".
// It compares the name in any case: a quoted "PG_CLASS" is another table than
// pg_class, and is refused all the same, since a schema in front of it says
// which one it is.
//
// The server searches pg_catalog, and the session's temporary schema, before
// any schema of the search path that does not name them, so an unqualified
// name the catalog holds there reaches the catalog's object, whatever schema
// the author meant. The guard accepts no search path but the empty one and the
// one the session starts with ([postgresSettingReach]), and the names were
// read under the latter, which searches every schema the former does. The name
// a CREATE gives its object is not looked up; see [protection.creationNamespace].
func (p protection) unqualifiedNamespace(class catalogClass, name string) string {
	folded := strings.ToLower(name)
	var namespace string
	if len(p.catalog) == 0 {
		namespace = fallbackCatalogNamespace(p.dialect, folded)
	} else {
		namespace = p.catalog[class][folded]
	}
	if namespace != "" && p.unqualified != nil && *p.unqualified == "" {
		*p.unqualified = name
	}
	return namespace
}

// creationNamespace returns the namespace the server owns that a CREATE can
// put name in when name is written without a schema, or "".
//
// A CREATE does not look its name up. The server puts the object in the
// session's creation namespace, the first schema of the search path that
// exists, and never in pg_catalog unless the path lists pg_catalog: `CREATE
// TYPE point` makes a public.point beside pg_catalog.point, and `CREATE TABLE
// pg_class` a public.pg_class. Reading the name through
// [protection.unqualifiedNamespace] would refuse both for an object they do
// not touch. A later statement that names the object without a schema is a
// reference and is still looked up, so `INSERT INTO pg_class` after that
// CREATE is refused, as the server would write pg_catalog.pg_class.
//
// The path is read once, by [guard.ValidateSession], and the creation
// namespace is not fixed by it. The server skips a schema of the path that
// does not exist, so an accepted `DROP SCHEMA public CASCADE` makes the next
// schema of the path the creation namespace: measured on PostgreSQL 18, a
// session whose path is public, pg_catalog then creates a domain, an operator
// and a type in pg_catalog. So every schema of the path counts, and a path
// that lists a namespace the server owns anywhere refuses every such CREATE,
// as a path that starts with one does. The replay can change the search path
// only to the empty path or back to the one the session starts with
// ([postgresSettingReach]), and under the empty path a CREATE with no schema
// fails on the server, so the schemas read are the only ones such a CREATE
// can reach. A session that read nothing, or whose path names no schema that
// exists, has no creation namespace and a CREATE is accepted, as the server
// either refuses it or puts it in a schema the guard does not own. A
// temporary table is refused on a path that lists a namespace the server
// owns, although `CREATE TEMP TABLE` would go to the temporary schema; the
// session's own temporary schema on the path is accepted where session tables
// are, as [protection.protectedNamespaceFromMutationTarget] accepts pg_temp.
func (p protection) creationNamespace(name string) string {
	for _, namespace := range p.path {
		if !p.isProtectedPostgresNamespace(namespace) {
			continue
		}
		if p.sessionTables && (namespace == "pg_temp" || strings.HasPrefix(namespace, "pg_temp_")) {
			continue
		}
		if p.unqualified != nil && *p.unqualified == "" {
			*p.unqualified = name
		}
		return namespace
	}
	return ""
}

// sessionPath is the fourth column of [sessionCatalogQuery].
type sessionPath struct {
	// Existing is current_schemas(false): the schemas of the search path that
	// exist, in its order, without the implicit ones.
	Existing []string `json:"existing"`
	// Setting is the search_path setting as the session holds it.
	Setting string `json:"setting"`
	// User is the session's user, which $user in the setting stands for.
	User string `json:"user"`
	// Schemas are the schemas of the dev database.
	Schemas []string `json:"schemas"`
}

// readSessionPath decodes the fourth column of [sessionCatalogQuery].
func readSessionPath(encoded string) (sessionPath, error) {
	var path sessionPath
	if err := json.Unmarshal([]byte(encoded), &path); err != nil {
		return sessionPath{}, fmt.Errorf("read the dev session's search path: %w", err)
	}
	return path, nil
}

// searchPath returns the schemas the search_path setting lists, as the
// server reads them: separated by commas, an unquoted name folded to lower
// case and a double-quoted one kept with its doubled quotes undone, and $user
// read as the session's user. A schema listed that does not exist is kept,
// since the replay can create it.
func (s sessionPath) searchPath() []string {
	var schemas []string
	for _, item := range splitSearchPath(s.Setting) {
		if item == "$user" {
			item = s.User
		}
		if item != "" {
			schemas = append(schemas, item)
		}
	}
	return schemas
}

// splitSearchPath splits a search_path setting into its names, as the
// server's SplitIdentifierString does.
func splitSearchPath(setting string) []string {
	var names []string
	var name strings.Builder
	quoted, inQuotes := false, false
	flush := func() {
		item := name.String()
		if !quoted {
			item = strings.ToLower(strings.TrimSpace(item))
		}
		names = append(names, item)
		name.Reset()
		quoted = false
	}
	for index := 0; index < len(setting); index++ {
		character := setting[index]
		switch {
		case inQuotes && character == '"' && index+1 < len(setting) && setting[index+1] == '"':
			name.WriteByte('"')
			index++
		case character == '"':
			inQuotes = !inQuotes
			quoted = true
		case inQuotes:
			name.WriteByte(character)
		case character == ',':
			flush()
		case character == ' ' || character == '\t' || character == '\n':
			if name.Len() > 0 && !quoted {
				name.WriteByte(character)
			}
		default:
			name.WriteByte(character)
		}
	}
	if strings.TrimSpace(setting) != "" {
		flush()
	}
	return names
}

// createdNameAt returns the namespace the server owns that the name a CREATE
// creates, beginning at index, is in: the schema it is qualified with, or the
// session's creation namespace when it has none, as
// [protection.creationNamespace] answers. An operator's name is its whole
// symbol ([operatorSymbolAt]).
func (p protection) createdNameAt(tokens []lexer.Token, index int) string {
	if namespace := p.protectedPostgresQualifiedNamespaceAt(tokens, index); namespace != "" {
		return namespace
	}
	if index < 0 || index >= len(tokens) || index > 0 && tokens[index-1].MatchOperatorValue(".") ||
		index+1 < len(tokens) && tokens[index+1].MatchOperatorValue(".") {
		return ""
	}
	token := tokens[index]
	if symbol, ok := operatorSymbolAt(tokens, index); ok {
		return p.creationNamespace(symbol)
	}
	if token.Type == lexer.TokenIdentifier || quotedIdentifier(token) {
		return p.creationNamespace(replay.IdentifierValue(token))
	}
	return ""
}

// unqualifiedNameAt returns the namespace the server owns that the name at
// index reaches when it is written without a schema, as
// [protection.unqualifiedNamespace] answers for class, or "". A name followed
// by a dot is a qualifier, read by [protection.protectedPostgresQualifiedNamespaceAt].
// An operator's name is its symbol.
func (p protection) unqualifiedNameAt(class catalogClass, tokens []lexer.Token, index int) string {
	if index < 0 || index >= len(tokens) || index > 0 && tokens[index-1].MatchOperatorValue(".") ||
		index+1 < len(tokens) && tokens[index+1].MatchOperatorValue(".") {
		return ""
	}
	token := tokens[index]
	if class == classOperator {
		if symbol, ok := operatorSymbolAt(tokens, index); ok {
			return p.unqualifiedNamespace(class, symbol)
		}
	}
	if token.Type == lexer.TokenIdentifier || quotedIdentifier(token) {
		return p.unqualifiedNamespace(class, replay.IdentifierValue(token))
	}
	return ""
}

// operatorSymbolAt reads the operator symbol that begins at index: the
// operator characters written there with nothing between them, which the
// lexer reads one token each, so `|||` is one name and not `|`. It reports
// false for a token that is no operator character, and for one that
// continues a symbol begun before index, which is read from its start.
func operatorSymbolAt(tokens []lexer.Token, index int) (string, bool) {
	if index < 0 || index >= len(tokens) || !operatorCharacters(tokens[index]) {
		return "", false
	}
	if index > 0 && operatorCharacters(tokens[index-1]) && tokens[index-1].End == tokens[index].Start {
		return "", false
	}
	var symbol strings.Builder
	symbol.WriteString(tokens[index].Value)
	for next := index + 1; next < len(tokens) && operatorCharacters(tokens[next]) && tokens[next-1].End == tokens[next].Start; next++ {
		symbol.WriteString(tokens[next].Value)
	}
	return symbol.String(), true
}

// operatorCharacters reports an operator token written only in the characters
// PostgreSQL builds an operator name from.
func operatorCharacters(token lexer.Token) bool {
	return token.Type == lexer.TokenOperator && token.Value != "" &&
		strings.Trim(token.Value, "+-*/<>=~!@#%^&|`?") == ""
}

// protectedNameAt returns the reader of a name of class: the namespace the
// server owns that the name beginning at a token is qualified with, or that
// it reaches unqualified, or "".
func (p protection) protectedNameAt(class catalogClass) func([]lexer.Token, int) string {
	return func(tokens []lexer.Token, index int) string {
		if namespace := p.protectedPostgresQualifiedNamespaceAt(tokens, index); namespace != "" {
			return namespace
		}
		return p.unqualifiedNameAt(class, tokens, index)
	}
}

// objectKindClass returns the class of the object a kind word names, the
// word at index of `COMMENT ON kind name`, `DROP kind name` or `GRANT ...
// ON kind name`, and false for a kind that names no object of a namespace: a
// schema, a role, a database, an extension, a trigger, a policy or a rule,
// whose name belongs to its table, and the rest. A privilege written with no
// kind, `GRANT SELECT ON name`, is on a table.
func objectKindClass(tokens []lexer.Token, index int) (catalogClass, bool) {
	switch replay.NormalizedIdentifier(replay.TokenAt(tokens, index)) {
	case "TABLE", "VIEW", "SEQUENCE", "INDEX", "UNIQUE", "COLUMN":
		return classRelation, true
	case "MATERIALIZED":
		return classRelation, replay.TokenSequenceAt(tokens, index, "MATERIALIZED", "VIEW")
	case "FOREIGN":
		return classRelation, replay.TokenSequenceAt(tokens, index, "FOREIGN", "TABLE")
	case "FUNCTION", "PROCEDURE", "ROUTINE", "AGGREGATE":
		return classRoutine, true
	case "TYPE", "DOMAIN":
		return classType, true
	case "COLLATION":
		return classCollation, true
	case "CONVERSION":
		return classConversion, true
	case "OPERATOR":
		if replay.TokenSequenceAt(tokens, index, "OPERATOR", "CLASS") ||
			replay.TokenSequenceAt(tokens, index, "OPERATOR", "FAMILY") {
			return classOperatorClass, true
		}
		return classOperator, true
	case "TEXT":
		return classTextSearch, replay.TokenSequenceAt(tokens, index, "TEXT", "SEARCH")
	case "STATISTICS":
		return classStatistics, true
	}
	return "", false
}

// copyTargetNamespace reads `COPY name [(columns)] FROM ...` and returns the
// namespace the server owns that name is in, qualified or not, or "". COPY
// ... FROM writes the rows it reads into the table, STDIN included, and COPY
// ... TO reads them, as COPY (query) TO does; a COPY that is not read as TO is
// read as a write.
func (p protection) copyTargetNamespace(tokens []lexer.Token) string {
	if len(tokens) < 2 || tokens[1].MatchOperatorValue("(") {
		return ""
	}
	_, next := replay.QualifiedIdentifierAt(tokens, 1)
	if next < len(tokens) && tokens[next].MatchOperatorValue("(") {
		next = closingParenthesis(tokens, next) + 1
	}
	if replay.TokenSequenceAt(tokens, next, "TO") {
		return ""
	}
	target, _ := replay.QualifiedIdentifierAt(tokens, 1)
	if namespace := p.writeTargetNamespace(target); namespace != "" {
		return namespace
	}
	folded, _ := foldedNameAt(tokens, 1)
	return p.graphReach(folded)
}

// closingParenthesis returns the index of the parenthesis that closes the one
// at open, or the last index when none does.
func closingParenthesis(tokens []lexer.Token, open int) int {
	depth := 0
	for index := open; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return len(tokens) - 1
}

// unqualifiedRemedy ends the refusal of a name written without a schema that
// reaches a namespace the server owns.
const unqualifiedRemedy = "qualify the name with the schema of the object it means"

// refuseProtectedNamespace is the refusal of operation, a change to a
// namespace the server owns. unqualified is the name written without a schema
// that reached the namespace, and the refusal then says how to name another
// object. through names the object that carries the change to the namespace,
// such as `the relation "v"` for a view the run created over pg_authid. The
// remedy follows it only when the statement names that object without a
// schema, since the object the server finds for such a name depends on the
// search path, and naming it with its schema reaches the same namespace
// otherwise. Both are "" for a statement that names the namespace itself.
func refuseProtectedNamespace(dialect, operation, unqualified, through string) error {
	switch {
	case through != "" && unqualified != "":
		return replay.RefuseWithRemedy(dialect, fmt.Sprintf("%s through %s", operation, through), unqualifiedRemedy)
	case through != "":
		return replay.Refuse(dialect, fmt.Sprintf("%s through %s", operation, through))
	case unqualified != "":
		return replay.RefuseWithRemedy(dialect,
			fmt.Sprintf("%s through the unqualified name %q", operation, unqualified), unqualifiedRemedy)
	default:
		return replay.Refuse(dialect, operation)
	}
}
