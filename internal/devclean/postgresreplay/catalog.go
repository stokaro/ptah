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
// from the dev session: its client_encoding, its standard_conforming_strings,
// and the names an unqualified name can reach, as a JSON array of [class,
// namespace, names] for every class and namespace of the session's search
// path, the schemas the server searches without being told included:
// pg_catalog, the session's temporary schema and, on CockroachDB,
// pg_extension. [readCatalogNames] keeps the namespaces the server owns.
//
// The names are read from the session rather than declared because they are
// the server's: PostgreSQL 16 holds 264 relations in pg_catalog and 19 holds
// 274, YugabyteDB adds yb_ and gv$ views, and CockroachDB keeps three PostGIS
// tables in pg_extension. Every function is called qualified, so a session
// whose search path puts another schema first is read the same way.
// COALESCE is a keyword, not a function, and takes no schema.
const sessionCatalogQuery = `SELECT pg_catalog.current_setting('client_encoding'),
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
	WHERE n.nspname = ANY (pg_catalog.current_schemas(true)))`

// catalogNames is, for each class, the names an unqualified name reaches in a
// namespace the server owns, folded to lower case, and the namespace that
// holds each. A nil or empty value is a guard that read no session; see
// [fallbackCatalogNamespace].
type catalogNames map[catalogClass]map[string]string

// readCatalogNames decodes the third column of [sessionCatalogQuery] and keeps
// the names in a namespace the server of dialect owns.
func readCatalogNames(dialect, encoded string) (catalogNames, error) {
	var rows [][3]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &rows); err != nil {
		return nil, fmt.Errorf("read the dev session's catalog names: %w", err)
	}
	policy := protection{dialect: dialect}
	names := make(catalogNames)
	for _, row := range rows {
		var class, namespace string
		var members []string
		if err := json.Unmarshal(row[0], &class); err != nil {
			return nil, fmt.Errorf("read the dev session's catalog names: %w", err)
		}
		if err := json.Unmarshal(row[1], &namespace); err != nil {
			return nil, fmt.Errorf("read the dev session's catalog names: %w", err)
		}
		if err := json.Unmarshal(row[2], &members); err != nil {
			return nil, fmt.Errorf("read the dev session's catalog names: %w", err)
		}
		if !policy.isProtectedPostgresNamespace(namespace) {
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
	return names, nil
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
// read under the latter, which searches every schema the former does.
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
	switch {
	case class == classOperator && token.Type == lexer.TokenOperator:
		return p.unqualifiedNamespace(class, token.Value)
	case token.Type == lexer.TokenIdentifier || quotedIdentifier(token):
		return p.unqualifiedNamespace(class, replay.IdentifierValue(token))
	}
	return ""
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
	return p.protectedNameAt(classRelation)(tokens, 1)
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
// that reached the namespace, or "" for a statement that names the namespace
// itself; the refusal then says how to name another object.
func refuseProtectedNamespace(dialect, operation, unqualified string) error {
	if unqualified == "" {
		return replay.Refuse(dialect, operation)
	}
	return replay.RefuseWithRemedy(dialect,
		fmt.Sprintf("%s through the unqualified name %q", operation, unqualified), unqualifiedRemedy)
}
