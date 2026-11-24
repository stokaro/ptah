package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// The cleanup of a server that commits each drop as it runs refuses, before
// its first drop, an object it does not drop that depends on one it does. A
// refusal at that object's drop would come after the retry had dropped every
// other object, and none of those drops rolls back.
//
// The catalog reports most of these dependencies, and
// outsideStructuredDependentsQuery reads them. CockroachDB reports the others
// only as text, so outsideTextDependentsQuery reads that text and
// refuseOutsideCleanupDependents matches it: a column default, a computed
// column, an ON UPDATE expression, a check constraint, a view definition, a
// policy expression, an index expression or predicate and a trigger's WHEN
// condition name the sequences, functions and types they use, and a routine
// body names the relations, sequences and types it uses. The text match
// refuses on a name that could be the object rather than miss one, so a body
// that only quotes the name in a string is refused too.
//
// The text can lose a type: CockroachDB v26.4.1 keeps `NULL::public.e[]` in a
// view or a body as `NULL`, and a default `'a'::public.e::STRING` as
// `'a'::text`, and still refuses to drop the type while they exist. So for
// each type it drops the cleanup also asks the server, through EXPLAIN (DDL)
// of its drop, which objects depend on it. What none of these reports reaches
// the server's refusal at the drop, and the error then lists the drops that
// ran before it.

// outsideStructuredDependentsQuery asks which objects depend on an object of
// the schemas named, wherever the dependent lives, through the catalog row
// that records the dependency. Each row is one dependency: how it is recorded,
// the family, schema and name of the object depended on, the family, schema,
// name and kind of the dependent, and the constraint, trigger or column that
// carries it. A family is relation, routine or type, the three name spaces a
// cleanup object lives in. The caller keeps the rows whose source the cleanup
// drops and whose dependent it does not.
//
// The branches cover a rule that uses a relation (the pg_depend rows of class
// pg_rewrite), a foreign key that references a table (pg_constraint), a
// routine that calls a routine (pg_depend), a trigger that runs a function
// (pg_trigger), a column of a type or of an array of it (pg_attribute), and a
// routine that takes or returns one (pg_proc). The row type of a table or a
// view is that relation, which the cleanup lists as a relation and not as a
// type. Measured on CockroachDB v26.4.1, pg_depend records the call and not
// the trigger, the column type or the signature, so each is read from the
// catalog that holds it, and a function in another schema that returns SETOF
// a listed table keeps the table from being dropped. A rule is a view's
// definition, and on CockroachDB also a table whose trigger runs a function
// that uses the relation: the rule is then the table's, so the dependent is
// named by its own kind rather than as a view.
const outsideStructuredDependentsQuery = `
	SELECT 'rule', 'relation', source_ns.nspname, source.relname,
		'relation', dependent_ns.nspname, dependent.relname, dependent.relkind::text, ''
	FROM pg_depend d
	JOIN pg_rewrite r ON r.oid = d.objid
	JOIN pg_class dependent ON dependent.oid = r.ev_class
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
	JOIN pg_class source ON source.oid = d.refobjid
	JOIN pg_namespace source_ns ON source_ns.oid = source.relnamespace
	WHERE d.classid = 'pg_catalog.pg_rewrite'::regclass
		AND dependent.oid <> source.oid AND source_ns.nspname IN (%[1]s)
	UNION ALL
	SELECT 'foreign key', 'relation', source_ns.nspname, source.relname,
		'relation', dependent_ns.nspname, dependent.relname, dependent.relkind::text, con.conname::text
	FROM pg_constraint con
	JOIN pg_class source ON source.oid = con.confrelid
	JOIN pg_namespace source_ns ON source_ns.oid = source.relnamespace
	JOIN pg_class dependent ON dependent.oid = con.conrelid
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
	WHERE con.contype = 'f' AND source_ns.nspname IN (%[1]s)
	UNION ALL
	SELECT 'call', 'routine', source_ns.nspname, source.proname,
		'routine', dependent_ns.nspname, dependent.proname, dependent.prokind::text, ''
	FROM pg_depend d
	JOIN pg_proc source ON source.oid = d.refobjid
	JOIN pg_namespace source_ns ON source_ns.oid = source.pronamespace
	JOIN pg_proc dependent ON dependent.oid = d.objid
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.pronamespace
	WHERE d.classid = 'pg_catalog.pg_proc'::regclass AND d.refclassid = 'pg_catalog.pg_proc'::regclass
		AND dependent.oid <> source.oid AND source_ns.nspname IN (%[1]s)
	UNION ALL
	SELECT 'trigger', 'routine', source_ns.nspname, source.proname,
		'relation', dependent_ns.nspname, dependent.relname, dependent.relkind::text, t.tgname::text
	FROM pg_trigger t
	JOIN pg_proc source ON source.oid = t.tgfoid
	JOIN pg_namespace source_ns ON source_ns.oid = source.pronamespace
	JOIN pg_class dependent ON dependent.oid = t.tgrelid
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
	WHERE source_ns.nspname IN (%[1]s)
	UNION ALL
	SELECT 'column type', CASE WHEN source_row.relkind IS NULL OR source_row.relkind = 'c' THEN 'type' ELSE 'relation' END,
		source_ns.nspname, source.typname,
		'relation', dependent_ns.nspname, dependent.relname, dependent.relkind::text, a.attname::text
	FROM pg_attribute a
	JOIN pg_type column_type ON column_type.oid = a.atttypid
	JOIN pg_type source ON source.oid = column_type.oid OR source.oid = column_type.typelem
	JOIN pg_namespace source_ns ON source_ns.oid = source.typnamespace
	LEFT JOIN pg_class source_row ON source_row.oid = source.typrelid
	JOIN pg_class dependent ON dependent.oid = a.attrelid
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
	WHERE a.attnum > 0 AND NOT a.attisdropped AND source_ns.nspname IN (%[1]s)
	UNION ALL
	SELECT 'signature type', CASE WHEN source_row.relkind IS NULL OR source_row.relkind = 'c' THEN 'type' ELSE 'relation' END,
		source_ns.nspname, source.typname,
		'routine', dependent_ns.nspname, dependent.proname, dependent.prokind::text, ''
	FROM pg_proc dependent
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.pronamespace
	JOIN pg_type signature ON signature.oid = dependent.prorettype OR signature.oid = ANY (dependent.proargtypes)
	JOIN pg_type source ON source.oid = signature.oid OR source.oid = signature.typelem
	JOIN pg_namespace source_ns ON source_ns.oid = source.typnamespace
	LEFT JOIN pg_class source_row ON source_row.oid = source.typrelid
	WHERE source_ns.nspname IN (%[1]s)`

// outsideTextDependentsQuery reads what refuseOutsideCleanupDependents
// matches as text: every column default, computed column, ON UPDATE
// expression, check constraint, view and materialized view definition,
// policy expression, index with an expression or a predicate, trigger WHEN
// condition and routine body outside the system schemas, with the object that
// holds it, and the database name, the sequences and the types of the schemas
// named, which the text spells them by. Each row is what it carries, the
// schema, name and kind of an object, the column, constraint, policy, index or
// trigger that carries it or an OID, and the text.
//
// Measured on CockroachDB v26.4.1, a default reads
// `nextval('public.s'::regclass)` and `public.fn()`, a check `CHECK ((d::public.e = 'a'::public.e))`,
// a policy `public.fn() = 1:::INT8`, an index `CREATE INDEX ix ON other.t USING btree ((public.fn() + id) ASC)`
// and a trigger's tgqual `(public.fn() = 1:::INT8)`, whether or not the name
// was written qualified. A view definition and a routine body read a sequence
// as `nextval(4208:::REGCLASS)` and a type as `@104221`, and a routine body
// reads a relation as `db.public.t`. pg_depend records none of them. An
// expression index keeps its expression in a hidden column too, which
// pg_attrdef lists as dropped, so the index row is read and the column is not.
// An ON UPDATE expression is in no pg_catalog table, and
// information_schema.columns carries it as column_on_update on v26.4.1 and not
// on v24.3.37, so the column is read through the row's JSON, where an absent
// key reads as NULL. pg_get_triggerdef does not exist on v25.4.21, so a
// trigger's condition is read from tgqual.
const outsideTextDependentsQuery = `
	SELECT CASE WHEN a.attgenerated = '' THEN 'default' ELSE 'computed' END,
		dependent_ns.nspname, dependent.relname, dependent.relkind::text, a.attname::text,
		pg_get_expr(ad.adbin, ad.adrelid)
	FROM pg_attrdef ad
	JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
	JOIN pg_class dependent ON dependent.oid = ad.adrelid
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
	WHERE NOT a.attisdropped
		AND dependent_ns.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
	UNION ALL
	SELECT 'view', v.schemaname::text, v.viewname::text, 'v', '', v.definition::text
	FROM pg_views v
	WHERE v.schemaname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
	UNION ALL
	SELECT 'view', m.schemaname::text, m.matviewname::text, 'm', '', m.definition::text
	FROM pg_matviews m
	WHERE m.schemaname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
	UNION ALL
	SELECT 'policy using', p.schemaname::text, p.tablename::text, 'r', p.policyname::text, p.qual::text
	FROM pg_policies p
	WHERE p.qual IS NOT NULL
		AND p.schemaname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
	UNION ALL
	SELECT 'policy check', p.schemaname::text, p.tablename::text, 'r', p.policyname::text, p.with_check::text
	FROM pg_policies p
	WHERE p.with_check IS NOT NULL
		AND p.schemaname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
	UNION ALL
	SELECT 'index', dependent_ns.nspname, dependent.relname, dependent.relkind::text, index_class.relname::text,
		pg_get_indexdef(i.indexrelid)
	FROM pg_index i
	JOIN pg_class index_class ON index_class.oid = i.indexrelid
	JOIN pg_class dependent ON dependent.oid = i.indrelid
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
	WHERE (i.indexprs IS NOT NULL OR i.indpred IS NOT NULL)
		AND dependent_ns.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
	UNION ALL
	SELECT 'trigger', dependent_ns.nspname, dependent.relname, dependent.relkind::text, t.tgname::text,
		t.tgqual::text
	FROM pg_trigger t
	JOIN pg_class dependent ON dependent.oid = t.tgrelid
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
	WHERE t.tgqual IS NOT NULL
		AND dependent_ns.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
	UNION ALL
	SELECT 'check', dependent_ns.nspname, dependent.relname, dependent.relkind::text, con.conname::text,
		pg_get_constraintdef(con.oid)
	FROM pg_constraint con
	JOIN pg_class dependent ON dependent.oid = con.conrelid
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
	WHERE con.contype = 'c'
		AND dependent_ns.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
	UNION ALL
	SELECT 'on update', c.table_schema::text, c.table_name::text, 'r', c.column_name::text,
		to_jsonb(c) ->> 'column_on_update'
	FROM information_schema.columns c
	WHERE to_jsonb(c) ->> 'column_on_update' IS NOT NULL
		AND c.table_schema NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
	UNION ALL
	SELECT 'body', dependent_ns.nspname, dependent.proname, dependent.prokind::text, '', dependent.prosrc
	FROM pg_proc dependent
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.pronamespace
	WHERE dependent_ns.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
		AND dependent.prosrc IS NOT NULL
	UNION ALL
	SELECT 'sequence', n.nspname, c.relname, '', c.oid::text, ''
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind = 'S' AND n.nspname IN (%[1]s)
	UNION ALL
	SELECT 'type', n.nspname, t.typname, '', t.oid::text, ''
	FROM pg_type t
	JOIN pg_namespace n ON n.oid = t.typnamespace
	WHERE n.nspname IN (%[1]s)
	UNION ALL
	SELECT 'database', '', current_database(), '', '', ''`

// cleanupFamily is the name space of a cleanup object: two objects of one
// family cannot share a schema and a name.
type cleanupFamily string

const (
	relationFamily cleanupFamily = "relation"
	routineFamily  cleanupFamily = "routine"
	typeFamily     cleanupFamily = "type"
)

// familyOfCleanupKind answers the family of a cleanup object's kind, and false
// for a kind nothing outside the cleanup can depend on by name, such as a
// constraint or a policy.
func familyOfCleanupKind(kind string) (cleanupFamily, bool) {
	switch kind {
	case "table", "view", "materialized view", "foreign table", "sequence", "index":
		return relationFamily, true
	case "function", "procedure", "aggregate":
		return routineFamily, true
	case "type":
		return typeFamily, true
	default:
		return "", false
	}
}

// cleanupKey names one object of a family.
type cleanupKey struct {
	family       cleanupFamily
	schema, name string
}

// describe spells the object of key for a diagnostic, with kind as its kind.
func (key cleanupKey) describe(kind string) string {
	return fmt.Sprintf("%s %q.%q", kind, key.schema, key.name)
}

// dependentOf returns the key of a dependent the catalog reports as being of
// family in the dependent columns, with kind its pg_class.relkind or
// pg_proc.prokind, and the kind to spell it by. A composite type is a pg_class
// row of kind c, and belongs to the type family a cleanup lists it in.
func dependentOf(family cleanupFamily, schema, name, kind string) (cleanupKey, string) {
	if family == routineFamily {
		return cleanupKey{routineFamily, schema, name}, routineKindName(kind)
	}
	if kind == "c" {
		return cleanupKey{typeFamily, schema, name}, "type"
	}
	return cleanupKey{relationFamily, schema, name}, relationKindName(kind)
}

// routineKindName spells a pg_proc.prokind for a diagnostic.
func routineKindName(kind string) string {
	switch kind {
	case "p":
		return "procedure"
	case "a":
		return "aggregate"
	default:
		return "function"
	}
}

// refuseOutsideCleanupDependents refuses the cleanup of objects, before it
// drops any, when an object outside objects depends on one in them, and names
// each such dependency. It runs where the server commits each drop as it
// runs. A cleanup that lists every object of the schemas it empties, as a dev
// or shadow database cleanup does, finds dependents only in other schemas.
func refuseOutsideCleanupDependents(ctx context.Context, q cleanupConn, objects []postgresCleanupObject) error {
	dropped := make(map[cleanupKey]string)
	var schemas []string
	for _, object := range objects {
		family, ok := familyOfCleanupKind(object.Kind)
		if !ok || object.Qualifier != "" {
			continue
		}
		dropped[cleanupKey{family, object.Schema, object.Name}] = object.Kind
		if !slices.Contains(schemas, object.Schema) {
			schemas = append(schemas, object.Schema)
		}
	}
	if len(schemas) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(schemas))
	args := make([]any, 0, len(schemas))
	for i, schema := range schemas {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, schema)
	}
	in := strings.Join(placeholders, ", ")

	blocked, err := structuredOutsideDependents(ctx, q, in, args, dropped)
	if err != nil {
		return err
	}
	textual, database, err := textOutsideDependents(ctx, q, in, args, dropped)
	if err != nil {
		return err
	}
	blocked = append(blocked, textual...)
	reported, err := serverReportedTypeDependents(ctx, q, dropped, database, blocked)
	if err != nil {
		return err
	}
	blocked = append(blocked, reported...)
	if len(blocked) == 0 {
		return nil
	}
	sentences := make([]string, 0, len(blocked))
	for _, dependency := range blocked {
		sentences = append(sentences, dependency.sentence)
	}
	return fmt.Errorf(
		"an object the cleanup does not drop depends on one it drops: %s; the server commits each drop as it "+
			"runs, so the cleanup refuses before it drops anything",
		strings.Join(sentences, "; "),
	)
}

// outsideDependency is a dependency of dependent, an object the cleanup does
// not drop, on source, one it drops, and the sentence that names it. A
// dependent the server names only by its qualified name has the zero key.
type outsideDependency struct {
	source, dependent cleanupKey
	sentence          string
}

// compareOutsideDependencies orders dependencies by their sentences, so a
// refusal names them in the same order on every run.
func compareOutsideDependencies(a, b outsideDependency) int {
	return strings.Compare(a.sentence, b.sentence)
}

// structuredOutsideDependents reads outsideStructuredDependentsQuery over the
// schemas in, bound by args, and describes each dependency on an object of
// dropped held by an object outside it.
func structuredOutsideDependents(
	ctx context.Context,
	q cleanupConn,
	in string,
	args []any,
	dropped map[cleanupKey]string,
) ([]outsideDependency, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf(outsideStructuredDependentsQuery, in), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to read what depends on the cleanup's objects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var blocked []outsideDependency
	for rows.Next() {
		var how, sourceFamily, sourceSchema, sourceName string
		var dependentFamily, dependentSchema, dependentName, dependentKind, carrier string
		if err := rows.Scan(&how, &sourceFamily, &sourceSchema, &sourceName,
			&dependentFamily, &dependentSchema, &dependentName, &dependentKind, &carrier); err != nil {
			return nil, fmt.Errorf("failed to read what depends on the cleanup's objects: %w", err)
		}
		source := cleanupKey{cleanupFamily(sourceFamily), sourceSchema, sourceName}
		sourceKind, drops := dropped[source]
		dependent, kind := dependentOf(cleanupFamily(dependentFamily), dependentSchema, dependentName, dependentKind)
		if _, alsoDropped := dropped[dependent]; !drops || alsoDropped {
			continue
		}
		blocked = append(blocked, outsideDependency{
			source:    source,
			dependent: dependent,
			sentence: describeOutsideDependency(how, kind, dependent.describe(kind), carrier,
				source.describe(sourceKind)),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read what depends on the cleanup's objects: %w", err)
	}
	slices.SortFunc(blocked, compareOutsideDependencies)
	return blocked, nil
}

// describeOutsideDependency spells one dependency of dependent, an object of
// kind dependentKind, on source, of the kind how names, carried by the
// constraint, trigger or column carrier.
func describeOutsideDependency(how, dependentKind, dependent, carrier, source string) string {
	switch how {
	case "rule":
		if dependentKind == "view" || dependentKind == "materialized view" {
			return fmt.Sprintf("%s reads %s", dependent, source)
		}
		return fmt.Sprintf("%s depends on %s", dependent, source)
	case "foreign key":
		return fmt.Sprintf("foreign key %q on %s references %s", carrier, dependent, source)
	case "call":
		return fmt.Sprintf("%s calls %s", dependent, source)
	case "trigger":
		return fmt.Sprintf("trigger %q on %s runs %s", carrier, dependent, source)
	case "column type":
		return fmt.Sprintf("column %q of %s has %s", carrier, dependent, source)
	default:
		return fmt.Sprintf("%s takes or returns %s", dependent, source)
	}
}

// textOutsideDependents reads outsideTextDependentsQuery over the schemas in,
// bound by args, describes each text outside dropped that names an object of
// dropped, and returns the name of the database the texts qualify names by.
func textOutsideDependents(
	ctx context.Context,
	q cleanupConn,
	in string,
	args []any,
	dropped map[cleanupKey]string,
) ([]outsideDependency, string, error) {
	holders, spellings, err := readOutsideTexts(ctx, q, in, args, dropped)
	if err != nil || len(holders) == 0 {
		return nil, spellings.database, err
	}
	sources := slices.SortedFunc(maps.Keys(dropped), compareCleanupKeys)
	bodyPatterns := make(map[cleanupKey]*regexp.Regexp, len(sources))
	expressionPatterns := make(map[cleanupKey]*regexp.Regexp, len(sources))
	for _, source := range sources {
		bodyPatterns[source] = spellings.bodyPattern(source)
		expressionPatterns[source] = expressionPattern(source)
	}
	var blocked []outsideDependency
	for _, h := range holders {
		for _, source := range sources {
			if spellings.textNames(h, source, bodyPatterns[source], expressionPatterns[source]) {
				blocked = append(blocked, outsideDependency{
					source:    source,
					dependent: h.key,
					sentence:  h.describe(source.describe(dropped[source])),
				})
			}
		}
	}
	slices.SortFunc(blocked, compareOutsideDependencies)
	return blocked, spellings.database, nil
}

// outsideText is a text outside a cleanup that can name an object of it: what
// it is, the object that holds it, the kind to spell that object by, the
// column, constraint, policy, index or trigger that carries it, and its text.
type outsideText struct {
	what       string
	key        cleanupKey
	kind, text string
	carrier    string
}

// describe spells the text naming source, an object the cleanup drops, for a
// diagnostic.
func (t outsideText) describe(source string) string {
	holder := t.key.describe(t.kind)
	switch t.what {
	case "body":
		return fmt.Sprintf("the text of the body of %s names %s", holder, source)
	case "view":
		return fmt.Sprintf("the definition of %s names %s", holder, source)
	case "policy using":
		return fmt.Sprintf("the USING expression of policy %q on %s names %s", t.carrier, holder, source)
	case "policy check":
		return fmt.Sprintf("the WITH CHECK expression of policy %q on %s names %s", t.carrier, holder, source)
	case "index":
		return fmt.Sprintf("index %q on %s names %s", t.carrier, holder, source)
	case "trigger":
		return fmt.Sprintf("the WHEN condition of trigger %q on %s names %s", t.carrier, holder, source)
	case "check":
		return fmt.Sprintf("check constraint %q on %s names %s", t.carrier, holder, source)
	case "computed":
		return fmt.Sprintf("the expression of computed column %q of %s names %s", t.carrier, holder, source)
	case "on update":
		return fmt.Sprintf("the ON UPDATE expression of column %q of %s names %s", t.carrier, holder, source)
	default:
		return fmt.Sprintf("the default of column %q of %s names %s", t.carrier, holder, source)
	}
}

// readOutsideTexts reads outsideTextDependentsQuery over the schemas in, bound
// by args: the texts whose holder dropped does not list, and the spellings
// they name objects by.
func readOutsideTexts(
	ctx context.Context,
	q cleanupConn,
	in string,
	args []any,
	dropped map[cleanupKey]string,
) ([]outsideText, cleanupSpellings, error) {
	spellings := cleanupSpellings{oids: make(map[cleanupKey]string)}
	rows, err := q.QueryContext(ctx, fmt.Sprintf(outsideTextDependentsQuery, in), args...)
	if err != nil {
		return nil, spellings, fmt.Errorf("failed to read the defaults and routine bodies outside the cleanup: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var texts []outsideText
	for rows.Next() {
		var what, schema, name, kind, column, text string
		if err := rows.Scan(&what, &schema, &name, &kind, &column, &text); err != nil {
			return nil, spellings, fmt.Errorf("failed to read the defaults and routine bodies outside the cleanup: %w", err)
		}
		switch what {
		case "database":
			spellings.database = name
		case "sequence":
			spellings.oids[cleanupKey{relationFamily, schema, name}] = column
		case "type":
			spellings.oids[cleanupKey{typeFamily, schema, name}] = column
		default:
			family := relationFamily
			if what == "body" {
				family = routineFamily
			}
			key, spelled := dependentOf(family, schema, name, kind)
			if _, alsoDropped := dropped[key]; alsoDropped {
				continue
			}
			texts = append(texts, outsideText{what: what, key: key, kind: spelled, text: text, carrier: column})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, spellings, fmt.Errorf("failed to read the defaults and routine bodies outside the cleanup: %w", err)
	}
	return texts, spellings, nil
}

// compareCleanupKeys orders keys by family, schema and name, so a refusal
// names its dependencies in the same order on every run.
func compareCleanupKeys(a, b cleanupKey) int {
	return strings.Compare(string(a.family)+"\x00"+a.schema+"\x00"+a.name,
		string(b.family)+"\x00"+b.schema+"\x00"+b.name)
}

// cleanupSpellings holds what CockroachDB spells an object by in a default or
// a routine body besides its name: the database that qualifies a relation,
// and the OID of a sequence or a type.
type cleanupSpellings struct {
	database string
	oids     map[cleanupKey]string
}

// regclassLiteral matches a quoted name cast to regclass, and regclassOID an
// OID cast to it, as an expression or a body spells a sequence.
var (
	regclassLiteral = regexp.MustCompile(`(?i)'((?:[^']|'')*)'\s*::\s*regclass`)
	regclassOID     = regexp.MustCompile(`(?i)\b([0-9]+)\s*:::?\s*regclass`)
)

// textNames reports whether the text of t names source: a routine body by
// body, and an expression by expression.
func (s cleanupSpellings) textNames(t outsideText, source cleanupKey, body, expression *regexp.Regexp) bool {
	if t.what == "body" {
		return s.bodyNames(t.text, source, body)
	}
	return s.expressionNames(t.text, source, expression)
}

// expressionNames reports whether the expression or view definition text
// names source: a relation by its name or OID cast to regclass, a routine by
// pattern, the name expressionPattern returns, and a type by pattern or by its
// OID after an @, as a view definition spells it.
func (s cleanupSpellings) expressionNames(text string, source cleanupKey, pattern *regexp.Regexp) bool {
	switch source.family {
	case relationFamily:
		return s.regclassNames(text, source)
	case typeFamily:
		if oid, ok := s.oids[source]; ok && containsTypeOID(text, oid) {
			return true
		}
	}
	return pattern != nil && pattern.MatchString(text)
}

// expressionPattern returns the pattern an expression names source by, its
// schema-qualified name, followed by an argument list for a routine, or nil for
// a relation, which an expression names only as a regclass.
func expressionPattern(source cleanupKey) *regexp.Regexp {
	end := `(?:$|[^A-Za-z0-9_$"])`
	switch source.family {
	case routineFamily:
		end = `\s*\(`
	case relationFamily:
		return nil
	}
	return regexp.MustCompile(`(?i)(?:^|[^A-Za-z0-9_$"])` + identifierPattern(source.schema) + `\s*\.\s*` +
		identifierPattern(source.name) + end)
}

// regclassNames reports whether text names the relation source, by its name
// cast to regclass or by its OID. A name with no schema matches a relation of
// that name in any schema.
func (s cleanupSpellings) regclassNames(text string, source cleanupKey) bool {
	for _, match := range regclassLiteral.FindAllStringSubmatch(text, -1) {
		parts := splitQualifiedName(strings.ReplaceAll(match[1], "''", "'"))
		switch {
		case len(parts) == 1 && parts[0] == source.name,
			len(parts) >= 2 && parts[len(parts)-2] == source.schema && parts[len(parts)-1] == source.name:
			return true
		}
	}
	return s.namesOID(text, source)
}

// namesOID reports whether text casts the OID of source to regclass.
func (s cleanupSpellings) namesOID(text string, source cleanupKey) bool {
	oid, ok := s.oids[source]
	if !ok {
		return false
	}
	for _, match := range regclassOID.FindAllStringSubmatch(text, -1) {
		if match[1] == oid {
			return true
		}
	}
	return false
}

// bodyPattern returns the pattern a routine body names source by, its name
// qualified with the database and the schema, or nil for a routine, whose
// calls the catalog reports.
func (s cleanupSpellings) bodyPattern(source cleanupKey) *regexp.Regexp {
	if source.family == routineFamily || s.database == "" {
		return nil
	}
	return regexp.MustCompile(`(?i)(?:^|[^A-Za-z0-9_$"])` + identifierPattern(s.database) + `\s*\.\s*` +
		identifierPattern(source.schema) + `\s*\.\s*` + identifierPattern(source.name) + `(?:$|[^A-Za-z0-9_$"])`)
}

// bodyNames reports whether the routine body text names source: by pattern,
// the name bodyPattern returns, and a sequence by its OID cast to regclass
// and a type by its OID after an @.
func (s cleanupSpellings) bodyNames(text string, source cleanupKey, pattern *regexp.Regexp) bool {
	if pattern != nil && pattern.MatchString(text) {
		return true
	}
	switch source.family {
	case relationFamily:
		return s.namesOID(text, source)
	case typeFamily:
		oid, ok := s.oids[source]
		return ok && containsTypeOID(text, oid)
	default:
		return false
	}
}

// containsTypeOID reports whether text names the type of OID oid as a routine
// body or a view definition spells it, an @ before the OID and no digit after
// it.
func containsTypeOID(text, oid string) bool {
	needle := "@" + oid
	for rest := text; ; {
		at := strings.Index(rest, needle)
		if at < 0 {
			return false
		}
		rest = rest[at+len(needle):]
		if rest == "" || rest[0] < '0' || rest[0] > '9' {
			return true
		}
	}
}

// identifierPattern matches name written bare or in double quotes.
func identifierPattern(name string) string {
	quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	return `(?:` + regexp.QuoteMeta(name) + `|` + regexp.QuoteMeta(quoted) + `)`
}

// splitQualifiedName splits a possibly quoted, dot-separated name into its
// parts, unquoting each quoted one.
func splitQualifiedName(name string) []string {
	var parts []string
	var part strings.Builder
	quoted := false
	for i := 0; i < len(name); i++ {
		switch ch := name[i]; {
		case ch == '"' && quoted && i+1 < len(name) && name[i+1] == '"':
			part.WriteByte('"')
			i++
		case ch == '"':
			quoted = !quoted
		case ch == '.' && !quoted:
			parts = append(parts, part.String())
			part.Reset()
		default:
			part.WriteByte(ch)
		}
	}
	return append(parts, part.String())
}

// typeDependentsSavepoint is the savepoint serverReportedTypeDependents asks
// under inside a transaction, so the refusal of one EXPLAIN does not abort the
// transaction the cleanup runs in.
const typeDependentsSavepoint = "ptah_type_dependents"

// serverReportedTypeDependents asks the server, for each type of dropped,
// which objects depend on it, and describes the dependents that are neither
// in dropped nor named for that type by known. The objects are named as the
// server writes them, without the database, because the server names a
// dependent by its qualified name alone and does not quote it.
//
// Measured on CockroachDB v26.4.1 and v25.4.21, EXPLAIN (DDL) DROP TYPE plans
// the drop without running it, and refuses with SQLSTATE 2BP01 and
// `cannot drop type "e" because other objects ([db.public.t db.other.v]) still depend on it`
// while anything depends on the type, the objects in the cleanup among them.
// It runs inside the cleanup's transaction without committing it, and a
// rollback to a savepoint leaves that transaction open.
func serverReportedTypeDependents(
	ctx context.Context,
	q cleanupConn,
	dropped map[cleanupKey]string,
	database string,
	known []outsideDependency,
) ([]outsideDependency, error) {
	var reported []outsideDependency
	for _, source := range slices.SortedFunc(maps.Keys(dropped), compareCleanupKeys) {
		if source.family != typeFamily {
			continue
		}
		list, err := typeDependentsList(ctx, q, source)
		if err != nil {
			return nil, err
		}
		inside := slices.Collect(maps.Keys(dropped))
		for _, dependency := range known {
			if dependency.source == source {
				inside = append(inside, dependency.dependent)
			}
		}
		if rest := typeDependentsOutside(list, database, inside); rest != "" {
			reported = append(reported, outsideDependency{
				source: source,
				sentence: fmt.Sprintf("CockroachDB records that %s is used by %s",
					source.describe(dropped[source]), rest),
			})
		}
	}
	return reported, nil
}

// typeDependentsList answers the objects the server lists as depending on the
// type source, as it writes the list, or "" when it lists none.
func typeDependentsList(ctx context.Context, q cleanupConn, source cleanupKey) (string, error) {
	explain := "EXPLAIN (DDL) DROP TYPE " + quoteIdent(source.schema) + "." + quoteIdent(source.name)
	err := explainInSavepoint(ctx, q, explain)
	if err == nil {
		return "", nil
	}
	serverErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || serverErr.Code != dependentObjectsStillExist {
		return "", fmt.Errorf("failed to ask the server what depends on %s: %w", source.describe("type"), err)
	}
	_, after, found := strings.Cut(serverErr.Message, "([")
	list, _, closed := strings.Cut(after, "])")
	if !found || !closed {
		return "", fmt.Errorf("failed to read what depends on %s from the server's refusal: %w",
			source.describe("type"), err)
	}
	return list, nil
}

// explainInSavepoint runs explain, and inside a transaction runs it under
// typeDependentsSavepoint and rolls back to it when the server refuses, so
// the transaction stays open.
func explainInSavepoint(ctx context.Context, q cleanupConn, explain string) error {
	if _, inTransaction := q.(*sql.Tx); !inTransaction {
		_, err := q.ExecContext(ctx, explain)
		return err
	}
	if _, err := q.ExecContext(ctx, "SAVEPOINT "+typeDependentsSavepoint); err != nil {
		return fmt.Errorf("failed to open a savepoint to ask what depends on a type: %w", err)
	}
	_, explainErr := q.ExecContext(ctx, explain)
	finish := "RELEASE SAVEPOINT " + typeDependentsSavepoint
	if explainErr != nil {
		finish = "ROLLBACK TO SAVEPOINT " + typeDependentsSavepoint
	}
	if _, err := q.ExecContext(ctx, finish); err != nil {
		return fmt.Errorf("failed to close the savepoint after asking what depends on a type: %w", err)
	}
	return explainErr
}

// typeDependentsOutside removes from list, the objects the server names as
// db.schema.name separated by a space or a comma and a space, every object of
// inside, and returns the rest without the database prefix, or "".
func typeDependentsOutside(list, database string, inside []cleanupKey) string {
	rest := " " + strings.ReplaceAll(list, ", ", " ") + " "
	for _, key := range inside {
		token := " " + database + "." + key.schema + "." + key.name + " "
		for strings.Contains(rest, token) {
			rest = strings.Replace(rest, token, " ", 1)
		}
	}
	rest = strings.ReplaceAll(rest, " "+database+".", " ")
	return strings.Join(strings.Fields(rest), " ")
}
