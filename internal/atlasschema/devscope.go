package atlasschema

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/sqlreach"
)

// DevScopeError reports that a statement about to be rehearsed on the dev
// database names a schema the dev database does not own, on a dialect where a
// schema *is* a database. Executing it would modify that other database no
// matter which connection issues it, so the rehearsal is refused instead.
type DevScopeError struct {
	// StatementIndex is the 1-based position of the offending statement.
	StatementIndex int
	// Schema is the schema name the statement carries.
	Schema string
	// DevSchema is the database the dev connection is bound to.
	DevSchema string
	// Dialect is the normalized dialect whose schema names a database.
	Dialect string
}

func (e *DevScopeError) Error() string {
	return fmt.Sprintf(
		"dev database simulation refused: statement %d names schema %q, but the dev database is %q. "+
			"On %s a schema is a database, so that statement would modify %q no matter which connection "+
			"issues it, and a simulation must not touch anything but the dev database",
		e.StatementIndex, e.Schema, e.DevSchema, e.Dialect, e.Schema)
}

// IsDevScopeEscape reports whether err wraps a dev database scope refusal.
func IsDevScopeEscape(err error) bool {
	var target *DevScopeError
	return errors.As(err, &target)
}

// schemaScopeNamesDatabase reports whether a connection's schema scope names a
// whole database rather than a namespace inside the connected one: whether the
// dialect's descriptor states SchemaIsDatabase.
//
// This is the difference that makes stokaro/ptah#1240 a MySQL defect. Measured
// on 2026-08-07 against a live MySQL 9.7 and a live PostgreSQL 17, one apply
// each from a freshly created target/dev pair:
//
//   - MySQL: the plan reads CREATE TABLE `target`.`users`; rehearsing it on the
//     dev connection created `users` in the TARGET database and left the dev
//     database empty.
//   - PostgreSQL: the plan reads CREATE TABLE "public"."users"; rehearsing it on
//     the dev connection created `public.users` in the DEV database. The same
//     qualified-plan-against-dev shape is present, and it is contained, because
//     a PostgreSQL schema cannot name anything outside the connected database.
func schemaScopeNamesDatabase(dialect string) bool {
	return described(dialect).SchemaIsDatabase
}

// rescopeStatementsForDevDatabase rewrites an apply plan so it rehearses inside
// the dev database, and refuses it when that cannot be guaranteed.
//
// On dialects where a schema is a namespace inside the connected database the
// statements are returned untouched: the connection already contains them.
// Where a schema is a database, every schema name the statements carry is the
// target's, and executing them unchanged would modify the target. Each such
// name is rewritten to the dev database's own name, and any name that is left
// naming neither the dev database (a cross-database reference, or a spelling
// this rewrite does not recognize) refuses the rehearsal rather than running it
// somewhere unknown.
func rescopeStatementsForDevDatabase(statements []string, dialect, targetSchema, devSchema string) ([]string, error) {
	if !schemaScopeNamesDatabase(dialect) {
		return statements, nil
	}
	dev := strings.TrimSpace(devSchema)
	if dev == "" {
		return nil, errors.New(
			"--dev-url must name a database: the apply plan carries the target's schema name, and on this dialect a " +
				"schema is a database, so rehearsing the plan needs the dev database's own name to put in its place")
	}
	target := strings.TrimSpace(targetSchema)
	rescoped := make([]string, len(statements))
	for i, statement := range statements {
		rewritten := rewriteSchemaNames(statement, dialect, target, dev)
		if err := checkStatementScopedToDev(rewritten, i+1, dialect, dev); err != nil {
			return nil, err
		}
		rescoped[i] = rewritten
	}
	return rescoped, nil
}

// scopeMaterializationToDev keeps the creation of a document on the dev
// database inside it.
//
// Where a schema is a database, a document that names its tables with a
// database -- an HCL document has to, a SQL file may -- renders statements
// that create them in that database, whichever connection runs them. Run as
// written on a dev database of the same server, the materialization of a
// document naming `app` wrote its tables into the target database `app`:
// measured on MySQL 8.4 and MariaDB 11.4, `schema diff --from <file> --to
// mysql://…/app --dev-url mysql://…/appdev` left `app`.`t9` in the target.
//
// A document that names one database is created in the dev database instead,
// that name rewritten to the dev database's own, which is how the pinned
// community binary v1.3.0 creates a one-schema HCL document there. A document
// naming two databases, or a statement that reaches any other, is refused
// before anything runs: there is no one database to put it in. That binary
// runs a SQL file as written and writes into the database it names; that is
// the defect this refusal exists for, and it is not copied.
//
// A whole dev server takes the document database by database, as written, and
// a dialect whose schema is a namespace inside the connected database already
// contains every statement, so both are returned untouched.
func scopeMaterializationToDev(statements []string, info catalog.ServerInfo) ([]string, error) {
	if info.WholeServer || !schemaScopeNamesDatabase(info.Dialect) {
		return statements, nil
	}
	return rescopeStatementsForDevDatabase(statements, info.Dialect, soleSchemaName(statements, info.Dialect), info.Schema)
}

// soleSchemaName returns the one schema the statements name, and "" when they
// name none or more than one.
func soleSchemaName(statements []string, dialect string) string {
	sole := ""
	for _, statement := range statements {
		tokens := sqlreach.SignificantTokens(statement, sqlreach.DialectUsesBackslashEscapes(dialect), dialect)
		for _, position := range schemaNamePositions(tokens) {
			name := unquoteSchemaName(tokens[position])
			switch {
			case sole == "":
				sole = name
			case name != sole:
				return ""
			}
		}
	}
	return sole
}

// rewriteSchemaNames replaces every schema name spelled `from` with `to`,
// quoted for dialect. Only bare and dialect-quoted identifiers are rewritten;
// anything else is left for checkStatementScopedToDev to refuse.
func rewriteSchemaNames(statement, dialect, from, to string) string {
	if from == "" || from == to {
		return statement
	}
	tokens := sqlreach.SignificantTokens(statement, sqlreach.DialectUsesBackslashEscapes(dialect), dialect)
	positions := schemaNamePositions(tokens)
	replacement := sqlident.Quote(dialect, to)
	// Splice from the end so earlier offsets stay valid.
	for _, position := range slices.Backward(positions) {
		token := tokens[position]
		if token.Type != lexer.TokenIdentifier || unquoteSchemaName(token) != from {
			continue
		}
		statement = statement[:token.Start] + replacement + statement[token.End:]
	}
	return statement
}

// checkStatementScopedToDev refuses a statement that still names a schema other
// than the dev database's own. It scans under both string-escape
// interpretations, like [CheckPlanStatementsSandboxable], so a statement whose
// tokenization depends on the escape mode cannot hide a name under the one that
// was not checked.
func checkStatementScopedToDev(statement string, index int, dialect, devSchema string) error {
	for _, backslashEscapes := range []bool{false, true} {
		tokens := sqlreach.SignificantTokens(statement, backslashEscapes, dialect)
		for _, position := range schemaNamePositions(tokens) {
			name := unquoteSchemaName(tokens[position])
			if name == devSchema || (readsCatalog(dialect, tokens) && isCatalogSchema(name)) {
				continue
			}
			return &DevScopeError{
				StatementIndex: index,
				Schema:         name,
				DevSchema:      devSchema,
				Dialect:        platform.NormalizeDialect(dialect),
			}
		}
	}
	return nil
}

// readsCatalog reports whether a statement may name a server's own catalog
// without leaving the dev database: the dialect's descriptor declares table
// rebuilds, whose plan reads the catalog before it drops a table, and every
// statement in the text is a SELECT.
//
// A SELECT changes no database. The plan hands one statement to each entry, but
// the check looks at every statement of the text, so a SELECT followed by a
// DROP TABLE after a semicolon does not pass on its first token.
func readsCatalog(dialect string, tokens []lexer.Token) bool {
	described, ok := platform.Lookup(dialect)
	if !ok || !described.TableRebuild {
		return false
	}
	start := 0
	for i := 0; i <= len(tokens); i++ {
		if i < len(tokens) && tokens[i].Type != lexer.TokenSemicolon {
			continue
		}
		if i > start && !sqlreach.IsKeyword(tokens[start], "SELECT") {
			return false
		}
		start = i + 1
	}
	return len(tokens) > 0
}

// isCatalogSchema reports whether a schema name is a server's own catalog, which
// a SELECT reads on the dev connection as it would on the target's. A table
// rebuild asks the catalog, system.tables on ClickHouse, whether a view reads
// the table before it drops it.
func isCatalogSchema(name string) bool {
	return strings.EqualFold(name, "system") || strings.EqualFold(name, "information_schema")
}

// schemaNamePositions returns the token indexes that name a schema:
//
//   - a qualifier, the head of a dotted object reference (`db`.`users`);
//   - the operand of USE, and of CREATE/DROP/ALTER SCHEMA|DATABASE, which name
//     a database directly rather than through a dot.
//
// The second form is restricted to the statement's leading keywords so that an
// ordinary column named `database` — legal and unreserved in MySQL — is not
// mistaken for one. Inside a view or trigger body, a name the body binds in
// front of a column -- an alias, a table it reads, a trigger's NEW or OLD -- is
// not a qualifier either; see [columnQualifiers].
func schemaNamePositions(tokens []lexer.Token) []int {
	var positions []int
	columns := columnQualifiers(tokens)
	for i := range tokens {
		if !isSchemaNameToken(tokens[i]) || columns[i] {
			continue
		}
		if i+1 >= len(tokens) || !tokens[i+1].MatchOperatorValue(".") {
			continue
		}
		// The tail of a longer dotted path names an object, not a schema.
		if i > 0 && tokens[i-1].MatchOperatorValue(".") {
			continue
		}
		positions = append(positions, i)
	}
	if operand, ok := schemaOperandPosition(tokens); ok {
		positions = append(positions, operand)
	}
	slices.Sort(positions)
	return slices.Compact(positions)
}

// schemaOperandPosition finds the database named directly by a statement's
// leading keywords: `USE <db>` and `CREATE|DROP|ALTER SCHEMA|DATABASE [IF [NOT]
// EXISTS] <db>`. USE matters most: a plan that switches the session's database
// would silently move every later statement out of the dev database.
func schemaOperandPosition(tokens []lexer.Token) (int, bool) {
	if len(tokens) >= 2 && sqlreach.IsKeyword(tokens[0], "USE") && isSchemaNameToken(tokens[1]) {
		return 1, true
	}
	if len(tokens) < 3 {
		return 0, false
	}
	if !sqlreach.IsKeyword(tokens[0], "CREATE") && !sqlreach.IsKeyword(tokens[0], "DROP") && !sqlreach.IsKeyword(tokens[0], "ALTER") {
		return 0, false
	}
	if !sqlreach.IsKeyword(tokens[1], "SCHEMA") && !sqlreach.IsKeyword(tokens[1], "DATABASE") {
		return 0, false
	}
	position := 2
	for position < len(tokens) && (sqlreach.IsKeyword(tokens[position], "IF") ||
		sqlreach.IsKeyword(tokens[position], "NOT") || sqlreach.IsKeyword(tokens[position], "EXISTS")) {
		position++
	}
	if position >= len(tokens) || !isSchemaNameToken(tokens[position]) {
		return 0, false
	}
	return position, true
}

// isSchemaNameToken reports whether a token can name a schema. Double-quoted
// names arrive from the lexer as strings, and they are accepted here so a
// quoted foreign schema name is refused rather than silently unseen; only bare
// and dialect-quoted identifiers are ever rewritten.
func isSchemaNameToken(token lexer.Token) bool {
	switch token.Type {
	case lexer.TokenIdentifier:
		return true
	case lexer.TokenString:
		return strings.HasPrefix(token.Value, `"`)
	default:
		return false
	}
}

// unquoteSchemaName returns the schema name a token spells, undoubling the
// escaped delimiter the SQL standard uses inside a quoted identifier.
func unquoteSchemaName(token lexer.Token) string {
	value := token.Value
	for _, quote := range []string{"`", `"`, "[]"} {
		open, closing := quote[:1], quote[len(quote)-1:]
		if !strings.HasPrefix(value, open) || !strings.HasSuffix(value, closing) || len(value) < 2 {
			continue
		}
		inner := value[1 : len(value)-1]
		if open == closing {
			inner = strings.ReplaceAll(inner, open+open, open)
		}
		return inner
	}
	return value
}

// RebindDatabase is state, a schema read through source, with the objects a
// one-database read names by source's database moved to destination's, where
// a schema is a database and the two connections select databases of
// different names. A dev database rehearses a target's schema with it, and the
// drift check compares a shadow database's replay with the target with it.
//
// A read of one database records that database on each view, on each
// ClickHouse materialized view, on each MySQL or MariaDB routine and MariaDB
// sequence, and on each grant, where a table is left unqualified, and the
// server qualifies the relations of a stored view body with it. Compared as
// read, the same view in a shadow database `app_shadow` and in the target
// `app` is a missing view and an extra view, and recreated as read on a dev
// database `appdev` the baseline wrote `app`.`v`, which the baseline guard
// refused as a write to another database. Each of these objects in source's
// database is left unqualified, as its tables are, and a view body reads
// destination's tables instead. A view body that also reads another database
// keeps that name, and an object in another database keeps its qualifier.
//
// A grant on a table, a view or a sequence names its object by the database
// and the object, and is left unqualified the same way: `GRANT SELECT ON t`
// replayed in `app_shadow` reads as a grant on `app_shadow`.`t`, and it is the
// target's grant on `app`.`t`. A grant on the whole database names that
// database as its object, so it is moved to destination's database, which a
// grant cannot leave unnamed: `GRANT SELECT ON *` replayed there reads as a
// grant on `app_shadow`.*, and it is the target's grant on `app`.*.
//
// A routine body is not rewritten: it is procedural text, and a name in it
// that reads source's database is left as written. On a dev database of the
// target's server the dev cleanup refuses first, since another database
// holds a routine; on a dev server of its own the routine is created as
// written, and a statement that calls it fails there as it would on any
// server without that database. state is not modified.
func RebindDatabase(state *catalog.Database, source, destination catalog.ServerInfo) *catalog.Database {
	from, to := strings.TrimSpace(source.Schema), strings.TrimSpace(destination.Schema)
	if state == nil || !schemaScopeNamesDatabase(destination.Dialect) || from == "" || to == "" || from == to {
		return state
	}
	moved := *state
	moved.Views = slices.Clone(state.Views)
	for i := range moved.Views {
		if moved.Views[i].Schema == from {
			moved.Views[i].Schema = ""
			moved.Views[i].Body = viewBodyInDev(moved.Views[i].Body, destination.Dialect, from, to)
		}
	}
	moved.MatViews = slices.Clone(state.MatViews)
	for i := range moved.MatViews {
		if moved.MatViews[i].Schema == from {
			moved.MatViews[i].Schema = ""
			moved.MatViews[i].Body = viewBodyInDev(moved.MatViews[i].Body, destination.Dialect, from, to)
		}
	}
	moved.Functions = slices.Clone(state.Functions)
	for i := range moved.Functions {
		if moved.Functions[i].Schema == from {
			moved.Functions[i].Schema = ""
		}
	}
	moved.Sequences = slices.Clone(state.Sequences)
	for i := range moved.Sequences {
		if moved.Sequences[i].Schema == from {
			moved.Sequences[i].Schema = ""
		}
	}
	moved.Grants = slices.Clone(state.Grants)
	for i := range moved.Grants {
		grant := &moved.Grants[i]
		switch {
		case strings.EqualFold(grant.ObjectType, "SCHEMA"):
			if grant.ObjectName == from {
				grant.ObjectName = to
			}
		case grant.Schema == from:
			grant.Schema = ""
		}
	}
	return &moved
}

// viewBodyHead is the header a view body is rewritten under: the rewrite
// tells a qualifier from a name the body binds, an alias or a table it reads,
// only inside a statement whose body it can find.
const viewBodyHead = "CREATE VIEW v AS "

// viewBodyInDev is body, a stored view body, with each qualifier naming the
// database from rewritten to name to. A name the body binds is kept, so
// `SELECT app.id FROM app.t1 app` in database app keeps its alias.
func viewBodyInDev(body, dialect, from, to string) string {
	return strings.TrimPrefix(rewriteSchemaNames(viewBodyHead+body, dialect, from, to), viewBodyHead)
}
