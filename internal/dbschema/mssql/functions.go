package mssql

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlroutine"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// readFunctions reads routine signatures and options from the catalog. Bodies
// and explicit INLINE settings come from the stored definition. A routine
// whose options cannot be preserved is refused before comparison.
//
// SQL Server records parameter defaults in sys.parameters for CLR routines
// only. The renderer refuses defaults in T-SQL routines because a declaration
// with one cannot converge against that catalog signature.
func (r *Reader) readFunctions(ctx context.Context) ([]catalog.Function, error) {
	parameters, returns, err := r.readRoutineSignatures(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT s.name, o.name, o.type, ISNULL(m.definition, ''),
			   CASE WHEN m.definition IS NULL THEN 1 ELSE 0 END AS encrypted,
			   ISNULL(CONVERT(varchar(30), m.execute_as_principal_id), 'CALLER') AS execute_as,
			   CASE WHEN m.execute_as_principal_id > 0 THEN ISNULL(USER_NAME(m.execute_as_principal_id), '') ELSE '' END AS principal,
			   m.is_schema_bound, m.is_recompiled, m.null_on_null_input,
			   m.uses_native_compilation, ISNULL(m.inline_type, 0), ISNULL(m.is_inlineable, 0)
		FROM sys.objects AS o
		JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		JOIN sys.sql_modules AS m ON m.object_id = o.object_id
		WHERE o.type IN ('FN', 'IF', 'TF', 'P') AND o.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, o.name`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []catalog.Function
	for rows.Next() {
		var row routineOptionsRow
		var schema, name, objectType, definition string
		if err := rows.Scan(&schema, &name, &objectType, &definition, &row.encrypted, &row.executeAs, &row.principal,
			&row.schemaBound, &row.recompiled, &row.nullOnNullInput, &row.nativelyCompiled, &row.inlined, &row.inlineable); err != nil {
			return nil, err
		}
		word := "function"
		if isProcedureObjectType(objectType) {
			word = schemamodel.FunctionKindProcedure
		}
		row.inlineOption = routineInlineOption(definition, objectType)
		facets, err := row.facets(word, schema+"."+name)
		if err != nil {
			return nil, err
		}
		key := routineKey{schema: schema, name: name}
		functions = append(functions, catalog.Function{
			Facets:     facets,
			Name:       name,
			Kind:       routineKind(objectType),
			Schema:     r.outputSchema(schema),
			Parameters: parameters[key],
			// A procedure returns nothing, and sys.parameters reports no
			// return row for one, so asking functionReturns would produce the
			// empty string by accident rather than by decision.
			Returns: procedureAwareReturns(objectType, returns[key]),
			// The language is the engine's own. T-SQL has no LANGUAGE clause --
			// `LANGUAGE SQL` is `Incorrect syntax near 'LANGUAGE'` -- so there
			// is nothing to read, and reporting the owner's language is what
			// lets a declaration that says the same, or nothing, compare equal.
			Language: mssqlroutine.Language,
			// The security mode is read rather than assumed, because the
			// catalog carries it and a default written in here would be a
			// claim about somebody else's function. execute_as_principal_id is
			// NULL for EXECUTE AS CALLER, -2 for OWNER and the principal's id
			// for a named one, which the facets above carry. T-SQL has no
			// volatility clause; see mssqlroutine.Volatility.
			Security:   functionSecurity(row.executeAs),
			Volatility: mssqlroutine.Volatility,
			Body:       routineBody(objectType, definition),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return functions, nil
}

// routineKey identifies a routine by the pair the catalog reports, because a
// name alone is not unique across schemas.
type routineKey struct{ schema, name string }

// readRoutineSignatures builds the parameter list and the return type of every
// function, from the catalog rather than from the statement text.
func (r *Reader) readRoutineSignatures(ctx context.Context) (parameters, returns map[routineKey]string, err error) {
	query := `
		SELECT s.name, o.name, p.parameter_id, ISNULL(p.name, ''), t.name,
			   p.max_length, p.precision, p.scale
		FROM sys.objects AS o
		JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		JOIN sys.parameters AS p ON p.object_id = o.object_id
		JOIN sys.types AS t ON t.user_type_id = p.user_type_id
		WHERE o.type IN ('FN', 'IF', 'TF', 'P') AND o.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, o.name, p.parameter_id`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	arguments := make(map[routineKey][]string)
	returns = make(map[routineKey]string)
	for rows.Next() {
		var schema, name, parameterName, typeName string
		var parameterID int
		var maxLength, precision, scale int
		if err := rows.Scan(&schema, &name, &parameterID, &parameterName, &typeName,
			&maxLength, &precision, &scale); err != nil {
			return nil, nil, err
		}
		key := routineKey{schema: schema, name: name}
		rendered := routineTypeName(typeName, maxLength, precision, scale)
		// Ordinal zero is the return value, and it carries no name.
		if parameterID == 0 {
			returns[key] = rendered
			continue
		}
		arguments[key] = append(arguments[key], parameterName+" "+rendered)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	parameters = make(map[routineKey]string, len(arguments))
	for key, list := range arguments {
		parameters[key] = strings.Join(list, ", ")
	}
	return parameters, returns, nil
}

// routineTypeName spells a parameter's type the way a declaration writes it.
//
// The catalog reports the base type name beside its facets, so the length and
// the precision have to be put back. max_length is in BYTES, which is why an
// nvarchar's length is halved: `nvarchar(50)` is stored as max_length 100, and
// rendering it as nvarchar(100) would compare unequal to the declaration that
// created it. A max_length of -1 is the MAX form.
func routineTypeName(typeName string, maxLength, precision, scale int) string {
	switch strings.ToLower(typeName) {
	case "varchar", "char", "varbinary", "binary":
		if maxLength < 0 {
			return typeName + "(max)"
		}
		return fmt.Sprintf("%s(%d)", typeName, maxLength)
	case "nvarchar", "nchar":
		if maxLength < 0 {
			return typeName + "(max)"
		}
		return fmt.Sprintf("%s(%d)", typeName, maxLength/2)
	case "decimal", "numeric":
		return fmt.Sprintf("%s(%d,%d)", typeName, precision, scale)
	default:
		return typeName
	}
}

// functionBody takes the body out of the statement text the catalog stored.
//
// The header is `CREATE ... (params) RETURNS <type> [WITH ...] AS <body>`, so
// the body starts at the FIRST `AS` that stands at depth zero after the RETURNS
// keyword. Taking the last one instead looks equivalent on a scalar function
// and is wrong on a table-valued one: `RETURNS TABLE ... AS RETURN SELECT 1 AS
// ok` has a second depth-zero `AS` inside the body, and the last-match rule
// returns `ok WHERE ...` as the whole function. A parameter's own `AS` cannot
// interfere because parameters live inside the parentheses.
//
// This is exact for a function Ptah wrote, because the definition the catalog
// hands back is Ptah's own rendering. For a function written by someone else it
// is best-effort, and the comparator's answer for one is a body difference
// rather than a wrong statement.
func functionBody(definition string) string {
	upper := strings.ToUpper(definition)
	returns := -1
	depth := 0
	for i := 0; i < len(definition); i++ {
		if end := commentEnd(definition, i); end >= 0 {
			i = end
			continue
		}
		switch definition[i] {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case '\'':
			if next := strings.IndexByte(definition[i+1:], '\''); next >= 0 {
				i += next + 1
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if returns < 0 {
			if i+7 <= len(upper) && upper[i:i+7] == "RETURNS" && standaloneWord(definition, i, i+7) {
				returns = i
			}
			continue
		}
		// A function may omit AS before BEGIN or RETURN.
		for _, keyword := range []string{"BEGIN", "RETURN"} {
			if strings.HasPrefix(upper[i:], keyword) && standaloneWord(definition, i, i+len(keyword)) {
				return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition[i:]), ";"))
			}
		}
		if i+2 > len(upper) || upper[i:i+2] != "AS" || !standaloneWord(definition, i, i+2) {
			continue
		}
		// `RETURNS int WITH EXECUTE AS OWNER AS BEGIN ... END` keeps both
		// words in the definition, as a procedure's does.
		if precededByExecute(upper, i) {
			continue
		}
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition[i+2:]), ";"))
	}
	return strings.TrimSpace(definition)
}

// commentEnd reports the index of the last byte of the comment beginning at i,
// or -1 when the text at i does not begin one.
//
// A scanner looking for a keyword has to know about comments, because a comment
// may hold anything -- keywords included. Measured on SQL Server 2025, a view
// whose header carries one:
//
//	CREATE VIEW dbo.v /* the header AS lives here */ AS SELECT id AS ident FROM dbo.orders
//
// The word AS inside it is a standalone word by every test [standaloneWord]
// applies, so a blind scan cut the definition there and read the body as
// `lives here */ AS SELECT id AS ident FROM dbo.orders`. Replayed, the server
// answered `Incorrect syntax near 'lives'`.
//
// It is consulted before the bracket depth as well as before the keyword, which
// is the other half: a lone `[` or `(` inside a comment would otherwise leave
// the scanner at a depth it never returns from, and every keyword after it is
// skipped.
//
// Both kinds are here because T-SQL has both, and the block form NESTS: the
// first `*/` does not necessarily close it (stokaro/ptah#2115).
func commentEnd(definition string, i int) int {
	if i+1 >= len(definition) {
		return -1
	}
	switch {
	case definition[i] == '-' && definition[i+1] == '-':
		if next := strings.IndexByte(definition[i+2:], '\n'); next >= 0 {
			return i + 2 + next
		}
		return len(definition) - 1
	case definition[i] == '/' && definition[i+1] == '*':
		depth := 1
		for j := i + 2; j+1 < len(definition); j++ {
			switch {
			case definition[j] == '/' && definition[j+1] == '*':
				depth++
				j++
			case definition[j] == '*' && definition[j+1] == '/':
				depth--
				j++
				if depth == 0 {
					return j
				}
			}
		}
		// An unterminated comment runs to the end, which is what the server
		// would do with it too.
		return len(definition) - 1
	}
	return -1
}

// viewBody extracts a view's SELECT from the statement text sys.sql_modules
// keeps.
//
// SQL Server stores the object's whole creation text, unlike PostgreSQL, whose
// pg_get_viewdef returns the body alone. Storing the statement where the body
// belongs made the renderer write it inside another `CREATE VIEW ... AS`, and
// the server refused the pair with `'CREATE VIEW' must be the first statement
// in a query batch` -- so a document describing any SQL Server view could not
// be applied (stokaro/ptah#2115).
//
// The body begins after the FIRST `AS` outside brackets and quotes, which is
// the opposite of the rule [functionBody] needs: a view's own body is full of
// column aliases spelled `AS`, and the last one would cut the statement in
// half. Everything before it -- the name, an optional column list, and a
// `WITH SCHEMABINDING` or `WITH VIEW_METADATA` clause -- is the header.
func viewBody(definition string) string {
	start := viewHeaderAS(definition)
	if start < 0 {
		// A definition with no header-level AS is not one this reader can
		// split, so it is carried whole rather than cut at a guess.
		return strings.TrimSpace(definition)
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition[start+2:]), ";"))
}

// viewHeaderAS reports where the header's AS keyword begins, or -1 when the
// definition has none.
//
// One scan rather than two. [viewBody] takes what follows it and
// [viewAttributes] takes what precedes it, and a header split at two different
// places by two different walks is a defect neither of them can be tested for
// on its own.
func viewHeaderAS(definition string) int {
	upper := strings.ToUpper(definition)
	depth := 0
	for i := 0; i < len(definition); i++ {
		if end := commentEnd(definition, i); end >= 0 {
			i = end
			continue
		}
		switch definition[i] {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case '\'':
			if next := strings.IndexByte(definition[i+1:], '\''); next >= 0 {
				i += next + 1
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if i+2 > len(upper) || upper[i:i+2] != "AS" || !standaloneWord(definition, i, i+2) {
			continue
		}
		return i
	}
	return -1
}

// viewAttributes reads the WITH clause a view's header carries.
//
// SQL Server writes `CREATE VIEW name [(columns)] [WITH attribute [,...]] AS`,
// and the attributes are part of what the view IS: SCHEMABINDING binds it to
// the tables it names, so they cannot be altered under it, and an indexed view
// requires it. [viewBody] cuts the header off and nothing kept it, so a
// replayed view was not bound. Measured on SQL Server 2025, source against the
// replay of Ptah's own description:
//
//	v_bound   IsSchemaBound=1   ->   v_bound   IsSchemaBound=0
//
// with `schema apply --dry-run` reporting `Schema is synced` and the replay
// reporting success, so nothing anywhere said the guarantee had been dropped
// (stokaro/ptah#2125).
//
// A view created WITH ENCRYPTION cannot reach this: OBJECT_DEFINITION answers
// NULL for one, so the reader never sees a definition to take a header from.
func viewAttributes(definition string) []string {
	end := viewHeaderAS(definition)
	if end < 0 {
		return nil
	}
	start := headerWith(definition[:end])
	if start < 0 {
		return nil
	}
	var attributes []string
	for _, attribute := range splitTopLevel(definition[start:end]) {
		if trimmed := strings.ToUpper(strings.TrimSpace(attribute)); trimmed != "" {
			attributes = append(attributes, trimmed)
		}
	}
	return attributes
}

// headerWith reports where the attribute list begins -- just past the header's
// WITH -- or -1 when the header has none.
func headerWith(header string) int {
	upper := strings.ToUpper(header)
	depth := 0
	for i := 0; i < len(header); i++ {
		if end := commentEnd(header, i); end >= 0 {
			i = end
			continue
		}
		switch header[i] {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case '\'':
			if next := strings.IndexByte(header[i+1:], '\''); next >= 0 {
				i += next + 1
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if i+4 > len(upper) || upper[i:i+4] != "WITH" || !standaloneWord(header, i, i+4) {
			continue
		}
		return i + 4
	}
	return -1
}

// splitTopLevel cuts a comma-separated list, ignoring commas inside brackets,
// quotes and comments.
func splitTopLevel(list string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(list); i++ {
		if end := commentEnd(list, i); end >= 0 {
			i = end
			continue
		}
		switch list[i] {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case '\'':
			if next := strings.IndexByte(list[i+1:], '\''); next >= 0 {
				i += next + 1
			}
		case ',':
			if depth == 0 {
				parts = append(parts, list[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, list[start:])
}

// standaloneWord reports whether the range is a whole word rather than part of
// a longer identifier.
func standaloneWord(value string, start, end int) bool {
	if start > 0 && isWordByte(value[start-1]) {
		return false
	}
	if end < len(value) && isWordByte(value[end]) {
		return false
	}
	return true
}

func isWordByte(b byte) bool {
	return b == '_' || b == '@' || b == '$' ||
		(b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

// routineOptionsRow is what sys.sql_modules reports of a routine's header.
type routineOptionsRow struct {
	encrypted               int
	executeAs, principal    string
	schemaBound, recompiled bool
	nullOnNullInput         bool
	nativelyCompiled        bool
	inlined, inlineable     bool
	inlineOption            string
}

// facets returns the SQL Server owner's routine options facet of a routine,
// and none for one whose header states no option the facet holds. Measured on
// SQL Server 2025 RTM-CU9, execute_as_principal_id is the principal's own id
// for EXECUTE AS 'name', and for EXECUTE AS SELF the id of the principal that
// created the routine, which is reported by that name; is_schema_bound,
// is_recompiled and null_on_null_input report SCHEMABINDING, RECOMPILE and
// RETURNS NULL ON NULL INPUT.
//
// An option the facet cannot hold is refused by name rather than read as
// absent, since a routine read without it would be replaced without it:
// ENCRYPTION, which leaves sys.sql_modules no definition, NATIVE_COMPILATION,
// and an explicit INLINE setting. The definition names INLINE even when
// the function cannot be inlined and the catalog flags are both zero. A routine that runs as a principal the read cannot name is refused
// too, since reporting it as one that runs as its owner would hide a
// principal change.
func (row routineOptionsRow) facets(kind, routine string) (schemaext.Facets, error) {
	refuse := func(option, why string) error {
		return fmt.Errorf("%w: sqlserver %s %s is created WITH %s, %s; Ptah can neither compare nor replace it",
			ptaherr.ErrUnsupportedFeature, kind, routine, option, why)
	}
	switch {
	case row.encrypted == 1:
		return schemaext.Facets{}, refuse("ENCRYPTION", "and sys.sql_modules holds no definition of it")
	case row.nativelyCompiled:
		return schemaext.Facets{}, refuse("NATIVE_COMPILATION", "which Ptah cannot write")
	case row.inlineOption != "":
		return schemaext.Facets{}, refuse(row.inlineOption, "which Ptah cannot write")
	case row.inlineable && !row.inlined:
		return schemaext.Facets{}, refuse("INLINE = OFF", "which Ptah cannot write")
	}
	options := mssqlschema.RoutineOptions{SchemaBinding: row.schemaBound, Recompile: row.recompiled, ReturnsNullOnNullInput: row.nullOnNullInput}
	if row.executeAs != "CALLER" && row.executeAs != "-2" {
		if strings.TrimSpace(row.principal) == "" {
			return schemaext.Facets{}, fmt.Errorf("routine %q runs as principal id %s, which USER_NAME does not name", routine, row.executeAs)
		}
		options.Principal = row.principal
	}
	if options.IsZero() {
		return schemaext.Facets{}, nil
	}
	return mssqlschema.WithObservedRoutineOptions(schemaext.Facets{}, options)
}

// functionSecurity spells the principal a function runs as.
//
// T-SQL writes it `WITH EXECUTE AS OWNER` or `EXECUTE AS CALLER`, and the
// catalog reports the first as a principal id and the second as NULL. CALLER is
// the default and is what the shared declaration calls INVOKER.
func functionSecurity(executeAs string) string {
	if executeAs == "CALLER" {
		return "INVOKER"
	}
	return "DEFINER"
}

// functionReturns spells what a function gives back.
//
// A scalar function has a row in sys.parameters at ordinal zero carrying its
// return type. A table-valued one has no such row at all -- the catalog
// describes its result as a table shape elsewhere -- so the type comes from
// sys.objects.type instead: IF is an inline table-valued function and TF a
// multi-statement one.
//
// The word is lower case because that is the spelling the declaration
// canonicalizes to, and the comparison is exact. A reader returning TABLE where
// the declaration holds table reports a difference on every run, which is the
// same non-convergence the key exists to prevent.
func functionReturns(objectType, scalarType string) string {
	switch strings.ToUpper(strings.TrimSpace(objectType)) {
	case "IF", "TF":
		return "table"
	default:
		return scalarType
	}
}

// isProcedureObjectType reports whether sys.objects.type names a stored
// procedure. The column is char(2), so the value arrives padded.
func isProcedureObjectType(objectType string) bool {
	return strings.EqualFold(strings.TrimSpace(objectType), "P")
}

// routineKind spells the kind the schema model uses. Empty means function,
// which is what every description written before procedures existed meant.
func routineKind(objectType string) string {
	if isProcedureObjectType(objectType) {
		return schemamodel.FunctionKindProcedure
	}
	return ""
}

// procedureAwareReturns answers the empty string for a procedure rather than
// letting functionReturns arrive there through a missing catalog row.
func procedureAwareReturns(objectType, scalarType string) string {
	if isProcedureObjectType(objectType) {
		return ""
	}
	return functionReturns(objectType, scalarType)
}

// routineBody extracts the body from the statement text sys.sql_modules keeps.
func routineBody(objectType, definition string) string {
	if isProcedureObjectType(objectType) {
		return procedureBody(definition)
	}
	return functionBody(definition)
}

// procedureBody returns everything after the AS that opens a procedure body.
//
// A function is found by scanning past RETURNS, which a procedure does not
// have, so the same walk would run off the end and hand back the whole CREATE
// statement. The header ends at the first standalone AS outside brackets --
// except the one in `WITH EXECUTE AS OWNER`, which is part of the header and
// sits before the real one. Measured on SQL Server 2025: a procedure created
// `WITH EXECUTE AS OWNER AS BEGIN ... END` keeps both words in the definition,
// so a walk that took the first AS would report `OWNER AS BEGIN ... END` as
// the body and replan the procedure on every run.
func procedureBody(definition string) string {
	upper := strings.ToUpper(definition)
	depth := 0
	for i := 0; i < len(definition); i++ {
		if end := commentEnd(definition, i); end >= 0 {
			i = end
			continue
		}
		switch definition[i] {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case '\'':
			if next := strings.IndexByte(definition[i+1:], '\''); next >= 0 {
				i += next + 1
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if i+2 > len(upper) || upper[i:i+2] != "AS" || !standaloneWord(definition, i, i+2) {
			continue
		}
		if precededByExecute(upper, i) || precededByParameter(definition, i) {
			continue
		}
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition[i+2:]), ";"))
	}
	return strings.TrimSpace(definition)
}

// precededByParameter reports whether the AS at position i follows a
// parameter's name, as in `@a AS INT`, which T-SQL accepts in a procedure's
// parameter list. Measured on SQL Server 2025 RTM-CU9, the definition keeps it.
func precededByParameter(definition string, i int) bool {
	end := i
	for end > 0 && isSpaceByte(definition[end-1]) {
		end--
	}
	start := end
	for start > 0 && isWordByte(definition[start-1]) {
		start--
	}
	return end-start > 1 && definition[start] == '@'
}

// precededByExecute reports whether the AS at position i is the one in an
// EXECUTE AS clause rather than the one that opens the body.
func precededByExecute(upper string, i int) bool {
	end := i
	for end > 0 && isSpaceByte(upper[end-1]) {
		end--
	}
	const executeWord = "EXECUTE"
	start := end - len(executeWord)
	if start < 0 || upper[start:end] != executeWord {
		return false
	}
	return standaloneWord(upper, start, end)
}

func isSpaceByte(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// routineInlineOption reads an explicit INLINE setting from the stored header.
// Catalog inlineability flags do not distinguish an explicit OFF setting on a
// function that cannot be inlined from the same function without that setting.
func routineInlineOption(definition, objectType string) string {
	body := routineBody(objectType, definition)
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition), ";"))
	header, found := strings.CutSuffix(trimmed, body)
	if !found || header == "" {
		return ""
	}
	start := headerWith(header)
	if start < 0 {
		return ""
	}
	for _, option := range splitTopLevel(header[start:]) {
		scan := lexer.NewLexerWithOptions(option, dialectlexer.Options(platform.SQLServer))
		var words []string
		for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
			if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
				words = append(words, strings.ToUpper(token.Value))
			}
		}
		if len(words) > 0 && words[0] == "INLINE" {
			if len(words) >= 3 {
				return strings.Join(words[:3], " ")
			}
			return "INLINE"
		}
	}
	return ""
}
