package mssql

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// readFunctions recovers the functions the connected database declares.
//
// The issue this closes assumed the header would have to be parsed out of
// sys.sql_modules.definition, which returns the whole original `CREATE`
// statement as one string. It does not. SQL Server publishes the header in the
// catalog the same way MySQL's information_schema.ROUTINES does, and measured
// on SQL Server 2025 (RTM-CU8), 17.0.4075.5:
//
//	SPECIFIC_NAME | ORDINAL_POSITION | PARAMETER_NAME | DATA_TYPE | CHARACTER_MAXIMUM_LENGTH
//	fn_scalar     | 0                | NULL           | varchar   | 100
//	fn_scalar     | 1                | @a             | int       | NULL
//	fn_scalar     | 2                | @b             | varchar   | 50
//
// Ordinal 0 is the return value and the rest are the arguments in order, so
// the name, the parameter list and the return type all come from
// INFORMATION_SCHEMA.PARAMETERS. Only the BODY has to be taken out of the
// definition text, which is a much narrower problem than a header parser: the
// header's end is the last `AS` outside brackets and quotes, and everything
// after it is the body.
//
// What the catalog does not carry is a parameter's DEFAULT.
// `@b varchar(50) = 'x'` reports has_default_value = 0 in sys.parameters,
// because SQL Server records defaults for CLR parameters only. A declaration
// carrying one therefore reads back without it and would be replanned forever,
// which is why the renderer refuses that shape up front instead.
func (r *Reader) readFunctions() ([]types.DBFunction, error) {
	parameters, returns, err := r.readRoutineSignatures()
	if err != nil {
		return nil, err
	}

	query := `
		SELECT s.name, o.name, o.type, m.definition,
			   ISNULL(CONVERT(varchar(30), m.execute_as_principal_id), 'CALLER') AS execute_as,
			   ISNULL(OBJECTPROPERTY(o.object_id, 'IsDeterministic'), 0) AS is_deterministic
		FROM sys.objects AS o
		JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		JOIN sys.sql_modules AS m ON m.object_id = o.object_id
		WHERE o.type IN ('FN', 'IF', 'TF') AND o.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, o.name`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []types.DBFunction
	for rows.Next() {
		var schema, name, objectType, definition, executeAs string
		var deterministic int
		if err := rows.Scan(&schema, &name, &objectType, &definition,
			&executeAs, &deterministic); err != nil {
			return nil, err
		}
		key := routineKey{schema: schema, name: name}
		functions = append(functions, types.DBFunction{
			Name:       name,
			Schema:     r.outputSchema(schema),
			Parameters: parameters[key],
			Returns:    functionReturns(objectType, returns[key]),
			// The language is the engine's own. T-SQL has no LANGUAGE clause --
			// `LANGUAGE SQL` is `Incorrect syntax near 'LANGUAGE'` -- so there
			// is nothing to read, and reporting "sql" is what lets a
			// declaration that says the same compare equal.
			Language: "sql",
			// Both of these are read rather than assumed, because the catalog
			// carries them and a default written in here would be a claim about
			// somebody else's function. execute_as_principal_id is NULL for
			// EXECUTE AS CALLER and a principal id for OWNER; IsDeterministic
			// is the engine's own verdict on the body, which is where T-SQL
			// keeps the fact PostgreSQL spells as volatility.
			Security:   functionSecurity(executeAs),
			Volatility: functionVolatility(deterministic),
			Body:       functionBody(definition),
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
func (r *Reader) readRoutineSignatures() (parameters, returns map[routineKey]string, err error) {
	query := `
		SELECT s.name, o.name, p.parameter_id, ISNULL(p.name, ''), t.name,
			   p.max_length, p.precision, p.scale
		FROM sys.objects AS o
		JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		JOIN sys.parameters AS p ON p.object_id = o.object_id
		JOIN sys.types AS t ON t.user_type_id = p.user_type_id
		WHERE o.type IN ('FN', 'IF', 'TF') AND o.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, o.name, p.parameter_id`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
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
		if i+2 > len(upper) || upper[i:i+2] != "AS" || !standaloneWord(definition, i, i+2) {
			continue
		}
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition[i+2:]), ";"))
	}
	return strings.TrimSpace(definition)
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

// functionVolatility maps the engine's determinism verdict onto the shared
// vocabulary.
//
// T-SQL has no volatility clause: determinism is inferred from the body, and
// SCHEMABINDING is what usually makes it true. A function Ptah wrote is not
// schema-bound and reports false, which is the VOLATILE a declaration defaults
// to -- so the two agree without either side pretending.
func functionVolatility(deterministic int) string {
	if deterministic == 1 {
		return "IMMUTABLE"
	}
	return "VOLATILE"
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
