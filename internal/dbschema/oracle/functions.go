package oracle

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/oracleroutine"
)

// routineQuery reads the standalone functions and procedures this schema owns.
//
// PROCEDURE_NAME IS NULL is what makes them standalone. ALL_PROCEDURES has one
// row per standalone routine with that column empty, and one row per subprogram
// of a package with the subprogram's name in it; without the predicate every
// package member would arrive as a top-level routine of its package's name.
//
// AUTHID and DETERMINISTIC are the two header properties Ptah's model carries
// that this view reports, and both were measured on 23.26.2.0.0 and on
// 21.3.0.0.0 with the same answers:
//
//	OBJECT_NAME  AUTHID        DETERMINISTIC
//	ZZ_A         CURRENT_USER  YES
//	ZZ_P         DEFINER       NO
//
// DETERMINISTIC is reported for a procedure too, not only for a function, so
// the volatility a declaration states survives the round trip on both kinds.
const routineQuery = `
SELECT p.object_name, p.object_type, p.authid, p.deterministic
FROM all_procedures p
WHERE p.owner = :1
  AND p.object_type IN ('FUNCTION', 'PROCEDURE')
  AND p.procedure_name IS NULL
ORDER BY p.object_name`

// routineArgumentQuery reads the parameter list and the return type.
//
// POSITION 0 is the return value and carries no name; the arguments follow in
// declaration order. DATA_LEVEL 0 keeps the argument itself rather than the
// fields a record or collection argument expands into, and PACKAGE_NAME IS
// NULL keeps the standalone routines this reader describes.
//
// A formal parameter's type is unconstrained in PL/SQL -- `p IN VARCHAR2(50)`
// is PLS-00103 -- so DATA_LENGTH, DATA_PRECISION and DATA_SCALE have nothing to
// put back and are not read. That is what makes this simpler than the SQL
// Server reader's routineTypeName, which has to rebuild every facet.
const routineArgumentQuery = `
SELECT a.object_name, a.position, NVL(a.argument_name, ' '),
       NVL(a.data_type, ' '), NVL(a.in_out, ' ')
FROM all_arguments a
WHERE a.owner = :1
  AND a.package_name IS NULL
  AND a.data_level = 0
ORDER BY a.object_name, a.position`

// routineSourceQuery reads the stored text of every standalone routine.
//
// ALL_SOURCE stores the statement WITHOUT its `CREATE OR REPLACE` prefix: line
// 1 of a function created as `CREATE OR REPLACE FUNCTION f(...) RETURN NUMBER
// IS` reads back as `FUNCTION f(...) RETURN NUMBER IS`. Everything after the
// header's `IS` is the body, which is what [oracleroutine.Body] takes out.
//
// The text is stored verbatim, comments and indentation included, so a body
// Ptah rendered reads back byte for byte and compares equal to the declaration
// that produced it.
const routineSourceQuery = `
SELECT s.name, s.type, s.text
FROM all_source s
WHERE s.owner = :1
  AND s.type IN ('FUNCTION', 'PROCEDURE')
ORDER BY s.name, s.type, s.line`

// readFunctions recovers the functions and procedures the connected schema
// declares.
//
// Three views answer between them what one statement declared, and none of the
// three is optional:
//
//   - ALL_PROCEDURES has the kind, the AUTHID and the determinism.
//   - ALL_ARGUMENTS has the parameters and the return type, so no header has to
//     be parsed for them.
//   - ALL_SOURCE has the text, which is the only place the body lives.
//
// What the catalog does NOT carry is a parameter's DEFAULT: ALL_ARGUMENTS
// reports DEFAULTED = 'Y' and never the value. A routine created with one
// therefore reads back without it and would be replanned on every run, which is
// why the renderer refuses that shape up front instead.
func (r *Reader) readFunctions(ctx context.Context) ([]catalog.Function, error) {
	parameters, returns, err := r.readRoutineArguments(ctx)
	if err != nil {
		return nil, fmt.Errorf("read routine arguments: %w", err)
	}
	sources, err := r.readRoutineSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("read routine sources: %w", err)
	}

	rows, err := r.db.QueryContext(ctx, routineQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []catalog.Function
	for rows.Next() {
		var name, objectType, authID, deterministic string
		if err := rows.Scan(&name, &objectType, &authID, &deterministic); err != nil {
			return nil, err
		}
		key := routineKey{name: name, objectType: objectType}
		functions = append(functions, catalog.Function{
			Name:       name,
			Kind:       routineKind(objectType),
			Parameters: parameters[key.name],
			// A procedure has no POSITION 0 row, so the map answers the empty
			// string for one by construction rather than by a rule written
			// here.
			Returns: returns[key.name],
			// PL/SQL is the engine's own routine language, and it is named
			// rather than left empty so that a declaration saying the same
			// compares equal. An annotation that omits `language=` is
			// defaulted to plpgsql by schemamodel.Function.Canonicalize and is
			// skipped by the renderer, which says so.
			Language:   oracleroutine.Language,
			Security:   oracleroutine.SecurityFromCatalog(authID),
			Volatility: oracleroutine.VolatilityFromCatalog(deterministic),
			Body:       oracleroutine.Body(sources[key]),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return functions, nil
}

// routineKey identifies a routine by the pair ALL_SOURCE is keyed on. A
// function and a procedure of the same name cannot both exist -- the second
// answers ORA-00955 -- but the view carries the type, and keying on the pair is
// what keeps a future package or type body from being read as a routine's text.
type routineKey struct{ name, objectType string }

// readRoutineArguments builds the parameter list and the return type of every
// standalone routine, from the catalog rather than from the statement text.
func (r *Reader) readRoutineArguments(ctx context.Context) (parameters, returns map[string]string, err error) {
	rows, err := r.db.QueryContext(ctx, routineArgumentQuery, r.schema)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	arguments := make(map[string][]string)
	returns = make(map[string]string)
	for rows.Next() {
		var name, argumentName, dataType, inOut string
		var position int
		if err := rows.Scan(&name, &position, &argumentName, &dataType, &inOut); err != nil {
			return nil, nil, err
		}
		if position == 0 {
			returns[name] = strings.ToLower(strings.TrimSpace(dataType))
			continue
		}
		arguments[name] = append(arguments[name], oracleroutine.Argument(argumentName, inOut, dataType))
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	parameters = make(map[string]string, len(arguments))
	for name, list := range arguments {
		parameters[name] = strings.Join(list, ", ")
	}
	return parameters, returns, nil
}

// readRoutineSources reassembles each routine's stored text from its lines.
func (r *Reader) readRoutineSources(ctx context.Context) (map[routineKey]string, error) {
	rows, err := r.db.QueryContext(ctx, routineSourceQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sources := make(map[routineKey]*strings.Builder)
	for rows.Next() {
		var name, objectType, text string
		if err := rows.Scan(&name, &objectType, &text); err != nil {
			return nil, err
		}
		key := routineKey{name: name, objectType: objectType}
		builder, ok := sources[key]
		if !ok {
			builder = &strings.Builder{}
			sources[key] = builder
		}
		builder.WriteString(text)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	joined := make(map[routineKey]string, len(sources))
	for key, builder := range sources {
		joined[key] = builder.String()
	}
	return joined, nil
}

// routineKind spells the kind the schema model uses. Empty means function,
// which is what every description written before procedures existed meant.
func routineKind(objectType string) string {
	if strings.EqualFold(strings.TrimSpace(objectType), "PROCEDURE") {
		return schemamodel.FunctionKindProcedure
	}
	return ""
}
