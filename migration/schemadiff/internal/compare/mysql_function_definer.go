package compare

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/mysqlroutine"
	"go.5x5.cz/ptah/internal/tableref"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// ValidateMySQLFunctionDefinerReplacements refuses a routine replacement that
// would silently change the execution principal.
//
// MySQL and MariaDB have no CREATE OR REPLACE FUNCTION form, so a modified
// function is planned as DROP followed by CREATE. A CREATE issued by an account
// other than the existing routine's DEFINER records the connected account as
// the new definer. When both current and desired security are DEFINER, that is
// a behavioral change the desired declaration did not ask for.
//
// Only database-aware comparison has an error channel and reader-supplied
// ownership facts, so the validation belongs on that boundary rather than in
// FunctionsWithSemantics or the planner. The latter would see the unsafe diff
// only after comparison had already represented it as an executable change.
func ValidateMySQLFunctionDefinerReplacements(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
) error {
	if !isMySQLFamily(dialect) || generated == nil || database == nil || diff == nil ||
		len(diff.FunctionsModified) == 0 {
		return nil
	}

	modified := make(map[string]struct{}, len(diff.FunctionsModified))
	for _, functionDiff := range diff.FunctionsModified {
		modified[functionDiff.FunctionName] = struct{}{}
	}

	semantics = semantics.Normalize("")
	for _, desired := range generated.Functions {
		if _, ok := modified[desired.Name]; !ok {
			continue
		}
		current, ok := findCurrentFunctionForDesired(desired, database.Functions, dialect, semantics)
		if !ok {
			continue
		}

		desired.Canonicalize()
		if !mysqlroutine.RunsLanguage(desired.Language) {
			continue
		}
		if !strings.EqualFold(current.Security, "DEFINER") || desired.Security != "DEFINER" {
			continue
		}
		if current.Definer == "" || current.CurrentAccount == "" {
			return fmt.Errorf(
				"%w: cannot safely replace %s function %q with SQL SECURITY DEFINER: "+
					"catalog ownership facts are incomplete (definer %q, connected account %q); "+
					"the replacement could change its execution principal",
				ptaherr.ErrInvalidSchemaDiff,
				dialect,
				desired.Name,
				current.Definer,
				current.CurrentAccount,
			)
		}
		if current.Definer != current.CurrentAccount {
			return fmt.Errorf(
				"%w: cannot safely replace %s function %q with SQL SECURITY DEFINER: "+
					"catalog definer %q differs from connected account %q; dropping and recreating "+
					"it would change the execution principal. Connect as the routine definer, "+
					"declare SQL SECURITY INVOKER, or leave it unchanged",
				ptaherr.ErrInvalidSchemaDiff,
				dialect,
				desired.Name,
				current.Definer,
				current.CurrentAccount,
			)
		}
	}
	return nil
}

func findCurrentFunctionForDesired(
	desired goschema.Function,
	current []types.DBFunction,
	dialect string,
	semantics identifier.Semantics,
) (types.DBFunction, bool) {
	if semantics.DefaultSchema != "" {
		byIdentity := make(map[tableIdentity]types.DBFunction, len(current))
		for _, function := range current {
			identity := newTableIdentity(
				function.Schema,
				routineIdentityKey(function.Name, dialect),
				semantics,
			)
			byIdentity[identity] = function
		}
		identity := newQualifiedTableIdentity(
			qualifiedRoutineIdentityKey(desired.Name, dialect),
			semantics,
		)
		function, ok := byIdentity[identity]
		return function, ok
	}

	byName := make(map[string][]types.DBFunction, len(current))
	byQualifiedName := make(map[string]types.DBFunction, len(current))
	for _, function := range current {
		key := routineIdentityKey(function.Name, dialect)
		byName[key] = append(byName[key], function)
		byQualifiedName[tableref.Canonical(function.Schema, key)] = function
	}
	return findDatabaseFunction(
		qualifiedRoutineIdentityKey(desired.Name, dialect),
		byName,
		byQualifiedName,
	)
}
