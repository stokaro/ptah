package compare

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlroutine"
	"ptah.run/internal/tableref"
	"ptah.run/migration/schemadiff/difftypes"
)

// ValidateRoutineDefinerReplacements refuses a routine replacement that would
// silently change the execution principal.
//
// On a target with [capability.RoutineReplacementResetsDefiner], Ptah's plan
// replaces a modified routine by DROP followed by CREATE, and the CREATE makes
// the connected account the new definer. When both the current and the desired
// security are DEFINER and the routine's definer is another account, that is a
// change of the principal the body runs as, which the declaration did not ask
// for. The capability is the gate, not the dialect: the rule holds wherever
// the plan replaces a routine that way.
//
// A routine whose definer or connected account the read did not record, such as
// one from a description rather than a live read, is refused as well: treating
// missing facts as the same account would allow the change this guard exists
// to stop.
//
// Only database-aware comparison has an error channel and reader-supplied
// ownership facts, so the validation belongs on that boundary rather than in
// FunctionsWithSemantics or the planner. The latter would see the unsafe diff
// only after comparison had already represented it as an executable change.
func ValidateRoutineDefinerReplacements(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	caps capability.Capabilities,
	semantics identifier.Semantics,
) error {
	if !caps.Has(capability.RoutineReplacementResetsDefiner) || desired == nil || database == nil || diff == nil ||
		len(diff.FunctionsModified) == 0 {
		return nil
	}

	modified := make(map[string]struct{}, len(diff.FunctionsModified))
	for _, functionDiff := range diff.FunctionsModified {
		modified[functionDiff.FunctionName] = struct{}{}
	}

	semantics = semantics.Normalize("")
	for _, desired := range desired.Functions {
		if _, ok := modified[desired.Name]; !ok {
			continue
		}
		current, ok := findCurrentFunctionForDesired(desired, database.Functions, dialect, semantics)
		if !ok {
			continue
		}

		desired.Canonicalize()
		// A routine in a language the target does not run is left alone,
		// with no DROP, so nothing is replaced. The targets with the
		// capability are the MySQL family, whose plan decides this with
		// mysqlroutine.RunsLanguage.
		if !mysqlroutine.RunsLanguage(desired.Language) {
			continue
		}
		if !strings.EqualFold(current.Security, "DEFINER") || desired.Security != "DEFINER" {
			continue
		}
		if current.Definer == "" || database.CurrentAccount == "" {
			return fmt.Errorf(
				"%w: cannot safely replace %s function %q with SQL SECURITY DEFINER: "+
					"catalog ownership facts are incomplete (definer %q, connected account %q); "+
					"the replacement could change its execution principal",
				ptaherr.ErrInvalidSchemaDiff,
				dialect,
				desired.Name,
				current.Definer,
				database.CurrentAccount,
			)
		}
		if current.Definer != database.CurrentAccount {
			return fmt.Errorf(
				"%w: cannot safely replace %s function %q with SQL SECURITY DEFINER: "+
					"catalog definer %q differs from connected account %q; dropping and recreating "+
					"it would change the execution principal. Connect as the routine definer, "+
					"declare SQL SECURITY INVOKER, or leave it unchanged",
				ptaherr.ErrInvalidSchemaDiff,
				dialect,
				desired.Name,
				current.Definer,
				database.CurrentAccount,
			)
		}
	}
	return nil
}

func findCurrentFunctionForDesired(
	desired schemamodel.Function,
	current []catalog.Function,
	dialect string,
	semantics identifier.Semantics,
) (catalog.Function, bool) {
	if semantics.DefaultSchema != "" {
		byIdentity := make(map[objectIdentity]catalog.Function, len(current))
		for _, function := range current {
			identity := newObjectIdentity(
				routineIdentityKind(function.Kind),
				function.Schema,
				routineIdentityKey(function.Name, dialect),
				semantics,
			)
			byIdentity[identity] = function
		}
		identity := newQualifiedObjectIdentity(
			routineIdentityKind(desired.Kind),
			qualifiedRoutineIdentityKey(desired.Name, dialect),
			semantics,
		)
		function, ok := byIdentity[identity]
		return function, ok
	}

	byName := make(map[string][]catalog.Function, len(current))
	byQualifiedName := make(map[string]catalog.Function, len(current))
	for _, function := range current {
		key := routineIdentityKey(function.Name, dialect)
		byName[routineKeyWithKind(function.Kind, key)] = append(byName[routineKeyWithKind(function.Kind, key)], function)
		byQualifiedName[routineKeyWithKind(function.Kind, tableref.Canonical(function.Schema, key))] = function
	}
	return findDatabaseFunction(
		desired.Kind,
		qualifiedRoutineIdentityKey(desired.Name, dialect),
		byName,
		byQualifiedName,
	)
}
