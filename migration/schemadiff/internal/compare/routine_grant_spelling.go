package compare

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlroutine"
	"ptah.run/internal/tableref"
)

// routineGrantsSpelledAsRead returns desired with each grant, revoked grant
// and denied grant on a routine named the way database spells the routine,
// where the target folds the case of a routine name and database holds the
// routine under another case. desired is not changed.
//
// A routine name compares without case on the MySQL family, and a grant names
// the routine as its statement wrote it: measured on MySQL 8.4.13 and 26.10.0
// and MariaDB 11.8.9 and 12.3.3, `GRANT EXECUTE ON FUNCTION F1 TO r` on the
// function f1 writes F1 to mysql.procs_priv, and the read reports the grant on
// f1. A grant identity compares the routine as a table name, which keeps cases
// apart where lower_case_table_names is 0. Without this, a declared grant on F1
// is planned again on every run, and the read grant on f1 revoked by every
// plan.
//
// The database part of a qualified name keeps its spelling and is compared as
// a table's database is.
func routineGrantsSpelledAsRead(
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) *schemamodel.Database {
	if desired == nil || database == nil || len(database.Functions) == 0 {
		return desired
	}
	respell := func(grants []schemamodel.Grant) []schemamodel.Grant {
		if !slices.ContainsFunc(grants, func(grant schemamodel.Grant) bool { return grant.OnRoutine != "" }) {
			return grants
		}
		spelled := slices.Clone(grants)
		for position, grant := range spelled {
			if grant.OnRoutine != "" {
				spelled[position].OnRoutine = readRoutineSpelling(grant, database, semantics)
			}
		}
		return spelled
	}
	out := *desired
	out.Grants = respell(desired.Grants)
	out.RevokedGrants = respell(desired.RevokedGrants)
	out.DeniedGrants = respell(desired.DeniedGrants)
	return &out
}

// readRoutineSpelling is the name of the routine grant names, with the
// routine part spelled as database spells the routine of that kind, or as
// grant names it where database holds no such routine.
func readRoutineSpelling(grant schemamodel.Grant, database *catalog.Database, semantics identifier.Semantics) string {
	ref, ok := tableref.Parse(grant.OnRoutine)
	if !ok {
		return grant.OnRoutine
	}
	kind := "function"
	if strings.EqualFold(grant.RoutineKind, "PROCEDURE") {
		kind = "procedure"
	}
	for _, function := range database.Functions {
		owner := cmp.Or(function.Schema, database.CurrentDatabase)
		sameSchema := !ref.Qualified || semantics.TableIdentityKey(ref.Schema) == semantics.TableIdentityKey(owner)
		if sameSchema && cmp.Or(strings.ToLower(function.Kind), "function") == kind &&
			mysqlroutine.IdentityKey(function.Name) == mysqlroutine.IdentityKey(ref.Name) {
			return tableref.Canonical(ref.Schema, function.Name)
		}
	}
	return grant.OnRoutine
}
