package difftypes

import (
	"strings"

	"ptah.run/core/schemamodel"
)

// GrantDeclarationsOf is every privilege db grants, as the references a plan
// names a grant with: one per privilege and column of each grant, in the
// declaration's order.
func GrantDeclarationsOf(db *schemamodel.Database) []GrantRef {
	if db == nil || len(db.Grants) == 0 {
		return nil
	}
	var refs []GrantRef
	for _, grant := range db.Grants {
		refs = append(refs, GrantRefsOf(grant)...)
	}
	return refs
}

// GrantRefsOf is grant as the references a plan names a grant with: one per
// privilege and, for a grant limited to columns, per column.
func GrantRefsOf(grant schemamodel.Grant) []GrantRef {
	grant.Canonicalize()
	objectType := "TABLE"
	objectName := grant.OnTable
	arguments := ""
	switch {
	case grant.OnDatabase:
		objectType = "DATABASE"
		objectName = ""
	case grant.OnObject != "":
		objectType = grant.ObjectKind
		objectName = grant.OnObject
	case grant.OnSchema != "":
		objectType = "SCHEMA"
		objectName = grant.OnSchema
	case grant.OnSequence != "":
		objectType = "SEQUENCE"
		objectName = grant.OnSequence
	case grant.OnRoutine != "":
		objectType = grant.RoutineKind
		objectName = grant.OnRoutine
		arguments = grant.RoutineArguments
	}
	columns := grant.Columns
	if len(columns) == 0 {
		columns = []string{""}
	}
	refs := make([]GrantRef, 0, len(grant.Privileges)*len(columns))
	for _, privilege := range grant.Privileges {
		for _, column := range columns {
			refs = append(refs, GrantRef{
				Role:       grant.Role,
				Privilege:  strings.ToUpper(strings.TrimSpace(privilege)),
				ObjectType: objectType,
				ObjectName: objectName,
				Arguments:  arguments,
				Column:     column,
				WithOption: grant.WithOption,
			})
		}
	}
	return refs
}
