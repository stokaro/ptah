package clickhouse

import (
	"database/sql"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// readRowPolicies reads the row policies the connected database declares.
//
// The read half needed no parser, which is what made this reachable at all.
// system.row_policies returns the parts as columns rather than as a statement,
// measured on ClickHouse 26.7.3.19:
//
//	short_name | select_filter | is_restrictive | apply_to_all | apply_to_list
//	pol        | tenant = 1    | 0              | 0            | ['r1']
//
// That is the same shape the declaration holds, so USING and TO come back where
// they were written.
//
// One column has nowhere to go. is_restrictive separates a policy that narrows
// what other policies permit from one that widens it, and the declaration model
// has no field for it. A restrictive policy therefore reads back looking exactly
// like the permissive one a declaration describes, and the comparator would call
// them the same. This renderer writes AS PERMISSIVE explicitly so its own
// policies are never that ambiguous, and managing a restrictive one is left
// undone rather than half-done (stokaro/ptah#1736).
func (r *Reader) readRowPolicies(dbName string) ([]types.DBRLSPolicy, error) {
	query := `
		SELECT short_name, table, select_filter, apply_to_all, apply_to_list, apply_to_except
		FROM system.row_policies
		WHERE database = ?
		ORDER BY table, short_name`
	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var policies []types.DBRLSPolicy
	for rows.Next() {
		var name, table string
		var filter sql.NullString
		var applyToAll bool
		var applyTo, applyToExcept []string
		if err := rows.Scan(&name, &table, &filter, &applyToAll, &applyTo, &applyToExcept); err != nil {
			return nil, err
		}
		policies = append(policies, types.DBRLSPolicy{
			Name:  name,
			Table: table,
			// A policy with no TO clause applies to nobody in particular, and
			// the catalog reports that as an empty list rather than as a name.
			// Spelling it back as an empty string is what lets a declaration
			// that also names no role compare equal. The catalog splits the
			// clause across three columns, and all three are read: a policy
			// written `TO ALL EXCEPT r1` reports apply_to_all with an exception
			// list, and building the string from apply_to_list alone would read
			// it back as naming nobody.
			ToRoles:         rowPolicyGrantees{all: applyToAll, list: applyTo, except: applyToExcept}.clause(),
			UsingExpression: filter.String,
			// ClickHouse's FOR accepts ALL and SELECT only, and the catalog
			// does not report which was used -- a SELECT-only policy and an ALL
			// policy are stored the same way. ALL is what an unset declaration
			// means, so it is what a read reports; a declaration naming SELECT
			// is the one case this cannot tell apart, and the renderer emits
			// the operation the declaration asked for either way.
			PolicyFor: "ALL",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return policies, nil
}

// rowPolicyGrantees is the TO clause as the catalog splits it.
//
// `TO r1, r2` fills the list. `TO ALL` sets all. `TO ALL EXCEPT r1` sets all
// with an exception list. Reading only the list would turn the last two into a
// policy that names nobody, which is the spelling a policy with no TO clause at
// all uses -- so a policy applying to everyone would compare equal to one
// applying to no one.
type rowPolicyGrantees struct {
	all    bool
	list   []string
	except []string
}

// clause spells the three columns back as one declaration would write them.
func (g rowPolicyGrantees) clause() string {
	if !g.all {
		return strings.Join(g.list, ", ")
	}
	if len(g.except) == 0 {
		return "ALL"
	}
	return "ALL EXCEPT " + strings.Join(g.except, ", ")
}
