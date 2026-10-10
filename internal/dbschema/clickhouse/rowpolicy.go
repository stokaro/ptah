package clickhouse

import (
	"context"
	"database/sql"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// readRowPolicies reads the row policies on the tables of the connected
// database as the ClickHouse owner's observed row policies.
//
// system.row_policies returns the parts as columns rather than as a
// statement, measured on ClickHouse 26.7.3.19:
//
//	short_name | select_filter | is_restrictive | apply_to_all | apply_to_list
//	pol        | tenant = 1    | 0              | 0            | ['r1']
//
// so the filter, the composition and the role selection come back as the
// model holds them: select_filter is NULL for a policy without USING, and
// apply_to_all with apply_to_except is TO ALL EXCEPT. A policy is named in the
// connection's database, the one its table is read from, so its reference
// leaves the database out as the table does.
//
// A policy written ON db.* applies to every table of the database, and the
// server reports it with an empty table. The model holds a policy on one
// table only (see [chschema.ValidateRowPolicyRef]), so those rows are not
// read, and nothing is planned for them.
func (r *Reader) readRowPolicies(ctx context.Context, dbName string) ([]schemaext.Object, error) {
	query := `
		SELECT short_name, table, select_filter, is_restrictive, apply_to_all, apply_to_list, apply_to_except
		FROM system.row_policies
		WHERE database = ? AND table != ''
		ORDER BY table, short_name`
	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var policies []schemaext.Object
	for rows.Next() {
		var name, table string
		var filter sql.NullString
		var restrictive, applyToAll bool
		var applyTo, applyToExcept []string
		if err := rows.Scan(&name, &table, &filter, &restrictive, &applyToAll, &applyTo, &applyToExcept); err != nil {
			return nil, err
		}
		observed := chschema.ObservedRowPolicy{
			Composition: chschema.Permissive,
			Roles:       chschema.RoleSelection{All: applyToAll, Names: applyTo, Except: applyToExcept},
		}
		if restrictive {
			observed.Composition = chschema.Restrictive
		}
		if filter.Valid {
			observed.Filter = new(filter.String)
		}
		policy, err := chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("", table, name), observed)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return policies, nil
}

// rowPolicyCoverage is what a read knows about row policies: every one on a
// table of the database when it read them, and none when the account could
// not, in which case a declared policy is undecided rather than created and an
// undeclared one is kept. readErr is the access refusal of the read, or nil.
func rowPolicyCoverage(readErr error) (schemaext.Coverage, error) {
	knowledge := schemaext.Knowledge{State: schemaext.Complete}
	if readErr != nil {
		knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the account may not read system.row_policies"}
	}
	return chschema.RowPolicyCoverage(schemaext.Observed, knowledge, nil)
}
