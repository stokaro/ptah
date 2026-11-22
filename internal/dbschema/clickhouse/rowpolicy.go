package clickhouse

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/revisiontable"
)

// readRowPolicies reads the row policies of the connected database as the
// ClickHouse owner's observed row policies, and records the ones on a relation
// the read does not hold as a table as policies it cannot describe. held
// reports whether the read holds a table of a name.
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
// table only (see [chschema.ValidateRowPolicyRef]), so such a policy is
// recorded as one the read cannot describe, under the table name `*`, the
// server's own spelling: the comparison keeps it and reports it, and nothing
// claims the read described it.
//
// The server binds a policy to a table name, not to a table: it keeps the
// policy when the table is dropped, accepts one for a name no relation has,
// and applies it to any relation created under the name later. Measured on
// 24.10.4.191 and 26.10.3.7, a policy on a dropped table stays in
// system.row_policies, and ALTER ROW POLICY and DROP ROW POLICY both succeed
// on it while no table of the name exists. So a policy whose name no relation
// of the database has is read like any other: the comparison plans it as an
// object of its own (see [ptah.run/engine.ObjectComparison.OutlivesParent]).
// An account cannot see a policy on a table it cannot see: SHOW ROW POLICIES
// is a table-level privilege, and on both lines an account that holds it
// lists the table in system.tables even after SHOW TABLES on it is revoked,
// while one without it is refused system.row_policies (code 497).
//
// A policy on a relation the read does not hold as a table -- a view, a
// materialized view, a dictionary, a table whose engine the read leaves out
// such as Null or Distributed, or Ptah's own bookkeeping table -- has no table
// in the description to be compared beside, and it is not a policy on a
// dropped table either. It is recorded as unrepresentable, so the comparison
// keeps it and reports it rather than dropping it. Whether a relation of the
// name exists is asked of system.tables, which lists dictionaries too, and
// only when some policy's table is not one the read holds.
//
// Such a policy's definition is still read, as an object the comparison never
// reads. A plan that drops a view drops its policies with it, and the
// rollback that creates the view again recreates them from this definition:
// system.row_policies reports the filter, the composition and both role lists
// for a policy on a view as it does for one on a table, measured on
// 24.10.4.191 and 26.10.3.7.
func (r *Reader) readRowPolicies(ctx context.Context, dbName string, held func(string) bool) (rowPolicyRead, error) {
	rows, err := r.readRowPolicyRows(ctx, dbName)
	if err != nil {
		return rowPolicyRead{}, err
	}
	var relations map[string]string
	unheldEngine := func(table string) (string, bool, error) {
		if held(table) {
			return "", false, nil
		}
		if relations == nil {
			if relations, err = r.readRelationEngines(ctx, dbName); err != nil {
				return "", false, err
			}
		}
		engine, found := relations[table]
		return engine, found, nil
	}
	var read rowPolicyRead
	for _, row := range rows {
		if row.table == "" {
			read.unread = append(read.unread, schemaext.SubjectCoverage{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("", "*", row.name),
				Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: fmt.Sprintf(
					"the policy is written ON %s.*, for every table of the database, which Ptah does not model; the plan keeps the policy", dbName)}})
			continue
		}
		ref := chschema.RowPolicyRef("", row.table, row.name)
		object, err := chschema.ObservedRowPolicyObject(ref, row.observed)
		if err != nil {
			return rowPolicyRead{}, err
		}
		engine, unheld, err := unheldEngine(row.table)
		if err != nil {
			return rowPolicyRead{}, err
		}
		if !unheld {
			read.policies = append(read.policies, object)
			continue
		}
		read.unread = append(read.unread, schemaext.SubjectCoverage{Kind: chschema.RowPolicyKind, Subject: ref,
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: unreadRelationReason(row.table, engine)}})
		read.definitions = append(read.definitions, object)
	}
	return read, nil
}

// rowPolicyRead is what one read of system.row_policies establishes: the
// policies the read describes, the records of the ones it cannot, and the
// definitions of the ones on a relation, kept as planning context.
type rowPolicyRead struct {
	policies    []schemaext.Object
	unread      []schemaext.SubjectCoverage
	definitions []schemaext.Object
}

// rowPolicyRow is one row of system.row_policies: a policy's name, its table,
// and the policy as the model observes it.
type rowPolicyRow struct {
	name, table string
	observed    chschema.ObservedRowPolicy
}

// readRowPolicyRows reads every policy of the database: on one table, or, with
// an empty table, on all of them.
func (r *Reader) readRowPolicyRows(ctx context.Context, dbName string) ([]rowPolicyRow, error) {
	query := `
		SELECT short_name, table, select_filter, is_restrictive, apply_to_all, apply_to_list, apply_to_except
		FROM system.row_policies
		WHERE database = ?
		ORDER BY table, short_name`
	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var read []rowPolicyRow
	for rows.Next() {
		var row rowPolicyRow
		var filter sql.NullString
		var restrictive, applyToAll bool
		var applyTo, applyToExcept []string
		if err := rows.Scan(&row.name, &row.table, &filter, &restrictive, &applyToAll, &applyTo, &applyToExcept); err != nil {
			return nil, err
		}
		row.observed = chschema.ObservedRowPolicy{
			Composition: chschema.Permissive,
			Roles:       chschema.RoleSelection{All: applyToAll, Names: applyTo, Except: applyToExcept},
		}
		if restrictive {
			row.observed.Composition = chschema.Restrictive
		}
		if filter.Valid {
			row.observed.Filter = new(filter.String)
		}
		read = append(read, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return read, nil
}

// readRelationEngines reads the engine of every relation of the database by
// its name: tables of any engine, views, materialized views and their
// storage, and dictionaries.
func (r *Reader) readRelationEngines(ctx context.Context, dbName string) (map[string]string, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT name, engine
		FROM system.tables
		WHERE database = ? AND is_temporary = 0`, dbName)
	if err != nil {
		return nil, fmt.Errorf("read the relations the row policies are on: %w", err)
	}
	defer rows.Close()
	relations := make(map[string]string)
	for rows.Next() {
		var name, engine string
		if err := rows.Scan(&name, &engine); err != nil {
			return nil, err
		}
		relations[name] = engine
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return relations, nil
}

// unreadRelationReason says why a policy on a relation the read does not hold
// as a table is not described.
func unreadRelationReason(table, engine string) string {
	if revisiontable.IsNative(table) {
		return fmt.Sprintf("the policy is on %q, Ptah's own bookkeeping table, which the read leaves out; the plan keeps the policy", table)
	}
	return fmt.Sprintf("the policy is on %q (engine %s), which the read does not describe as a table; the plan keeps the policy", table, engine)
}

// rowPolicyCoverage is what a read knows about row policies: every one on a
// table of the database, and on a name no relation of it has, when it read
// them, except unread, the ones on a relation it does not hold as a table or
// on the whole database;
// and none when the account could not, in which case a declared policy is
// undecided rather than created and an undeclared one is kept. readErr is the
// access refusal of the read, or nil.
func rowPolicyCoverage(readErr error, unread []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	knowledge := schemaext.Knowledge{State: schemaext.Complete}
	if readErr != nil {
		knowledge, unread = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the account may not read system.row_policies"}, nil
	}
	return chschema.RowPolicyCoverage(schemaext.Observed, knowledge, unread)
}
