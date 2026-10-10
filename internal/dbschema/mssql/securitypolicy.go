package mssql

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// UnreadablePredicateReason records a security policy the read lists and
// does not describe, because sys.security_predicates reports one of its
// predicates in a shape the reader does not read as a function call.
const UnreadablePredicateReason = "the catalog reports a predicate of the security policy that the reader cannot read as a function call"

// readSecurityPolicies reads the security policies of the schemas this read
// covers as observations of the owner in package mssqlschema, with the
// coverage that claims them in full. A policy whose predicate it cannot read
// is recorded as uninspected rather than described, so a comparison neither
// drops nor changes it.
//
// The catalog reports each predicate on its own, fully bracketed and wrapped,
// with the policy's state, schema binding and replication behavior beside it:
//
//	policy | predicate_definition          | type   | operation
//	dbo.p  | ([dbo].[fn_tenant]([tenant])) | FILTER | NULL
//	dbo.p  | ([dbo].[fn_tenant]([tenant])) | BLOCK  | AFTER UPDATE
//
// Measured on SQL Server 2025 (RTM-CU8), 17.0.4075.5. A predicate's arguments
// are kept as the catalog spells them: the owner compares them with a
// declaration's (see [mssqlschema.CompareArgument]).
func (r *Reader) readSecurityPolicies(ctx context.Context) (schemaext.Objects, schemaext.Coverage, error) {
	query := `
		SELECT s.name, p.name, p.is_enabled, p.is_schema_bound, p.is_not_for_replication,
			   ts.name, t.name, sp.predicate_definition, sp.predicate_type_desc, sp.operation_desc
		FROM sys.security_policies AS p
		JOIN sys.schemas AS s ON s.schema_id = p.schema_id
		LEFT JOIN sys.security_predicates AS sp ON sp.object_id = p.object_id
		LEFT JOIN sys.objects AS t ON t.object_id = sp.target_object_id
		LEFT JOIN sys.schemas AS ts ON ts.schema_id = t.schema_id
		WHERE p.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, p.name, sp.security_predicate_id`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	defer rows.Close()

	var order []objectidentity.Key
	refs := make(map[objectidentity.Key]objectidentity.ID)
	policies := make(map[objectidentity.Key]*mssqlschema.ObservedSecurityPolicy)
	unreadable := make(map[objectidentity.Key]bool)
	for rows.Next() {
		var row securityPredicateRow
		if err := rows.Scan(&row.schema, &row.name, &row.enabled, &row.schemaBound, &row.notForReplication,
			&row.tableSchema, &row.table, &row.definition, &row.kind, &row.operation); err != nil {
			return schemaext.Objects{}, schemaext.Coverage{}, err
		}
		ref := mssqlschema.SecurityPolicyRef(row.schema, row.name)
		policy, seen := policies[ref.Key()]
		if !seen {
			policy = &mssqlschema.ObservedSecurityPolicy{Enabled: row.enabled, SchemaBinding: row.schemaBound,
				NotForReplication: row.notForReplication}
			policies[ref.Key()], refs[ref.Key()] = policy, ref
			order = append(order, ref.Key())
		}
		if !row.definition.Valid {
			continue
		}
		predicate, ok := row.predicate()
		if !ok {
			unreadable[ref.Key()] = true
			continue
		}
		policy.Predicates = append(policy.Predicates, predicate)
	}
	if err := rows.Err(); err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}

	objects := make([]schemaext.Object, 0, len(order))
	var subjects []schemaext.SubjectCoverage
	for _, key := range order {
		if unreadable[key] {
			subjects = append(subjects, schemaext.SubjectCoverage{Kind: mssqlschema.SecurityPolicyKind, Subject: refs[key],
				Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: UnreadablePredicateReason}})
			continue
		}
		object, err := mssqlschema.ObservedSecurityPolicyObject(refs[key], *policies[key])
		if err != nil {
			return schemaext.Objects{}, schemaext.Coverage{}, fmt.Errorf("security policy %s: %w", refs[key], err)
		}
		objects = append(objects, object)
	}
	described, err := schemaext.NewObjects(objects...)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	coverage, err := mssqlschema.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, subjects)
	return described, coverage, err
}

// securityPredicateRow is one row of the policy read: a policy, and one of its
// predicates where it has any.
type securityPredicateRow struct {
	schema, name                            string
	enabled, schemaBound, notForReplication bool
	tableSchema, table, definition, kind    sql.NullString
	operation                               sql.NullString
}

// predicate reads the row's predicate, and reports false for one the owner
// does not read (see [mssqlschema.CatalogPredicate]).
func (row securityPredicateRow) predicate() (mssqlschema.Predicate, bool) {
	predicate, err := mssqlschema.CatalogPredicate(row.tableSchema.String, row.table.String, row.definition.String,
		row.kind.String, row.operation.String)
	return predicate, err == nil
}
