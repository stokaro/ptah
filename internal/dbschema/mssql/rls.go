package mssql

import (
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// readRLSPolicies reads the security policies the connected database declares,
// one entry per (policy, target table) pair.
//
// The catalog makes this feature reachable without a T-SQL parser, and that is
// worth stating because the neighbouring routine path is blocked on exactly
// the opposite. sys.sql_modules.definition returns a function's whole original
// `CREATE FUNCTION` text as one string, which is why capability.Functions is
// still false here. sys.security_predicates instead returns the predicate as an
// expression on its own:
//
//	policy | is_enabled | predicate_definition          | type   | operation    | target
//	dbo.p  | 1          | ([dbo].[fn_tenant]([tenant])) | FILTER | NULL         | t_rls
//	dbo.p  | 1          | ([dbo].[fn_tenant]([tenant])) | BLOCK  | AFTER UPDATE | t_rls
//
// Measured on SQL Server 2025 (RTM-CU8), 17.0.4075.5.
//
// A SQL Server policy may carry predicates for several tables, while the
// declaration model is one policy per table. The rows are grouped by target so
// each pair becomes one [types.DBRLSPolicy], which is the shape the comparator
// reads; a policy naming three tables therefore reads back as three entries
// carrying the same name.
func (r *Reader) readRLSPolicies() ([]types.DBRLSPolicy, error) {
	query := `
		SELECT s.name, p.name, t.name, sp.predicate_definition,
			   sp.predicate_type_desc, ISNULL(sp.operation_desc, '')
		FROM sys.security_policies AS p
		JOIN sys.schemas AS s ON s.schema_id = p.schema_id
		JOIN sys.security_predicates AS sp ON sp.object_id = p.object_id
		JOIN sys.objects AS t ON t.object_id = sp.target_object_id
		JOIN sys.schemas AS ts ON ts.schema_id = t.schema_id
		WHERE p.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, p.name, t.name, sp.security_predicate_id`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// The grouping key is the pair, not the policy: two predicates on one table
	// are the filter and the block halves of a single declared policy, and two
	// tables under one policy are two declarations that happen to share a name.
	type pairKey struct{ policy, table string }
	order := []pairKey{}
	byPair := map[pairKey]*types.DBRLSPolicy{}

	for rows.Next() {
		var policySchema, policyName, tableName, definition, predicateType, operation string
		if err := rows.Scan(&policySchema, &policyName, &tableName,
			&definition, &predicateType, &operation); err != nil {
			return nil, err
		}
		// The name is reported bare, like the table name beside it. The schema
		// is already the one this read was scoped to, and a qualified name here
		// would never match a declaration, which names a policy and a table and
		// carries no schema of its own.
		_ = policySchema
		key := pairKey{policy: policyName, table: tableName}
		policy, seen := byPair[key]
		if !seen {
			policy = &types.DBRLSPolicy{Name: policyName, Table: tableName}
			byPair[key] = policy
			order = append(order, key)
		}
		if strings.EqualFold(predicateType, "BLOCK") {
			policy.WithCheckExpression = normalizePredicateDefinition(definition)
			policy.PolicyFor = blockOperationPolicyFor(operation)
			continue
		}
		policy.UsingExpression = normalizePredicateDefinition(definition)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	policies := make([]types.DBRLSPolicy, 0, len(order))
	for _, key := range order {
		policies = append(policies, *byPair[key])
	}
	return policies, nil
}

// blockOperationPolicyFor maps the operation a BLOCK predicate reports onto the
// FOR clause the declaration model spells.
//
// A block predicate carrying no operation covers all four, which is what ALL
// means, and that is also what the catalog reports as an empty operation.
func blockOperationPolicyFor(operation string) string {
	switch strings.ToUpper(strings.TrimSpace(operation)) {
	case "AFTER INSERT":
		return "INSERT"
	case "AFTER UPDATE", "BEFORE UPDATE":
		return "UPDATE"
	case "BEFORE DELETE":
		return "DELETE"
	default:
		return "ALL"
	}
}

// applyRLSEnabled marks every table an enabled security policy filters.
//
// PostgreSQL stores this as a table attribute and its reader scans it straight
// out of pg_class. SQL Server has no such attribute -- the state lives on the
// policy -- so the flag is derived: a table is under row-level security exactly
// when some enabled policy carries a predicate on it. Deriving it rather than
// leaving it false is what lets the comparator see the same fact on both
// engines instead of reporting a permanent difference on this one.
func applyRLSEnabled(schema *types.DBSchema, enabled map[string]bool) {
	for i := range schema.Tables {
		if enabled[schema.Tables[i].Name] {
			schema.Tables[i].RLSEnabled = true
		}
	}
}

// readRLSEnabledTables reports which tables an ENABLED policy filters.
//
// The state matters and is read rather than assumed: `WITH (STATE = OFF)` is
// accepted at creation, and a disabled policy filters nothing. Reporting a
// table as protected because a dormant policy names it would be the kind of
// false all-clear this reader exists to avoid.
func (r *Reader) readRLSEnabledTables() (map[string]bool, error) {
	query := `
		SELECT DISTINCT t.name
		FROM sys.security_policies AS p
		JOIN sys.schemas AS s ON s.schema_id = p.schema_id
		JOIN sys.security_predicates AS sp ON sp.object_id = p.object_id
		JOIN sys.objects AS t ON t.object_id = sp.target_object_id
		WHERE p.is_ms_shipped = 0 AND p.is_enabled = 1
			  AND (` + schemaPredicatePlaceholder + `)`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	enabled := map[string]bool{}
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		enabled[table] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return enabled, nil
}

// normalizePredicateDefinition renders the catalog's predicate text in the
// spelling a declaration uses.
//
// sys.security_predicates returns the predicate fully bracketed and wrapped:
// a policy created from `dbo.fn_tenant(tenant)` reads back as
// `([dbo].[fn_tenant]([tenant]))`. The shared comparator already strips
// redundant outer parentheses, so that half converges on its own; the brackets
// do not, and a policy left carrying them reports using_expression as changed
// on every run forever while the plan it plans plans nothing new.
//
// The folding is done here rather than in the comparator because a bracket is
// only quoting in T-SQL. In PostgreSQL it is array syntax, and a shared rule
// that erased it would make `id = ANY(tenants[1:2])` compare equal to something
// it is not.
//
// It is a read-side normalization only. What this renderer emits comes from the
// declaration, never from a value that passed through here, so folding for
// comparison cannot write Ptah's spelling into anyone's DDL.
func normalizePredicateDefinition(definition string) string {
	folded := strings.TrimSpace(definition)
	for len(folded) > 1 && folded[0] == '(' && folded[len(folded)-1] == ')' && balancedOutside(folded) {
		folded = strings.TrimSpace(folded[1 : len(folded)-1])
	}
	return strings.NewReplacer("[", "", "]", "").Replace(folded)
}

// balancedOutside reports whether the first parenthesis closes only at the very
// end, which is what makes it redundant rather than one of a pair enclosing an
// argument list.
func balancedOutside(value string) bool {
	depth := 0
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(value)-1 {
				return false
			}
		}
	}
	return depth == 0
}
