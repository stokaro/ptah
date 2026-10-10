package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// rowSecurityQuery reads one schema's policies in the shape the row-security
// owner observes them. Role 0 is PUBLIC, which has no pg_roles row; a list
// that names it applies to everyone whatever else it names. PostgreSQL stores
// such a list as {0} alone, while CockroachDB 26.3 keeps {0, app}, so the
// query reports PUBLIC alone for either. A clause the policy does not have is
// NULL rather than an empty expression.
const rowSecurityQuery = `
	SELECT
		c.relname AS table_name,
		pol.polname AS policy_name,
		pol.polcmd::text AS command,
		CASE
			WHEN 0 = ANY(pol.polroles) THEN NULL
			ELSE (SELECT COALESCE(json_agg(rolname ORDER BY rolname), '[]'::json)::text
				FROM pg_roles WHERE oid = ANY(pol.polroles))
		END AS roles,
		pg_get_expr(pol.polqual, pol.polrelid) AS using_expression,
		pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check_expression,
		COALESCE(obj_description(pol.oid, 'pg_policy'), '') AS comment,
		pol.polpermissive AS permissive
	FROM pg_policy pol
	JOIN pg_class c ON c.oid = pol.polrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1
	ORDER BY c.relname, pol.polname`

// compositions maps pg_policy.polpermissive to the composition it stands for.
var compositions = map[bool]pgpolicy.Composition{true: pgpolicy.Permissive, false: pgpolicy.Restrictive}

// policyCommands maps pg_policy.polcmd to the command it stands for.
var policyCommands = map[string]pgpolicy.Command{
	"*": pgpolicy.CommandAll, "r": pgpolicy.CommandSelect, "a": pgpolicy.CommandInsert,
	"w": pgpolicy.CommandUpdate, "d": pgpolicy.CommandDelete,
}

// readRowSecurity reports the read's row-level security to the PostgreSQL
// row-security owner: each policy as an observed object, and the two switches
// of each table that has either on as an observed facet. The read asked the
// catalog about every policy and table in scope, so it claims complete
// knowledge of both: a table without the facet has both switches off.
func (r *Reader) readRowSecurity(ctx context.Context, schema *catalog.Database) error {
	for index := range schema.Tables {
		table := &schema.Tables[index]
		if table.RLSEnabled || table.RLSForced {
			facets, err := table.Facets.With(&pgpolicy.ObservedTableState{Enabled: table.RLSEnabled, Forced: table.RLSForced})
			if err != nil {
				return err
			}
			table.Facets = facets
		}
		table.RLSEnabled, table.RLSForced = false, false
	}
	for _, schemaName := range r.schemasToRead() {
		if err := r.readPolicies(ctx, schema, schemaName); err != nil {
			return fmt.Errorf("failed to read row-level security policies for schema %s: %w", schemaName, err)
		}
	}
	known, err := pgpolicy.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return err
	}
	schema.FeatureCoverage, err = schema.FeatureCoverage.Combine(known)
	return err
}

func (r *Reader) readPolicies(ctx context.Context, schema *catalog.Database, schemaName string) error {
	rows, err := r.db.QueryContext(ctx, rowSecurityQuery, schemaName)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var table, name, command, comment string
		var roles, using, withCheck sql.NullString
		var permissive bool
		if err := rows.Scan(&table, &name, &command, &roles, &using, &withCheck, &comment, &permissive); err != nil {
			return err
		}
		policy, err := scannedPolicy(command, roles, using, withCheck, comment, compositions[permissive])
		if err != nil {
			return fmt.Errorf("policy %q on table %q: %w", name, table, err)
		}
		object, err := pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef(r.outputSchema(schemaName), table, name), policy)
		if err != nil {
			return fmt.Errorf("policy %q on table %q: %w", name, table, err)
		}
		if schema.FeatureObjects, err = schema.FeatureObjects.With(object); err != nil {
			return err
		}
	}
	return rows.Err()
}

func scannedPolicy(command string, roles, using, withCheck sql.NullString, comment string, composition pgpolicy.Composition) (pgpolicy.ObservedPolicy, error) {
	policy := pgpolicy.ObservedPolicy{Command: policyCommands[command], Comment: comment, Composition: composition}
	if using.Valid {
		policy.Using = new(using.String)
	}
	if withCheck.Valid {
		policy.WithCheck = new(withCheck.String)
	}
	if !roles.Valid {
		policy.Roles = []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}
		return policy, nil
	}
	var names []string
	if err := json.Unmarshal([]byte(roles.String), &names); err != nil {
		return pgpolicy.ObservedPolicy{}, fmt.Errorf("read role list: %w", err)
	}
	for _, name := range names {
		policy.Roles = append(policy.Roles, pgpolicy.RoleSelector{Name: name})
	}
	return policy, nil
}

// clearRowSecuritySwitches drops the switches a table read reported when the
// read does not report row-level security: the shared model no longer holds
// them for this family, and the owner's facet needs the catalog read the
// capability gates.
func clearRowSecuritySwitches(tables []catalog.Table) {
	for index := range tables {
		tables[index].RLSEnabled, tables[index].RLSForced = false, false
	}
}
