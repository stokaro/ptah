package integrationharness

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/dbschema"
)

// rowSecurityState is what a PostgreSQL database holds for the row-level
// security scenarios, read from its catalog in the current schema.
//
// The scenarios assert on this rather than on the schema parsed from the
// fixture: the parsed schema states what the fixture declares, which holds
// whether or not a migration created it. In the parsed schema, row-level
// security belongs to the PostgreSQL row-security owner, so the shared
// RLSPolicies and RLSEnabledTables fields stay empty for these fixtures.
type rowSecurityState struct {
	// policies maps each policy name to the command it applies to: ALL,
	// SELECT, INSERT, UPDATE or DELETE.
	policies map[string]string
	// enabledTables names the tables with row-level security enabled.
	enabledTables []string
	// functions names the functions, one entry for each overload.
	functions []string
}

// readRowSecurityState reads the policies, the tables with row-level security
// enabled and the functions of the current schema.
func readRowSecurityState(ctx context.Context, conn *dbschema.DatabaseConnection) (rowSecurityState, error) {
	policies, err := readPolicyCommands(ctx, conn)
	if err != nil {
		return rowSecurityState{}, fmt.Errorf("read pg_policies: %w", err)
	}
	enabled, err := readNames(ctx, conn, `
		SELECT c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relrowsecurity`)
	if err != nil {
		return rowSecurityState{}, fmt.Errorf("read the tables with row-level security enabled: %w", err)
	}
	functions, err := readNames(ctx, conn, `
		SELECT p.proname
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = current_schema() AND p.prokind = 'f'`)
	if err != nil {
		return rowSecurityState{}, fmt.Errorf("read pg_proc: %w", err)
	}
	return rowSecurityState{policies: policies, enabledTables: enabled, functions: functions}, nil
}

// readPolicyCommands maps each policy of the current schema to its command.
func readPolicyCommands(ctx context.Context, conn *dbschema.DatabaseConnection) (map[string]string, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT policyname, cmd
		FROM pg_policies
		WHERE schemaname = current_schema()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	policies := make(map[string]string)
	for rows.Next() {
		var name, command string
		if err := rows.Scan(&name, &command); err != nil {
			return nil, err
		}
		policies[name] = command
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return policies, nil
}

// readNames runs a query returning one text column.
func readNames(ctx context.Context, conn *dbschema.DatabaseConnection, query string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

// expectPolicies reports an error unless the database holds exactly the
// policies named in want.
func (s rowSecurityState) expectPolicies(want ...string) error {
	return expectNames("RLS policies", slices.Collect(maps.Keys(s.policies)), want)
}

// expectEnabledTables reports an error unless row-level security is enabled
// on exactly the tables named in want.
func (s rowSecurityState) expectEnabledTables(want ...string) error {
	return expectNames("RLS enabled tables", s.enabledTables, want)
}

// expectFunctions reports an error unless the database holds exactly the
// functions named in want.
func (s rowSecurityState) expectFunctions(want ...string) error {
	return expectNames("functions", s.functions, want)
}

// expectPolicyCommand reports an error unless the database holds the policy
// name and it applies to command.
func (s rowSecurityState) expectPolicyCommand(name, command string) error {
	got, ok := s.policies[name]
	if !ok {
		return fmt.Errorf("expected RLS policy %s in the database", name)
	}
	if got != command {
		return fmt.Errorf("RLS policy %s should be FOR %s, got %s", name, command, got)
	}
	return nil
}

// expectNames reports an error unless got and want hold the same names, in
// any order.
func expectNames(what string, got, want []string) error {
	got = slices.Sorted(slices.Values(got))
	want = slices.Sorted(slices.Values(want))
	if !slices.Equal(got, want) {
		return fmt.Errorf("expected %s %v in the database, got %v", what, want, got)
	}
	return nil
}
