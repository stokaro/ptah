// Package policyprobe asks a connected PostgreSQL server to normalize declared
// row-security policies for the owner of package pgpolicy, so a comparison
// holds the server's spelling on both sides. Every probe runs inside a
// rolled-back transaction of the session it is given, on a temporary table;
// nothing it creates survives the call, and it never locks the real table
// beyond the read its column copy takes.
package policyprobe

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyrender"
	"ptah.run/internal/sqlident"
)

// Service attaches the server's spelling of each declared policy the database
// also holds. Its zero value is ready for concurrent use.
//
// PostgreSQL stores a policy's clauses as parse trees, and the cast it inserts
// depends on the type of the column a clause names: measured on 17.11,
// `owner = 'x'` is stored as `((owner)::text = 'x'::text)` over a varchar
// column and unchanged over text. It also records the role a CURRENT_ROLE,
// CURRENT_USER or SESSION_USER keyword resolved to. A declaration compared as
// written would drop and recreate a policy nobody changed on every run, which
// takes a security control away and puts it back (stokaro/ptah#2049).
//
// The declaration is put through the same server: a temporary copy of the
// policy's table is created with CREATE TEMPORARY TABLE ... (LIKE ...), which
// copies the column types, the declared policy is created on it with the
// statement a plan would run, and pg_policy is read back. The copy takes the
// real table's name inside pg_temp, so a clause that names its own table
// resolves, and pg_temp goes last in the search path, so every other name
// resolves as it does for the real policy.
//
// A probe the server refuses leaves its declaration without an answer and is
// not an error: a read-only transaction refuses the temporary table, a role
// without SELECT on the table refuses the column copy, a role the plan has not
// created yet refuses the policy, and a comparison then compares the declared
// text. Only policies the database holds and whose clauses or roles the server
// rewrites are probed; one being created carries its declaration into the
// CREATE statement unchanged.
type Service struct{}

type probe struct {
	object schemaext.Object
	value  *pgpolicy.DesiredPolicy
}

// NormalizeObjects returns every declared policy, with Normalized attached to
// those the server answered for. A session that could not open an isolated
// transaction answers nothing, and the declarations come back unchanged, as
// does a declaration whose answer the model cannot hold.
func (Service) NormalizeObjects(ctx context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
	if ctx == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: normalization requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.NormalizationResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: PostgreSQL row-security normalization on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if request.Session == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: normalization requires a probe session", schemaext.ErrInvalidValue)
	}
	probes, err := heldProbes(request)
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	result := schemaext.NormalizationResult{Complete: true, Desired: request.Desired}
	if len(probes) == 0 {
		return result, nil
	}
	answers := make([]*pgpolicy.NormalizedPolicy, len(probes))
	ran, err := request.Session.WithRolledBackTransaction(ctx, "resolve row-security policies",
		func(ctx context.Context, tx *sql.Tx) error {
			for i, probe := range probes {
				answer, err := normalizeOne(ctx, tx, probe)
				if err != nil {
					return err
				}
				answers[i] = answer
			}
			return nil
		})
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	if !ran {
		return result, nil
	}
	for i, probe := range probes {
		if answers[i] == nil {
			continue
		}
		value := probe.value.Copy()
		value.Normalized = answers[i]
		// An answer the model cannot hold is a server this probe does not
		// understand, which is unanswered rather than a failed comparison.
		if pgpolicy.ValidateDesiredPolicy(value) != nil {
			continue
		}
		result.Desired.Objects, err = result.Desired.Objects.Replace(schemaext.Object{Ref: probe.object.Ref, Value: value})
		if err != nil {
			return schemaext.NormalizationResult{}, err
		}
	}
	return result, ctx.Err()
}

// heldProbes selects the declarations whose policy the database holds, by
// identity, and whose clauses or roles
// the server rewrites. A policy with neither is compared as declared.
func heldProbes(request schemaext.NormalizationRequest) ([]probe, error) {
	held := make(map[objectidentity.Key]bool)
	for _, ref := range request.Current.Objects.Refs() {
		held[ref.Key()] = true
	}
	objects, err := request.Desired.Objects.All()
	if err != nil {
		return nil, err
	}
	var probes []probe
	for _, object := range objects {
		value, ok := object.Value.(*pgpolicy.DesiredPolicy)
		if !ok {
			return nil, fmt.Errorf("%w: unexpected row-security policy declaration %T", schemaext.ErrInvalidValue, object.Value)
		}
		if err := pgpolicy.ValidatePolicyRef(object.Ref); err != nil {
			return nil, err
		}
		rewritten := value.Using != nil || value.WithCheck != nil || slices.ContainsFunc(value.EffectiveRoles(), func(role pgpolicy.RoleSelector) bool {
			return role.Keyword != "" && role.Keyword != pgpolicy.Public
		})
		if held[object.Ref.Key()] && rewritten {
			probes = append(probes, probe{object: object, value: value})
		}
	}
	return probes, nil
}

// searchPathWithTempLast puts pg_temp at the end of the transaction's search
// path, so the probe table shadows nothing a clause reads. set_config's third
// argument makes it local, and the savepoint rollback undoes it with the rest
// of the probe.
const searchPathWithTempLast = `SELECT set_config('search_path', CASE
	WHEN current_setting('search_path') = '' THEN 'pg_temp'
	ELSE current_setting('search_path') || ', pg_temp' END, true)`

// readBack reads what pg_policy stores for the probe policy: the role list as
// a reader reports it, PUBLIC or names, and both clauses.
const readBack = `
	SELECT 0 = ANY(p.polroles),
	       (SELECT COALESCE(json_agg(r.rolname ORDER BY r.rolname), '[]'::json)::text
	        FROM pg_roles r WHERE r.oid = ANY(p.polroles)),
	       pg_get_expr(p.polqual, p.polrelid),
	       pg_get_expr(p.polwithcheck, p.polrelid)
	FROM pg_policy p
	WHERE p.polrelid = $1::regclass`

const savepoint = "ptah_policy_probe"

// normalizeOne creates one probe table and policy inside a savepoint and reads
// the stored policy back. A statement the server refuses rolls back to the
// savepoint and answers nil, so one refused declaration does not take the
// rest with it.
func normalizeOne(ctx context.Context, tx *sql.Tx, probe probe) (*pgpolicy.NormalizedPolicy, error) {
	schema := sqlident.Quote(platform.Postgres, probe.object.Ref.Schema.Source)
	table := sqlident.Quote(platform.Postgres, probe.object.Ref.Parent.Source)
	copied := "pg_temp." + table
	statements := []string{
		searchPathWithTempLast,
		fmt.Sprintf("CREATE TEMPORARY TABLE %s (LIKE %s.%s)", copied, schema, table),
		strings.TrimSuffix(policyrender.CreateStatement(savepoint, copied, probe.value), ";"),
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return nil, fmt.Errorf("resolve row-security policies: savepoint: %w", err)
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
				return nil, fmt.Errorf("resolve row-security policies: roll back to savepoint after %s: %w", probe.object.Ref, rollbackErr)
			}
			return nil, nil
		}
	}
	var (
		public           bool
		names            string
		using, withCheck sql.NullString
	)
	if err := tx.QueryRowContext(ctx, readBack, copied).Scan(&public, &names, &using, &withCheck); err != nil {
		return nil, fmt.Errorf("resolve row-security policies: read back %s: %w", probe.object.Ref, err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); err != nil {
		return nil, fmt.Errorf("resolve row-security policies: release probe: %w", err)
	}
	answer := &pgpolicy.NormalizedPolicy{Using: text(using), WithCheck: text(withCheck)}
	if public {
		answer.Roles = []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}
		return answer, nil
	}
	var roles []string
	if err := json.Unmarshal([]byte(names), &roles); err != nil {
		return nil, fmt.Errorf("resolve row-security policies: read back the roles of %s: %w", probe.object.Ref, err)
	}
	for _, role := range roles {
		answer.Roles = append(answer.Roles, pgpolicy.RoleSelector{Name: role})
	}
	return answer, nil
}

func text(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}
