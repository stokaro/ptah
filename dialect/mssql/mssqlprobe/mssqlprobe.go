// Package mssqlprobe asks a connected SQL Server to spell the predicates of
// declared security policies the way sys.security_predicates stores them,
// for the owner of package mssqlschema, so a comparison holds the server's
// spelling on both sides. Every probe runs inside a rolled-back transaction
// of the session it is given; nothing it creates survives the call.
package mssqlprobe

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlrender"
	"ptah.run/dialect/mssql/mssqlschema"
)

// predicatesQuery reads back the predicates of the probe policy.
const predicatesQuery = `SELECT ts.name, t.name, sp.predicate_definition, sp.predicate_type_desc, ISNULL(sp.operation_desc, '')
FROM sys.security_predicates AS sp
JOIN sys.objects AS t ON t.object_id = sp.target_object_id
JOIN sys.schemas AS ts ON ts.schema_id = t.schema_id
WHERE sp.object_id = OBJECT_ID(@p1)`

// savepoint isolates one probe inside the session's transaction, so a probe
// the server refuses leaves the others to run.
const savepoint = "ptah_policy_probe"

// Service attaches the server's spelling of each declared security policy
// whose comparison with the policy the database holds is undecided. Its zero
// value is ready for concurrent use.
//
// SQL Server stores a predicate's arguments rewritten: measured on SQL Server
// 2025, `CAST(tenant AS int) + 0` is stored as `CONVERT([int],[tenant])+(0)`.
// An offline comparison cannot tell such a rewrite from another expression
// (see [mssqlschema.CompareArgument]), so without the server's spelling a
// declaration nobody changed stays undecided on every run.
//
// The declaration is put through the same server: the probe creates it with
// the statement a plan would run ([mssqlrender.CreateStatement]), turned off
// so it does not collide with the enabled policy on the same tables, under a
// name of its own in the declared policy's schema, and reads
// sys.security_predicates back. A probe the server refuses, such as one whose
// function the plan has not created yet, leaves its declaration without an
// answer, and a comparison then stays undecided. Only declarations whose
// comparison is undecided are probed: one being created carries its arguments
// into the statement unchanged, and one that agrees or differs needs no
// server.
type Service struct{}

type probe struct {
	object schemaext.Object
	value  *mssqlschema.DesiredSecurityPolicy
}

// NormalizeObjects returns every declared policy, with
// [mssqlschema.DesiredSecurityPolicy.Normalized] attached to those the server
// spelled. A session that could not open an isolated transaction leaves every
// declaration unanswered.
func (Service) NormalizeObjects(ctx context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
	if ctx == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: normalization requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.NormalizationResult{}, err
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: SQL Server security policy normalization on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if request.Session == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: normalization requires a probe session", schemaext.ErrInvalidValue)
	}
	probes, err := undecidedProbes(request)
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	result := schemaext.NormalizationResult{Complete: true, Desired: request.Desired}
	if len(probes) == 0 {
		return result, nil
	}
	spelled := make([][]mssqlschema.Predicate, len(probes))
	ran, err := request.Session.WithRolledBackTransaction(ctx, "resolve security policy predicates", func(ctx context.Context, tx *sql.Tx) error {
		for i, probe := range probes {
			if err := ctx.Err(); err != nil {
				return err
			}
			answer, ok, err := spell(ctx, tx, request, probe)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			spelled[i] = answer
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
		if spelled[i] == nil {
			continue
		}
		value := probe.value.Copy()
		value.Normalized = spelled[i]
		result.Desired.Objects, err = result.Desired.Objects.Replace(schemaext.Object{Ref: probe.object.Ref, Value: value, Targets: probe.object.Targets})
		if err != nil {
			return schemaext.NormalizationResult{}, err
		}
	}
	return result, ctx.Err()
}

// undecidedProbes selects the declarations the database holds a policy for
// that [mssqlschema.ComparePolicy] cannot decide offline, paired under the
// request's identifier rules.
func undecidedProbes(request schemaext.NormalizationRequest) ([]probe, error) {
	held := make(map[objectidentity.Key]*mssqlschema.ObservedSecurityPolicy)
	current, err := request.Current.Objects.All()
	if err != nil {
		return nil, err
	}
	for _, object := range current {
		if value, ok := object.Value.(*mssqlschema.ObservedSecurityPolicy); ok {
			held[identity(request, object.Ref).Key()] = value
		}
	}
	desired, err := request.Desired.Objects.All()
	if err != nil {
		return nil, err
	}
	var probes []probe
	for _, object := range desired {
		value, ok := object.Value.(*mssqlschema.DesiredSecurityPolicy)
		if !ok {
			return nil, fmt.Errorf("%w: unexpected SQL Server security policy declaration %T", schemaext.ErrInvalidValue, object.Value)
		}
		observed, found := held[identity(request, object.Ref).Key()]
		if !found {
			continue
		}
		if agreement, _ := mssqlschema.ComparePolicy(request.Identifiers, value, observed); agreement == mssqlschema.Undecided {
			probes = append(probes, probe{object: object, value: value})
		}
	}
	return probes, nil
}

// identity is ref under the request's identifier rules.
func identity(request schemaext.NormalizationRequest, ref objectidentity.ID) objectidentity.ID {
	return mssqlschema.SecurityPolicyRefWith(request.Identifiers, ref.Schema.Source, ref.Name.Source)
}

// spell creates the declaration turned off under a probe name, reads its
// predicates back and rolls the creation back to a savepoint. It answers
// false, with a nil error, for a statement the server refuses or a predicate
// it reports in a shape the owner does not read; it errs only when the
// transaction cannot be returned to the savepoint.
func spell(ctx context.Context, tx *sql.Tx, request schemaext.NormalizationRequest, probe probe) ([]mssqlschema.Predicate, bool, error) {
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return nil, false, err
	}
	name := mssqlschema.ObjectName{Schema: probe.object.Ref.Schema.Source, Name: "ptah_probe_" + hex.EncodeToString(suffix)}
	off := false
	declared := probe.value.Copy()
	declared.Enabled, declared.Normalized = &off, nil
	if _, err := tx.ExecContext(ctx, "SAVE TRANSACTION "+savepoint); err != nil {
		return nil, false, err
	}
	answer, ok := create(ctx, tx, request, name, declared)
	if _, err := tx.ExecContext(ctx, "ROLLBACK TRANSACTION "+savepoint); err != nil {
		return nil, false, fmt.Errorf("security policy probe: return to the savepoint: %w", err)
	}
	return answer, ok, nil
}

// create runs the probe statement and reads the predicates the server stored,
// each in the slot of the declared predicate it spells.
func create(ctx context.Context, tx *sql.Tx, request schemaext.NormalizationRequest, name mssqlschema.ObjectName,
	declared *mssqlschema.DesiredSecurityPolicy,
) ([]mssqlschema.Predicate, bool) {
	if _, err := tx.ExecContext(ctx, mssqlrender.CreateStatement(name, declared)); err != nil {
		return nil, false
	}
	rows, err := tx.QueryContext(ctx, predicatesQuery, name.String())
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	var stored []mssqlschema.Predicate
	for rows.Next() {
		var tableSchema, table, definition, kind, operation string
		if err := rows.Scan(&tableSchema, &table, &definition, &kind, &operation); err != nil {
			return nil, false
		}
		predicate, err := mssqlschema.CatalogPredicate(tableSchema, table, definition, kind, operation)
		if err != nil {
			return nil, false
		}
		stored = append(stored, predicate)
	}
	if rows.Err() != nil || len(stored) != len(declared.Predicates) {
		return nil, false
	}
	spelled := make([]mssqlschema.Predicate, 0, len(declared.Predicates))
	for _, want := range declared.Predicates {
		var found bool
		for _, got := range stored {
			if mssqlschema.SameSlot(request.Identifiers, want, got) {
				want.Function, want.Arguments, found = got.Function, got.Arguments, true
				break
			}
		}
		if !found {
			return nil, false
		}
		spelled = append(spelled, want)
	}
	return spelled, true
}
