package ydbplan

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbtype"
)

// TTLService plans TTL changes in place and accounts for the TTL through table
// removal and rebuild. Its zero value supports concurrent use without database
// access.
//
// `ALTER TABLE t SET (TTL = ...)` puts a TTL on a table and replaces the one it
// has, and `ALTER TABLE t RESET (TTL)` removes it, so a change is one
// statement. The host places it after the table's added columns, because a TTL
// may read a column the plan adds, and before its dropped ones, because YDB
// refuses to drop the column a TTL reads (`Can't drop TTL column: 'ts',
// disable TTL first`, measured on 25.1.4.7 and 26.2.1.14).
type TTLService struct{}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation. A successful reply
// must join the host's plan before any operation is rendered or executed.
func (TTLService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.YDB {
		return featureplan.Result{}, fmt.Errorf("%w: YDB TTL planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{ydbschema.TTLKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported YDB TTL parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, err := planTTLChange(request, record, i)
		if err != nil {
			return ttlRefusal(ydbdiff.TTLKind, err, new(i), nil), nil
		}
		result.Contributions = append(result.Contributions, contribution)
		result.Changes = append(result.Changes, featureplan.ChangePlan{
			Subject: record.Subject, Kind: ydbdiff.TTLKind,
			Strategy: "set or reset the TTL in place after column additions and before column removals",
			Steps:    []plangraph.StepID{contribution.Steps[0].ID},
		})
	}
	if len(request.ParentKinds) == 0 {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		return result, nil
	}
	for i, table := range request.Tables {
		if table.Action == "" {
			continue
		}
		strategy, err := assessTTLParent(request.Capabilities, table)
		if err != nil {
			return ttlRefusal(ydbschema.TTLKind, err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: ydbschema.TTLKind, Action: table.Action, Strategy: strategy})
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func ttlRefusal(kind schemaext.Kind, err error, change, parent *int) featureplan.Result {
	code := schemavalidation.UnsupportedFeature
	if errors.Is(err, schemaext.ErrInvalidValue) {
		code = schemavalidation.InvalidSchema
	}
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: code, Kind: string(kind), Feature: "YDB TTL planning", Message: err.Error()},
		Change:  change, Parent: parent,
	}}}
}

func planTTLChange(request featureplan.Request, record schemaext.ChangeRecord, index int) (plangraph.Contribution[featureplan.Operation], error) {
	var result plangraph.Contribution[featureplan.Operation]
	change, ok := record.Value.(*ydbdiff.TTL)
	if !ok {
		return result, fmt.Errorf("%w: expected a YDB TTL change", schemaext.ErrInvalidValue)
	}
	if err := ydbast.ValidateTTLChange(change); err != nil {
		return result, err
	}
	position := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject.Key() == record.Subject.Key() })
	if position < 0 {
		return result, fmt.Errorf("%w: a TTL change requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[position]
	if table.Action != "" && table.Action != featureplan.AlterTable {
		return result, fmt.Errorf("%w: a TTL change requires a table that survives the plan in place", schemaext.ErrInvalidValue)
	}
	if err := refuseTTLChange(request.Capabilities, table, change); err != nil {
		return result, err
	}
	result.Owner = ydbschema.Owner
	ttl := table.Subject
	ttl.Kind = objectidentity.Kind(ydbschema.TTLKind)
	payload := &ydbast.AlterTTL{Change: *change.CloneChange().(*ydbdiff.TTL)}
	result.Steps = []plangraph.Step[featureplan.Operation]{{
		// Zero-padded, because a scheduler orders independent steps by name and
		// the statements should keep the order of the changes.
		ID: plangraph.StepID{Owner: result.Owner, Name: fmt.Sprintf("ttl/%06d", index)},
		// No note: the statement names the table, as every other YDB table
		// change in the plan does. YDB runs no schema statement in a
		// transaction.
		Payload:     featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: payload},
		Transaction: plangraph.TransactionForbidden,
		Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: ttl, Action: plangraph.Alter}},
		Impact:      payload.Effect(),
	}}
	return result, nil
}

// refuseTTLChange refuses, before anything is emitted, a change YDB would
// refuse or that would lose a setting. An integer column's unit needs
// capability.RowDeletionPolicyEpochColumn. A run interval only the SDK and the
// CLI set is reset by `SET (TTL = ...)`, so a table whose read found one keeps
// its TTL until it is changed by hand. The column a TTL reads must be one the
// table declares, of a type YDB reads a TTL from, through the type map the
// table's columns are written with.
func refuseTTLChange(caps capability.Capabilities, table featureplan.Table, change *ydbdiff.TTL) error {
	subject := "the TTL of table " + quoted(ttlDisplayName(table.Subject))
	if !caps.Has(capability.RowDeletionPolicy) {
		return ttlKey(capability.RowDeletionPolicy, subject)
	}
	if change.After == nil {
		return nil
	}
	if err := refuseDeclaredTTL(caps, table.Desired, subject, change.After); err != nil {
		return err
	}
	if change.Before != nil && change.Before.RunIntervalSeconds != 0 {
		return ttlFact(subject, fmt.Sprintf("the table's TTL runs every %d seconds, which only the SDK and the CLI set, "+
			"and SET (TTL = ...) resets it to YDB's default. Change the TTL by hand with `ydb table ttl set`, "+
			"or reset it first", change.Before.RunIntervalSeconds))
	}
	return nil
}

// refuseDeclaredTTL refuses a declared TTL the target cannot write: an integer
// column's unit without capability.RowDeletionPolicyEpochColumn, and a column
// the table does not declare or of a type YDB reads no TTL from, through the
// type map the table's columns are written with.
func refuseDeclaredTTL(caps capability.Capabilities, declaration schemacapture.TableDeclaration, subject string, ttl *ydbschema.DesiredTTL) error {
	if strings.TrimSpace(ttl.Policy.Unit) != "" && !caps.Has(capability.RowDeletionPolicyEpochColumn) {
		return ttlKey(capability.RowDeletionPolicyEpochColumn, subject+" reads an integer column counting "+ttl.Policy.Unit)
	}
	if !declaration.HasTable() {
		return nil
	}
	types := make(map[string]string, len(declaration.Fields))
	for _, field := range declaration.Fields {
		// A type the map refuses is the column's refusal, which is reported
		// where the column is written.
		if mapping, err := ydbtype.Map(field.Type, caps); err == nil {
			types[field.Name] = mapping.Type
		} else {
			types[field.Name] = ""
		}
	}
	if ydbType, found := types[ttl.Policy.Column]; found && ydbType == "" {
		return nil
	}
	if reason := ydbschema.TTLColumnRefusal(ttl.Policy, types); reason != "" {
		return ttlFact(subject, reason)
	}
	return nil
}

func ttlKey(key capability.Capability, subject string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: string(key), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target", subject, key, platform.YDB)}
}

func ttlFact(subject, reason string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: subject, Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: %s", subject, reason)}
}

// assessTTLParent accounts for the TTL through a table operation. Dropping a
// table removes its TTL with it, and a rebuilt table's CREATE TABLE writes the
// declared TTL, which is held to the target here so the plan refuses before
// any statement rather than when the rebuild is rendered. A table that
// survives keeps its TTL unless a change in the same plan replaces it.
func assessTTLParent(caps capability.Capabilities, table featureplan.Table) (string, error) {
	switch table.Action {
	case featureplan.DropTable:
		return "remove the TTL with the table", nil
	case featureplan.RebuildTable:
		declared, _, err := schemaext.FacetAs[*ydbschema.DesiredTTL](table.Desired.Table.Facets, ydbschema.TTLKind)
		if err != nil || declared == nil {
			return "write the declared TTL into the rebuilt table", err
		}
		subject := "the TTL of table " + quoted(ttlDisplayName(table.Subject))
		if !caps.Has(capability.RowDeletionPolicy) {
			return "", ttlKey(capability.RowDeletionPolicy, subject)
		}
		if err := refuseDeclaredTTL(caps, table.Desired, subject, declared); err != nil {
			return "", err
		}
		return "write the declared TTL into the rebuilt table", nil
	case featureplan.AlterTable:
		return "retain the TTL unless a planned change in this plan replaces it", nil
	default:
		return "", fmt.Errorf("%w: YDB TTL has no plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
	}
}

func ttlDisplayName(subject objectidentity.ID) string {
	if subject.Schema.Empty() || subject.Schema.Defaulted {
		return subject.Name.Source
	}
	return subject.Schema.Source + "/" + subject.Name.Source
}

func quoted(name string) string { return fmt.Sprintf("%q", name) }
