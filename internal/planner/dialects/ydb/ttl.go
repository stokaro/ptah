package ydb

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbttl"
	"ptah.run/internal/ydbtype"
	"ptah.run/migration/schemadiff/difftypes"
)

// How a YDB plan changes a table's TTL, its row deletion policy.
//
// `ALTER TABLE t SET (TTL = ...)` puts a TTL on a table and replaces the one it
// has, and `ALTER TABLE t RESET (TTL)` removes it, so a change is one
// statement either way. The statement goes after the table's added columns,
// because a TTL may read a column the plan adds, and before its dropped ones,
// because YDB refuses to drop the column a TTL reads (`Can't drop TTL column:
// 'ts', disable TTL first`, measured on 25.1.4.7 and 26.2.1.14) and drops it
// once the TTL reads another column or none.

// ttlOperation is the operation that takes a table's TTL from change.Current
// to change.Desired, or nil when the table's TTL does not change.
func ttlOperation(change *difftypes.RowDeletionPolicyChange) ast.AlterOperation {
	switch {
	case change == nil:
		return nil
	case change.Desired.IsZero():
		return &ast.DropRowDeletionPolicyOperation{}
	default:
		return &ast.SetRowDeletionPolicyOperation{
			Column:   change.Desired.Column,
			Interval: change.Desired.Interval,
			Unit:     change.Desired.Unit,
			Replace:  !change.Current.IsZero(),
		}
	}
}

// refuseTTLKey refuses a TTL change on a target without
// [capability.RowDeletionPolicy]. A table the plan rebuilds needs no more: it
// writes its TTL into the new CREATE TABLE, which the renderer checks whole.
func (p *Planner) refuseTTLKey(tableDiff difftypes.TableDiff) error {
	if tableDiff.RowDeletionPolicyChange == nil || p.caps.Has(capability.RowDeletionPolicy) {
		return nil
	}
	return refuseKey(capability.RowDeletionPolicy, ttlSubject(tableDiff))
}

// ttlSubject names a table's TTL in a refusal.
func ttlSubject(tableDiff difftypes.TableDiff) string {
	return fmt.Sprintf("the row deletion policy of table %q", tableDiff.TableName)
}

// refuseTTLChange refuses, before anything is emitted, a TTL change on a table
// changed in place that this target cannot make or YDB would refuse when the
// plan reaches it: a policy on a target without
// [capability.RowDeletionPolicy], an integer column's unit without
// [capability.RowDeletionPolicyEpochColumn], an interval or unit YDB refuses,
// and a column of a type YDB reads no TTL from. The renderer writes SET (TTL =
// ...) without seeing the column's type, so this is where the type is held
// to [ydbttl.ColumnRefusal].
//
// The table keeps what its TTL carries beyond the policy, which SET (TTL =
// ...) would reset, so a change is refused while the read recorded such a
// setting.
func (p *Planner) refuseTTLChange(tableDiff difftypes.TableDiff, notDescribed coverage.Set) error {
	if err := p.refuseTTLKey(tableDiff); err != nil {
		return err
	}
	change := tableDiff.RowDeletionPolicyChange
	if change == nil {
		return refuseDroppingTheTTLColumn(tableDiff)
	}
	if change.Desired.IsZero() {
		return nil
	}
	subject, desired := ttlSubject(tableDiff), change.Desired
	if strings.TrimSpace(desired.Unit) != "" && !p.caps.Has(capability.RowDeletionPolicyEpochColumn) {
		return refuseKey(capability.RowDeletionPolicyEpochColumn, subject+" reads an integer column counting "+desired.Unit)
	}
	if err := ydbttl.Validate(desired); err != nil {
		return refuseFact(subject, err.Error())
	}
	if !change.Current.IsZero() && tableDiff.Desired.HasTable() &&
		recordsSetting(notDescribed, coverage.TTL, tableDiff.Desired.Table) {
		return refuseFact(subject, "the table's TTL carries a run interval or a tiering policy Ptah does not model, "+
			"and SET (TTL = ...) resets it to YDB's default. Change the TTL by hand with `ydb table ttl set`, "+
			"or reset it first")
	}
	return p.refuseTTLColumn(subject, tableDiff.Desired, desired)
}

// refuseDroppingTheTTLColumn refuses dropping the column a table's TTL reads
// while the TTL stays: YDB refuses it (`Can't drop TTL column: 'ts', disable
// TTL first`). A declaration that names such a column is refused when it is
// validated, so this is reached by a TTL the declaration does not describe and
// keeps from the database, as an HCL document does.
func refuseDroppingTheTTLColumn(tableDiff difftypes.TableDiff) error {
	policy := tableDiff.Desired.Table.RowDeletionPolicy
	if policy.IsZero() {
		return nil
	}
	if !slices.ContainsFunc(tableDiff.ColumnsRemoved, func(column schemamodel.Field) bool { return column.Name == policy.Column }) {
		return nil
	}
	return refuseFact(fmt.Sprintf("dropping column %q of table %q", policy.Column, tableDiff.TableName),
		"the table's TTL reads it, and YDB refuses to drop the column a TTL reads (`Can't drop TTL column`); "+
			"remove the TTL, or move it to another column, first")
}

// refuseTTLColumn holds the column a policy reads to the types YDB reads a TTL
// from, through the type map the renderer writes the column with. A
// modification that carries no declaration of the table is left to the
// server, which refuses the statement by itself.
func (p *Planner) refuseTTLColumn(subject string, declaration difftypes.TableDeclaration, policy *ast.RowDeletionPolicySpec) error {
	if !declaration.HasTable() {
		return nil
	}
	index := slices.IndexFunc(declaration.Fields, func(field schemamodel.Field) bool { return field.Name == policy.Column })
	if index < 0 {
		return refuseFact(subject, fmt.Sprintf("it reads column %q, which the table does not declare "+
			"(`Cannot enable TTL on unknown column`)", policy.Column))
	}
	unit, err := ydbttl.Unit(policy.Unit)
	if err != nil {
		return refuseFact(subject, err.Error())
	}
	// A type the map refuses is the column's refusal, which is reported where
	// the column is written.
	if mapping, mapErr := ydbtype.Map(declaration.Fields[index].Type, p.caps); mapErr == nil {
		if reason := ydbttl.ColumnRefusal(policy.Column, mapping.Type, unit); reason != "" {
			return refuseFact(subject, reason)
		}
	}
	return nil
}

// recordsSetting reports whether the read of the database recorded a setting
// of kind on table as not described. A record naming the whole kind counts: a
// read that did not look at any table's setting cannot say this table has
// none.
//
// A format's limit does not count. It is what a document's loader records for
// a family the format has no spelling for -- an HCL document standing for the
// current state cannot say whether a table has a TTL -- and it says nothing
// about a table carrying one, where these records are read as exactly that.
func recordsSetting(set coverage.Set, kind coverage.Kind, table schemamodel.Table) bool {
	canonical := tableref.Canonical(table.Schema, table.Name)
	return slices.ContainsFunc(set.Objects, func(object coverage.Object) bool {
		formatLimit := object.Reason == coverage.Unsupported && object.Provenance == coverage.DerivedFromFact
		return object.Kind == kind && !formatLimit &&
			(object.WholeKind() || object.Name == canonical || strings.HasPrefix(object.Name, canonical+"/"))
	})
}
