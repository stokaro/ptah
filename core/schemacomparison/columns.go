package schemacomparison

import (
	"context"
	"errors"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
)

// ErrColumns identifies an invalid or incomplete column service exchange.
var ErrColumns = errors.New("invalid column semantics exchange")

// ColumnService provides contextful, offline batches for common columns.
// Requests retain captured declarations; receipts describe effective comparison
// facts. Implementations must not read a server or return a successful prefix.
type ColumnService interface {
	CompareColumns(context.Context, ColumnsRequest) (ColumnsResult, error)
	ReadColumnDefaults(context.Context, ColumnDefaultsRequest) (ColumnDefaultsResult, error)
}

// ColumnAspect names one comparison area delegated by the host.
type ColumnAspect string

const (
	// ColumnType delegates type equivalence outside domain-owned comparisons.
	ColumnType ColumnAspect = "type"
	// ColumnDefault delegates default equivalence.
	ColumnDefault ColumnAspect = "default"
	// ColumnNullability requests authoritative effective desired nullability.
	ColumnNullability ColumnAspect = "nullable"
	// ColumnSequence delegates sequence start and increment comparison.
	ColumnSequence ColumnAspect = "sequence"
)

// ColumnPair binds the host's chosen desired and observed columns, including
// renames. Aspects states exactly which comparison areas the owner must answer.
type ColumnPair struct {
	Desired objectidentity.ID
	Current objectidentity.ID
	Aspects []ColumnAspect
}

// ColumnTable retains full parents for a batch of host-selected column pairs.
type ColumnTable struct {
	DesiredSubject objectidentity.ID
	CurrentSubject objectidentity.ID
	Desired        schemacapture.TableDeclaration
	Current        schemacapture.TableObservation
	Pairs          []ColumnPair
}

// ColumnsRequest carries explicit target facts and complete parent captures.
// Missing columns are never interpreted as observed absence by this service.
type ColumnsRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Tables       []ColumnTable
}

// Clone returns independent captures and pair inventories.
func (r ColumnsRequest) Clone() ColumnsRequest {
	r.Identifiers = r.Identifiers.Clone()
	r.Tables = slices.Clone(r.Tables)
	for i := range r.Tables {
		table := &r.Tables[i]
		table.Desired, table.Current = table.Desired.Clone(), table.Current.Clone()
		table.Pairs = slices.Clone(table.Pairs)
		for j := range table.Pairs {
			table.Pairs[j].Aspects = slices.Clone(table.Pairs[j].Aspects)
		}
	}
	return r
}

// ColumnRelation states whether the owner considers a compared aspect equal.
type ColumnRelation string

const (
	// ColumnSame means the declared and observed aspect agree.
	ColumnSame ColumnRelation = "same"
	// ColumnChanged means the aspect differs; spellings explain the difference.
	ColumnChanged ColumnRelation = "changed"
)

// ColumnDifference carries an explicit verdict and diagnostic spellings. Empty
// default spellings remain valid; it never replaces an authored operation operand.
type ColumnDifference struct {
	Relation ColumnRelation
	Current  string
	Desired  string
}

// SequenceAttribute names a comparison setting, never a restart operation.
type SequenceAttribute string

const (
	// SequenceStart is the declared identity start setting.
	SequenceStart SequenceAttribute = "identity_start"
	// SequenceIncrement is the declared identity increment setting.
	SequenceIncrement SequenceAttribute = "identity_increment"
)

// ColumnSequenceDifference reports one changed sequence setting.
type ColumnSequenceDifference struct {
	Attribute SequenceAttribute
	Current   string
	Desired   string
}

// ColumnSequenceChanges is an explicit receipt even when Changes is empty.
type ColumnSequenceChanges struct{ Changes []ColumnSequenceDifference }

// ColumnAspectReceipt has exactly the value selected by Aspect. Nullable uses
// presence so an explicit false cannot be confused with a missing response.
type ColumnAspectReceipt struct {
	Aspect     ColumnAspect
	Difference *ColumnDifference
	Nullable   *bool
	Sequence   *ColumnSequenceChanges
}

// ColumnReceipt repeats both exact identities and the requested aspect order.
type ColumnReceipt struct {
	Desired objectidentity.ID
	Current objectidentity.ID
	Aspects []ColumnAspectReceipt
}

// ColumnTableReceipt repeats both captured parent identities and pair order.
type ColumnTableReceipt struct {
	Desired objectidentity.ID
	Current objectidentity.ID
	Columns []ColumnReceipt
}

// ColumnsResult explicitly completes the whole batch, including an empty one.
type ColumnsResult struct {
	Complete bool
	Tables   []ColumnTableReceipt
}

// ObservedColumnDefault binds a reported column to its full structured identity.
// A caller includes observed-only columns used to construct rollback operands.
type ObservedColumnDefault struct {
	Subject objectidentity.ID
	Column  catalog.Column
}

// ColumnDefaultsRequest asks the owner to interpret exact reported defaults.
type ColumnDefaultsRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Columns      []ObservedColumnDefault
}

// Clone returns independent column data and target identifier facts.
func (r ColumnDefaultsRequest) Clone() ColumnDefaultsRequest {
	r.Identifiers = r.Identifiers.Clone()
	r.Columns = slices.Clone(r.Columns)
	for i := range r.Columns {
		r.Columns[i].Column = r.Columns[i].Column.Clone()
	}
	return r
}

// ColumnDefaultKind distinguishes absence, literals and SQL expressions.
type ColumnDefaultKind string

const (
	// DefaultAbsent means the observed column has no default.
	DefaultAbsent ColumnDefaultKind = "absent"
	// DefaultLiteral retains a value, including an explicitly empty string.
	DefaultLiteral ColumnDefaultKind = "literal"
	// DefaultExpression retains a nonempty SQL expression.
	DefaultExpression ColumnDefaultKind = "expression"
)

// ColumnDefaultReceipt carries a typed readback value, separate from other column facts.
type ColumnDefaultReceipt struct {
	Subject objectidentity.ID
	Kind    ColumnDefaultKind
	Value   string
}

// ColumnDefaultsResult completes the exact observed inventory in input order.
type ColumnDefaultsResult struct {
	Complete bool
	Columns  []ColumnDefaultReceipt
}
