package ydbreplication

import (
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableref"
)

// ReplicationKind identifies one YDB async replication: tables of another
// database copied into replica tables of this one, at a path of the scheme
// tree.
const ReplicationKind schemaext.Kind = "ptah.run/ydb/async-replication"

// TransferKind identifies one YDB transfer: a topic's messages turned into
// rows of a table through a lambda, at a path of the scheme tree.
const TransferKind schemaext.Kind = "ptah.run/ydb/transfer"

// DesiredReplication is a declared async replication. A setting its spec
// leaves at the zero value declares nothing, and the comparison reads it as
// the value YDB gives a new replication.
type DesiredReplication struct {
	Spec ReplicationSpec `json:"spec"`
	// StructName preserves the Go holder that declared the replication. It
	// has no server counterpart and changes no statement.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedReplication is an async replication a database read described: the
// connection in its canonical form, the credential by the secret it names,
// one item per replicated table, and the state the replication reports.
//
// The read can hold what no declaration may say, so an observation is held
// only to the shape the reader writes: a replication that failed before it
// resolved its tables reads with no item, and a secret path outside the
// database reads absolute.
type ObservedReplication struct {
	Spec ReplicationSpec `json:"spec"`
	// State is the state the replication reported, one of the State
	// constants, or empty where none was read. No declaration sets it; a plan
	// reads it to decide what it may change.
	State string `json:"state,omitempty"`
}

// DesiredTransfer is a declared transfer.
type DesiredTransfer struct {
	Spec TransferSpec `json:"spec"`
	// StructName preserves the Go holder that declared the transfer. It has
	// no server counterpart and changes no statement.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedTransfer is a transfer a database read described: the lambda as YDB
// stores it, which may be text no declaration can write, the consumer it
// reads through, whether a declaration named it or YDB created it, and the
// state it reports.
type ObservedTransfer struct {
	Spec TransferSpec `json:"spec"`
	// State is the state the transfer reported, as [ObservedReplication.State].
	State string `json:"state,omitempty"`
}

// Kind returns the replication model identity.
func (*DesiredReplication) Kind() schemaext.Kind { return ReplicationKind }

// Kind returns the replication model identity.
func (*ObservedReplication) Kind() schemaext.Kind { return ReplicationKind }

// Kind returns the transfer model identity.
func (*DesiredTransfer) Kind() schemaext.Kind { return TransferKind }

// Kind returns the transfer model identity.
func (*ObservedTransfer) Kind() schemaext.Kind { return TransferKind }

// Clone returns an independent declaration snapshot.
func (v *DesiredReplication) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredReplication)(nil)
	}
	return &DesiredReplication{Spec: v.Spec.Clone(), StructName: v.StructName}
}

// Clone returns an independent observation snapshot.
func (v *ObservedReplication) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedReplication)(nil)
	}
	return &ObservedReplication{Spec: v.Spec.Clone(), State: v.State}
}

// Clone returns an independent declaration snapshot.
func (v *DesiredTransfer) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredTransfer)(nil)
	}
	return &DesiredTransfer{Spec: v.Spec, StructName: v.StructName}
}

// Clone returns an independent observation snapshot.
func (v *ObservedTransfer) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedTransfer)(nil)
	}
	return &ObservedTransfer{Spec: v.Spec, State: v.State}
}

// Equal compares captured declarations field by field, without resolving
// defaults: two declarations [ReplicationsEqual] reads as one replication may
// still differ here.
func (v *DesiredReplication) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredReplication)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.StructName == right.StructName && replicationFieldsEqual(v.Spec, right.Spec)
}

// Equal compares raw observations field by field.
func (v *ObservedReplication) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedReplication)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.State == right.State && replicationFieldsEqual(v.Spec, right.Spec)
}

// Equal compares captured declarations field by field, without resolving
// defaults.
func (v *DesiredTransfer) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredTransfer)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.StructName == right.StructName && v.Spec == right.Spec
}

// Equal compares raw observations field by field.
func (v *ObservedTransfer) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedTransfer)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.State == right.State && v.Spec == right.Spec
}

// Matches reports whether current is the replication v declares, as
// [ReplicationsEqual] reads the two: each resolved as YDB keeps it.
func (v *DesiredReplication) Matches(current *ObservedReplication) bool {
	return ReplicationsEqual(v.Spec, current.Spec)
}

// Matches reports whether current is the transfer v declares, as
// [TransfersEqual] reads the two.
func (v *DesiredTransfer) Matches(current *ObservedTransfer) bool {
	return TransfersEqual(v.Spec, current.Spec)
}

// Desired converts an observation into a declaration that keeps the
// replication as the database holds it. The state is the database's and is
// left out.
func (v *ObservedReplication) Desired() *DesiredReplication {
	if v == nil {
		return nil
	}
	return &DesiredReplication{Spec: v.Spec.Clone()}
}

// Observed projects the declaration as a read would report it once applied,
// in state. It establishes no inspection evidence.
func (v *DesiredReplication) Observed(state string) *ObservedReplication {
	if v == nil {
		return nil
	}
	return &ObservedReplication{Spec: v.Spec.Clone(), State: state}
}

// Desired converts an observation into a declaration that keeps the transfer
// as the database holds it, without its state.
func (v *ObservedTransfer) Desired() *DesiredTransfer {
	if v == nil {
		return nil
	}
	return &DesiredTransfer{Spec: v.Spec}
}

// Observed projects the declaration as a read would report it once applied,
// in state. It establishes no inspection evidence.
func (v *DesiredTransfer) Observed(state string) *ObservedTransfer {
	if v == nil {
		return nil
	}
	return &ObservedTransfer{Spec: v.Spec, State: state}
}

// Validate refuses a declaration YDB would not keep as written, by the rules
// [ParseReplication] and [ParseItem] read one with, and a holder name that is
// not valid text.
func (v *DesiredReplication) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: a desired async replication is nil", schemaext.ErrInvalidValue)
	}
	if err := schemaext.ValidText("an async replication's holder", v.StructName); err != nil {
		return err
	}
	if err := replicationText(v.Spec); err != nil {
		return err
	}
	if err := ValidateReplication(v.Spec); err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	return nil
}

// Validate refuses an observation the reader cannot write: text that is not
// valid, a state it does not report, a consistency level or an interval YDB
// does not hold, and an item without a source or a target.
func (v *ObservedReplication) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: an observed async replication is nil", schemaext.ErrInvalidValue)
	}
	if err := replicationText(v.Spec); err != nil {
		return err
	}
	if err := validState(v.State); err != nil {
		return err
	}
	spec := v.Spec
	switch {
	case spec.ConsistencyLevel != "" && spec.ConsistencyLevel != ConsistencyRow && spec.ConsistencyLevel != ConsistencyGlobal:
		return fmt.Errorf("%w: an observed consistency level is %s or %s, not %q", schemaext.ErrInvalidValue,
			ConsistencyRow, ConsistencyGlobal, spec.ConsistencyLevel)
	case spec.CommitInterval != "" && spec.ConsistencyLevel != ConsistencyGlobal:
		return fmt.Errorf("%w: an observed commit interval belongs to the global consistency level", schemaext.ErrInvalidValue)
	}
	if spec.CommitInterval != "" {
		if _, err := CommitIntervalMillis(spec.CommitInterval); err != nil {
			return fmt.Errorf("%w: observed commit interval %q %w", schemaext.ErrInvalidValue, spec.CommitInterval, err)
		}
	}
	if slices.ContainsFunc(spec.Items, func(item Item) bool { return item.Source == "" || item.Target == "" }) {
		return fmt.Errorf("%w: an observed item names a source and a target", schemaext.ErrInvalidValue)
	}
	return nil
}

// Validate refuses a declaration YDB would not keep as written, by the rules
// [ParseTransfer] reads one with, and a holder name that is not valid text.
func (v *DesiredTransfer) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: a desired transfer is nil", schemaext.ErrInvalidValue)
	}
	if err := schemaext.ValidText("a transfer's holder", v.StructName); err != nil {
		return err
	}
	if err := transferText(v.Spec); err != nil {
		return err
	}
	if err := ValidateTransfer(v.Spec); err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	return nil
}

// Validate refuses an observation the reader cannot write: text that is not
// valid, a state it does not report, no source, target or lambda, and a
// flush interval YDB does not hold.
func (v *ObservedTransfer) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: an observed transfer is nil", schemaext.ErrInvalidValue)
	}
	if err := transferText(v.Spec); err != nil {
		return err
	}
	if err := validState(v.State); err != nil {
		return err
	}
	if v.Spec.Source == "" || v.Spec.Target == "" || v.Spec.Lambda == "" {
		return fmt.Errorf("%w: an observed transfer names a source, a target and a lambda", schemaext.ErrInvalidValue)
	}
	if v.Spec.FlushInterval != "" {
		if _, err := FlushIntervalSeconds(v.Spec.FlushInterval); err != nil {
			return fmt.Errorf("%w: observed flush interval %q %w", schemaext.ErrInvalidValue, v.Spec.FlushInterval, err)
		}
	}
	return nil
}

// ValidateReplication holds spec to the rules a declaration is read with: its
// connection and consistency as [ParseReplication] takes them, and at least
// one item, each as [ParseItem] takes it, no two at one target.
func ValidateReplication(spec ReplicationSpec) error {
	if _, err := ParseReplication(replicationValues(spec)); err != nil {
		return err
	}
	return ValidateReplicationItems(spec.Items)
}

// ValidateTransfer holds spec to the rules [ParseTransfer] reads a
// declaration with.
func ValidateTransfer(spec TransferSpec) error {
	_, err := ParseTransfer(transferValues(spec))
	return err
}

// validState refuses a state the reader does not report.
func validState(state string) error {
	switch state {
	case "", StateRunning, StatePaused, StateDone, StateError:
		return nil
	}
	return fmt.Errorf("%w: unknown replication state %q", schemaext.ErrInvalidValue, state)
}

// connectionText refuses a connection field that is not valid text.
func connectionText(connection Connection) error {
	for _, field := range []struct{ name, value string }{
		{AttributeConnectionString, connection.ConnectionString},
		{AttributeTokenSecretName, connection.TokenSecretName},
		{AttributeTokenSecretPath, connection.TokenSecretPath},
		{AttributeUser, connection.User},
		{AttributePasswordSecretName, connection.PasswordSecretName},
		{AttributePasswordSecretPath, connection.PasswordSecretPath},
	} {
		if err := schemaext.ValidText(field.name, field.value); err != nil {
			return err
		}
	}
	return nil
}

// replicationText refuses a replication field that is not valid text.
func replicationText(spec ReplicationSpec) error {
	if err := connectionText(spec.Connection); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{AttributeConsistencyLevel, spec.ConsistencyLevel},
		{AttributeCommitInterval, spec.CommitInterval},
	} {
		if err := schemaext.ValidText(field.name, field.value); err != nil {
			return err
		}
	}
	for _, item := range spec.Items {
		if err := schemaext.ValidText("an item's "+AttributeSource, item.Source); err != nil {
			return err
		}
		if err := schemaext.ValidText("an item's "+AttributeTarget, item.Target); err != nil {
			return err
		}
	}
	return nil
}

// transferText refuses a transfer field that is not valid text.
func transferText(spec TransferSpec) error {
	if err := connectionText(spec.Connection); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{AttributeSource, spec.Source},
		{AttributeTarget, spec.Target},
		{AttributeUsing, spec.Lambda},
		{AttributeConsumer, spec.Consumer},
		{AttributeFlushInterval, spec.FlushInterval},
	} {
		if err := schemaext.ValidText(field.name, field.value); err != nil {
			return err
		}
	}
	return nil
}

// replicationFieldsEqual compares two replication specs field by field.
func replicationFieldsEqual(a, b ReplicationSpec) bool {
	return a.Connection == b.Connection && slices.Equal(a.Items, b.Items) &&
		a.ConsistencyLevel == b.ConsistencyLevel && a.CommitInterval == b.CommitInterval
}

// ReplicationRef builds the exact identity of the async replication name in
// the directory schema, relative to the database root. A literal dot stays
// part of its component.
func ReplicationRef(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(ReplicationKind), schema, name)
}

// TransferRef builds the exact identity of the transfer name in the directory
// schema, relative to the database root.
func TransferRef(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(TransferKind), schema, name)
}

// ValidateIdentity requires a replication or transfer identity with separate
// exact YDB directory and leaf names: a leaf without a slash, and a directory
// that is a clean path relative to the database root.
func ValidateIdentity(ref objectidentity.ID) error {
	schema, name := ref.Schema.Source, ref.Name.Source
	var want objectidentity.ID
	switch schemaext.Kind(ref.Kind) {
	case ReplicationKind:
		want = ReplicationRef(schema, name)
	case TransferKind:
		want = TransferRef(schema, name)
	default:
		return fmt.Errorf("%w: %s is not an async replication or a transfer", schemaext.ErrInvalidValue, ref.Kind)
	}
	if strings.TrimSpace(name) == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") || strings.ContainsRune(schema, 0) ||
		!utf8.ValidString(schema) || !utf8.ValidString(name) || ref != want ||
		(schema != "" && (path.IsAbs(schema) || path.Clean(schema) != schema || schema == "." || schema == ".." || strings.HasPrefix(schema, "../"))) {
		return fmt.Errorf("%w: an async replication or a transfer requires a schema-scoped YDB identity", schemaext.ErrInvalidValue)
	}
	return nil
}

// Reference is the canonical reference of the object name in the directory
// schema, the name the statements and the refusals of this package take:
// `schema.name`, each part quoted where it holds a dot, or the name alone at
// the database root.
func Reference(schema, name string) string {
	return tableref.Canonical(schema, name)
}

// Display names a replication or a transfer by the path YDB writes for it,
// unquoted, for messages: `dir/name`, or `name` at the database root.
func Display(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "/" + name
}

// DesiredReplicationObject records one declared async replication. Collections
// clone the value on entry.
func DesiredReplicationObject(schema, name, structName string, spec ReplicationSpec) schemaext.Object {
	return schemaext.Object{Ref: ReplicationRef(schema, name), Value: &DesiredReplication{Spec: spec.Clone(), StructName: structName}}
}

// ObservedReplicationObject records one async replication a read described,
// in the state it reported.
func ObservedReplicationObject(schema, name string, spec ReplicationSpec, state string) schemaext.Object {
	return schemaext.Object{Ref: ReplicationRef(schema, name), Value: &ObservedReplication{Spec: spec.Clone(), State: state}}
}

// DesiredTransferObject records one declared transfer.
func DesiredTransferObject(schema, name, structName string, spec TransferSpec) schemaext.Object {
	return schemaext.Object{Ref: TransferRef(schema, name), Value: &DesiredTransfer{Spec: spec, StructName: structName}}
}

// ObservedTransferObject records one transfer a read described, in the state
// it reported.
func ObservedTransferObject(schema, name string, spec TransferSpec, state string) schemaext.Object {
	return schemaext.Object{Ref: TransferRef(schema, name), Value: &ObservedTransfer{Spec: spec, State: state}}
}
