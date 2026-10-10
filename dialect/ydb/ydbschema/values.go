package ydbschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbtopic"
)

// ChangefeedKind identifies an individually named table-owned YDB stream.
const ChangefeedKind schemaext.Kind = "ptah.run/ydb/changefeed"

// DesiredChangefeed records a stream declaration or the requirement to retain
// an existing replication-managed stream in an effective planning snapshot.
type DesiredChangefeed struct {
	Spec ChangefeedSpec `json:"spec"`
	// RetainedReplication keeps an observed binding in an effective planning
	// snapshot. It is a requirement to retain that existing stream, never an
	// instruction to create an independently managed changefeed.
	RetainedReplication *ReplicationBinding `json:"retained_replication,omitempty"`
}

// ObservedChangefeed records a stream as inspected, including its disabled state.
type ObservedChangefeed struct {
	Spec        ChangefeedSpec      `json:"spec"`
	Replication *ReplicationBinding `json:"replication,omitempty"`
}

// Kind returns the stable changefeed model identity.
func (*DesiredChangefeed) Kind() schemaext.Kind { return ChangefeedKind }

// Kind returns the stable changefeed model identity.
func (*ObservedChangefeed) Kind() schemaext.Kind { return ChangefeedKind }

// Clone returns an independent declaration or retention requirement.
func (v *DesiredChangefeed) Clone() schemaext.Value {
	return &DesiredChangefeed{Spec: v.Spec.Clone(), RetainedReplication: v.RetainedReplication.Clone()}
}

// Clone returns an independent observation, including its replication binding.
func (v *ObservedChangefeed) Clone() schemaext.Value {
	return &ObservedChangefeed{Spec: v.Spec.Clone(), Replication: v.Replication.Clone()}
}

// Equal compares declarations without resolving server defaults or intervals.
func (v *DesiredChangefeed) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredChangefeed)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return equalSpec(v.Spec, w.Spec) && equalBinding(v.RetainedReplication, w.RetainedReplication)
}

// Equal compares observations without interpreting them as declarations.
func (v *ObservedChangefeed) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedChangefeed)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return equalSpec(v.Spec, w.Spec) && equalBinding(v.Replication, w.Replication)
}

// Desired captures the observation as effective desired state. A replication
// binding remains a retention requirement rather than a creation instruction.
func (v *ObservedChangefeed) Desired() *DesiredChangefeed {
	return &DesiredChangefeed{Spec: v.Spec.Clone(), RetainedReplication: v.Replication.Clone()}
}

// Observed projects a captured desired value without inspecting a server.
// Retained replication bindings survive; this projection proves no execution.
func (v *DesiredChangefeed) Observed() *ObservedChangefeed {
	return &ObservedChangefeed{Spec: v.Spec.Clone(), Replication: v.RetainedReplication.Clone()}
}

func equalSpec(a, b ChangefeedSpec) bool {
	if a.Name != b.Name || a.Mode != b.Mode || a.Format != b.Format ||
		a.VirtualTimestamps != b.VirtualTimestamps || a.ResolvedTimestamps != b.ResolvedTimestamps ||
		a.InitialScan != b.InitialScan || a.UserSIDs != b.UserSIDs || a.SchemaChanges != b.SchemaChanges ||
		a.TopicMinActivePartitions != b.TopicMinActivePartitions || a.TopicAutoPartitioning != b.TopicAutoPartitioning ||
		a.RetentionPeriod != b.RetentionPeriod || a.Disabled != b.Disabled || len(a.Consumers) != len(b.Consumers) {
		return false
	}
	a, b = a.Clone(), b.Clone()
	order := func(a, b ydbtopic.ConsumerSpec) int { return strings.Compare(a.Name, b.Name) }
	slices.SortFunc(a.Consumers, order)
	slices.SortFunc(b.Consumers, order)
	for i, left := range a.Consumers {
		right := b.Consumers[i]
		if left.Name != right.Name || left.Important != right.Important || left.ReadFrom != right.ReadFrom || left.AvailabilityPeriod != right.AvailabilityPeriod {
			return false
		}
		slices.Sort(left.SupportedCodecs)
		slices.Sort(right.SupportedCodecs)
		if !slices.Equal(left.SupportedCodecs, right.SupportedCodecs) {
			return false
		}
	}
	return true
}

// ChangefeedRef constructs a reference from separate YDB path components. A dot
// inside a component is not a boundary, and the table is a structured parent.
func ChangefeedRef(schema, table, name string) objectidentity.ID {
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	parent := builder.TableParts(schema, table)
	ref := builder.SchemaScopedParts(objectidentity.Kind(ChangefeedKind), schema, name)
	ref.Parent = parent.Name
	return ref
}

// DesiredObject captures one named declaration with its table parent.
func DesiredObject(schema, table string, spec ChangefeedSpec) schemaext.Object {
	return schemaext.Object{Ref: ChangefeedRef(schema, table, spec.Name), Value: &DesiredChangefeed{Spec: spec}}
}

// ObservedObject captures one named observation with its table parent.
func ObservedObject(schema, table string, spec ChangefeedSpec) schemaext.Object {
	return schemaext.Object{Ref: ChangefeedRef(schema, table, spec.Name), Value: &ObservedChangefeed{Spec: spec}}
}

// DesiredChangefeeds returns declarations belonging to one table in name order.
// A changefeed with an observed or unrecognized payload is an error.
// The returned specs omit replication bindings. Use the typed object values for
// capture, conversion, or replacement; specs alone cannot reconstruct ownership.
func DesiredChangefeeds(objects schemaext.Objects, schema, table string) ([]ChangefeedSpec, error) {
	return changefeeds(objects, schema, table, func(value schemaext.Value) (ChangefeedSpec, error) {
		v, ok := value.(*DesiredChangefeed)
		if !ok {
			return ChangefeedSpec{}, fmt.Errorf("%w: expected a desired changefeed, got %T", schemaext.ErrInvalidValue, value)
		}
		return v.Spec, nil
	})
}

// ObservedChangefeeds returns observations belonging to one table in name order.
// A changefeed with a desired or unrecognized payload is an error.
// The returned specs omit replication bindings. Use the typed object values for
// capture, conversion, or replacement; specs alone cannot reconstruct ownership.
func ObservedChangefeeds(objects schemaext.Objects, schema, table string) ([]ChangefeedSpec, error) {
	return changefeeds(objects, schema, table, func(value schemaext.Value) (ChangefeedSpec, error) {
		v, ok := value.(*ObservedChangefeed)
		if !ok {
			return ChangefeedSpec{}, fmt.Errorf("%w: expected an observed changefeed, got %T", schemaext.ErrInvalidValue, value)
		}
		return v.Spec, nil
	})
}

func changefeeds(objects schemaext.Objects, schema, table string, specOf func(schemaext.Value) (ChangefeedSpec, error)) ([]ChangefeedSpec, error) {
	parent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(schema, table)
	owned, err := objects.ForParent(parent).Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(ChangefeedKind) }).All()
	if err != nil {
		return nil, err
	}
	var result []ChangefeedSpec
	for _, object := range owned {
		spec, err := specOf(object.Value)
		if err != nil {
			return nil, err
		}
		if ChangefeedRef(schema, table, spec.Name).Key() != object.Ref.Key() {
			return nil, fmt.Errorf("%w: changefeed name disagrees with its reference", schemaext.ErrInvalidValue)
		}
		result = append(result, spec)
	}
	return result, nil
}
