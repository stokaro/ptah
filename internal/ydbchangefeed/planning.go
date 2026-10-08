package ydbchangefeed

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbtype"
)

// CheckPlanned refuses a changefeed the target cannot hold on a table
// whose first key column has the YDB type keyType, empty where unknown.
func CheckPlanned(table string, changefeed ydbschema.ChangefeedSpec, keyType string, caps capability.Capabilities) error {
	if refusal := Check(table, changefeed, caps); refusal != nil {
		if refusal.Key != "" {
			return refuseKey(refusal.Key, refusal.Subject)
		}
		return refuseFact(refusal.Subject, refusal.Reason)
	}
	if reason := KeyRefusal(changefeed, keyType); reason != "" {
		return refuseFact(fmt.Sprintf("changefeed %q of table %q", changefeed.Name, table), reason)
	}
	return nil
}

// FirstKeyType is the YDB type the declaration's first key column maps to, or
// empty where the declaration does not say.
func FirstKeyType(declaration schemacapture.TableDeclaration, caps capability.Capabilities) string {
	key := declaration.Table.PrimaryKey
	for _, field := range declaration.Fields {
		if len(key) == 0 && field.Primary {
			key = []string{field.Name}
		}
	}
	if len(key) == 0 {
		return ""
	}
	for _, field := range declaration.Fields {
		if field.Name != key[0] {
			continue
		}
		mapping, err := ydbtype.Map(field.Type, caps)
		if err != nil {
			return ""
		}
		return mapping.Type
	}
	return ""
}

// DroppedNote says what dropping a changefeed loses.
func DroppedNote(table string, changefeed ydbschema.ChangefeedSpec) string {
	note := fmt.Sprintf("Changefeed %s of table %s is dropped with its topic: the records nobody read are lost",
		changefeed.Name, table)
	consumers := consumerNames(changefeed)
	switch len(consumers) {
	case 0:
		return note + "."
	case 1:
		return note + ", and so is consumer " + consumers[0] + "."
	default:
		return note + ", and so are consumers " + strings.Join(consumers, ", ") + "."
	}
}

// RecreatedNote says why a changefeed is dropped and added again, and what
// the restart costs its readers.
func RecreatedNote(table string, changefeed ydbschema.ChangefeedSpec) string {
	note := fmt.Sprintf("Changefeed %s of table %s is dropped and added again, because YDB changes no option of a "+
		"changefeed in place. Its stream restarts: the records nobody read are lost", changefeed.Name, table)
	consumers := consumerNames(changefeed)
	switch len(consumers) {
	case 0:
		return note + "."
	case 1:
		return note + ", and consumer " + consumers[0] + " loses its position and starts again from the beginning " +
			"of the new stream."
	default:
		return note + ", and consumers " + strings.Join(consumers, ", ") + " lose their position and start again " +
			"from the beginning of the new stream."
	}
}

// RestartedConsumersNote says why consumers are dropped and added again.
func RestartedConsumersNote(table, changefeed string, consumers []string) string {
	const why = "because YDB keeps a consumer's codecs once it has any."
	if len(consumers) == 1 {
		return fmt.Sprintf("Consumer %s of changefeed %s of table %s is dropped and added again, %s It loses its "+
			"position and starts again from the beginning of the stream.", consumers[0], changefeed, table, why)
	}
	return fmt.Sprintf("Consumers %s of changefeed %s of table %s are dropped and added again, %s They lose their "+
		"position and start again from the beginning of the stream.", strings.Join(consumers, ", "), changefeed, table, why)
}

// consumerNames lists a changefeed's consumers by name.
func consumerNames(changefeed ydbschema.ChangefeedSpec) []string {
	names := make([]string, len(changefeed.Consumers))
	for i, consumer := range changefeed.Consumers {
		names[i] = consumer.Name
	}
	return names
}

// rebuildChangefeedNote says why a rebuild drops and adds changefeeds.
func rebuildChangefeedNote(table string, changefeeds []ydbschema.ChangefeedSpec) string {
	const why = "because YDB moves no table that carries a changefeed."
	if len(changefeeds) == 1 {
		return fmt.Sprintf("Changefeed %s of table %s is dropped before the swap and added again after it, %s Its "+
			"stream restarts: the records nobody read are lost, and its consumers start again from the beginning of "+
			"the new stream.", changefeeds[0].Name, table, why)
	}
	names := make([]string, len(changefeeds))
	for i, changefeed := range changefeeds {
		names[i] = changefeed.Name
	}
	return fmt.Sprintf("Changefeeds %s of table %s are dropped before the swap and added again after it, %s Each "+
		"stream restarts: the records nobody read are lost, and the consumers start again from the beginning of "+
		"the new stream.", strings.Join(names, ", "), table, why)
}

// RebuildNotes reports lost records and restarted consumers around a parent replacement.
func RebuildNotes(table string, current, desired []ydbschema.ChangefeedSpec) []ast.Node {
	var notes []ast.Node
	var restored []ydbschema.ChangefeedSpec
	for _, stream := range current {
		if slices.ContainsFunc(desired, func(want ydbschema.ChangefeedSpec) bool { return want.Name == stream.Name }) {
			restored = append(restored, stream)
		} else {
			notes = append(notes, ast.NewComment(DroppedNote(table, stream)))
		}
	}
	if len(restored) > 0 {
		notes = append(notes, ast.NewComment(rebuildChangefeedNote(table, restored)))
	}
	return notes
}

// ValidateOperand requires a change operand to agree with captured state and knowledge.
func ValidateOperand(ref objectidentity.ID, value schemaext.Value, objects schemaext.Objects, coverage schemaext.Coverage) error {
	object, found, err := objects.Get(ref)
	if err != nil {
		return err
	}
	if found != (value != nil) || (found && !object.Value.Equal(value)) {
		return refuseFact(ref.String(), "the changefeed change disagrees with its captured table state")
	}
	knowledge := coverage.Lookup(ydbschema.ChangefeedKind, ref)
	// A concrete definition can be known in a partially enumerated namespace.
	// An explicit limitation on that object still prevents changing it.
	if found {
		explicit, limited := coverage.SubjectKnowledge(ydbschema.ChangefeedKind, ref)
		if !limited {
			return nil
		}
		knowledge = explicit
	}
	if knowledge.State != schemaext.Complete && (found || knowledge.State != schemaext.Absent) {
		return refuseFact(ref.String(), "the changefeed operand is not known: "+knowledge.Reason)
	}
	return nil
}

func refuseKey(key capability.Capability, subject string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: string(key), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target", subject, key, platform.YDB)}
}
func refuseFact(subject, reason string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: subject, Err: ptaherr.ErrUnsupportedFeature, Message: fmt.Sprintf("%s: %s", subject, reason)}
}
