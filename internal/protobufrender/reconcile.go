package protobufrender

import (
	"fmt"
	"sort"
	"strings"
)

// reconcile applies the previously pinned numbering to the desired shape. This
// is where the compatibility contract is enforced: existing numbers are kept,
// removed identifiers are reserved by number and name, and new numbers are
// allocated above everything the type has ever used.
func (b *builder) reconcile(desired desiredShape, prev *previousSet) (outMessages []message, outEnums []enum, err error) {
	seenMessages := map[string]bool{}
	for _, dm := range desired.messages {
		seenMessages[dm.Name] = true
		msg, err := b.reconcileMessage(dm, prev)
		if err != nil {
			return nil, nil, err
		}
		outMessages = append(outMessages, msg)
	}

	seenEnums := map[string]bool{}
	for _, de := range desired.enums {
		seenEnums[de.Name] = true
		en, err := b.reconcileEnum(de, prev)
		if err != nil {
			return nil, nil, err
		}
		outEnums = append(outEnums, en)
	}

	if prev == nil {
		return outMessages, outEnums, nil
	}

	// A type that is already a tombstone was removed by an earlier run and has
	// been handled. Carrying it forward untouched is what makes regeneration
	// idempotent; re-applying the policy would make the default refuse forever
	// after a single tombstone, and force --proto-type-removal=tombstone on
	// every later export.
	var removedMessages, removedEnums []string
	for _, name := range missingNames(prev.Messages, seenMessages) {
		state := prev.Messages[name]
		if isMessageTombstone(state) && b.opts.TypeRemoval != RemovalDrop {
			outMessages = append(outMessages, tombstoneMessage(name, state))
			continue
		}
		removedMessages = append(removedMessages, name)
	}
	for _, name := range missingNames(prev.Enums, seenEnums) {
		state := prev.Enums[name]
		if isEnumTombstone(state) && b.opts.TypeRemoval != RemovalDrop {
			outEnums = append(outEnums, tombstoneEnum(name, state))
			continue
		}
		removedEnums = append(removedEnums, name)
	}

	if len(removedMessages) == 0 && len(removedEnums) == 0 {
		return outMessages, outEnums, nil
	}

	switch b.opts.TypeRemoval {
	case RemovalDrop:
		for _, name := range append(append([]string{}, removedMessages...), removedEnums...) {
			b.warn(name, fmt.Sprintf(
				"type %q was removed from the source schema and dropped; its field numbers are no longer reserved and wire compatibility for it is abandoned", name))
		}
	case RemovalTombstone:
		for _, name := range removedMessages {
			outMessages = append(outMessages, tombstoneMessage(name, prev.Messages[name]))
			b.warn(name, fmt.Sprintf("message %q was removed from the source schema and retained as a tombstone", name))
		}
		for _, name := range removedEnums {
			outEnums = append(outEnums, tombstoneEnum(name, prev.Enums[name]))
			b.warn(name, fmt.Sprintf("enum %q was removed from the source schema and retained as a tombstone", name))
		}
	default:
		var removed []string
		removed = append(removed, removedMessages...)
		removed = append(removed, removedEnums...)
		sort.Strings(removed)
		return nil, nil, fmt.Errorf(
			"types removed from the source schema: %s; protobuf cannot reserve a top-level type name, so choose --proto-type-removal=tombstone to retain them for wire compatibility or =drop to abandon it",
			strings.Join(removed, ", "))
	}
	return outMessages, outEnums, nil
}

func missingNames(previous map[string]previousType, seen map[string]bool) []string {
	var missing []string
	for name := range previous {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	return missing
}

func (b *builder) reconcileMessage(dm desiredMessage, prev *previousSet) (message, error) {
	msg := message{Name: dm.Name, Comment: dm.Comment}

	var state previousType
	if prev != nil {
		state = prev.Messages[dm.Name]
	}
	if state.Numbers == nil {
		state.Numbers = map[string]int32{}
	}
	msg.Reserved = cloneReservations(state.Reserved)

	highest := state.highestUsed()
	live := map[string]bool{}

	for _, df := range dm.Fields {
		live[df.Name] = true

		if err := b.releaseReservedName(&msg.Reserved, dm.Name, df.Name, "field"); err != nil {
			return message{}, err
		}

		number, existed := state.Numbers[df.Name]
		if existed {
			prevField := state.Fields[df.Name]
			changed := prevField.Type != df.Type || prevField.Repeated != df.Repeated
			if changed && b.opts.OnIncompatibleChange == ChangeRenumber {
				// Only the number is reserved. The name stays live on the new
				// field, and protobuf refuses to compile a file that reuses a
				// reserved name, so reserving it here would produce output that
				// does not build. The consequence is explicit: the JSON name
				// now binds to a different field number, which buf breaking
				// WIRE_JSON reports once.
				msg.Reserved.addNumber(number)
				b.warn(dm.Name+"."+df.Name, fmt.Sprintf(
					"field %q changed from %s to %s and was renumbered; number %d is now reserved, "+
						"and because the field keeps its name, JSON-name compatibility for it is abandoned",
					df.Name, describeType(prevField),
					describeType(previousField{Type: df.Type, Repeated: df.Repeated}), number))
				existed = false
			} else if changed {
				return message{}, fmt.Errorf(
					"field %q on message %q changed from %s to %s, which is not wire compatible; pass --proto-on-incompatible-change=renumber to reserve the old number and allocate a new one",
					df.Name, dm.Name, describeType(prevField), describeType(previousField{Type: df.Type, Repeated: df.Repeated}))
			}
		}

		if !existed {
			allocated, ok := nextNumber(highest)
			if !ok {
				return message{}, fmt.Errorf("message %q has exhausted the protobuf field number space", dm.Name)
			}
			number = allocated
		}
		highest = max(highest, number)
		msg.Fields = append(msg.Fields, field{
			Name:     df.Name,
			Number:   number,
			Type:     df.Type,
			Repeated: df.Repeated,
			Comment:  df.Comment,
		})
	}

	// Everything the previous file held that the source no longer describes is
	// retired by both number and name. Reserving the name as well as the number
	// is what keeps the export clean under buf breaking WIRE_JSON.
	var retired []string
	for name, number := range state.Numbers {
		if live[name] {
			continue
		}
		retired = append(retired, name)
		msg.Reserved.addNumber(number)
		msg.Reserved.addName(name)
	}
	if err := b.reportRetiredFields(dm.Name, retired); err != nil {
		return message{}, err
	}
	return msg, nil
}

// reportRetiredFields answers for fields the source stopped describing.
//
// Retiring a number is a change to the contract a consumer already holds, and
// it is the only compatibility-relevant event in this exporter that used to
// pass without a word: a renamed column exited 0 with one number retired and
// another allocated, where a removed type, a changed type and a reused name
// were each already refused by default (stokaro/ptah#905).
func (b *builder) reportRetiredFields(owner string, retired []string) error {
	if len(retired) == 0 {
		return nil
	}
	sort.Strings(retired)

	if b.opts.OnFieldRemoval == FieldRemovalReserve {
		for _, name := range retired {
			b.warn(owner, fmt.Sprintf(
				"field %q was removed from %s and retired; its number and name are reserved",
				name, owner))
		}
		return nil
	}
	return fmt.Errorf(
		"fields removed from %s: %s; retiring a number changes the contract consumers hold, so choose"+
			" --proto-on-field-removal=reserve to retire them, or restore the name if the column was renamed",
		owner, strings.Join(retired, ", "))
}

func (b *builder) reconcileEnum(de enum, prev *previousSet) (enum, error) {
	out := enum{Name: de.Name, Comment: de.Comment}

	var state previousType
	if prev != nil {
		state = prev.Enums[de.Name]
	}
	if state.Numbers == nil {
		state.Numbers = map[string]int32{}
	}
	out.Reserved = cloneReservations(state.Reserved)

	highest := state.highestUsed()
	live := map[string]bool{}

	for _, dv := range de.Values {
		live[dv.Name] = true

		// The zero value is synthesized and always keeps number 0, which never
		// raises the high-water mark: highestUsed is never negative.
		if dv.Number == 0 {
			out.Values = append(out.Values, enumValue{Name: dv.Name, Number: 0, Comment: dv.Comment})
			continue
		}

		if err := b.releaseReservedName(&out.Reserved, de.Name, dv.Name, "enum value"); err != nil {
			return enum{}, err
		}

		number, existed := state.Numbers[dv.Name]
		if !existed {
			allocated, ok := nextNumber(highest)
			if !ok {
				return enum{}, fmt.Errorf("enum %q has exhausted the protobuf number space", de.Name)
			}
			number = allocated
		}
		highest = max(highest, number)
		out.Values = append(out.Values, enumValue{Name: dv.Name, Number: number, Comment: dv.Comment})
	}

	for name, number := range state.Numbers {
		if live[name] || number == 0 {
			continue
		}
		out.Reserved.addNumber(number)
		out.Reserved.addName(name)
	}
	return out, nil
}

// releaseReservedName handles an identifier coming back after it was retired.
// protoc refuses to compile a file that reuses a reserved name, and dropping
// the reservation rebinds the JSON name to a different number, so the generator
// must not choose silently.
func (b *builder) releaseReservedName(res *reservations, owner, name, kind string) error {
	if !res.hasName(name) {
		return nil
	}
	if b.opts.OnNameReuse != NameReuseRelease {
		return fmt.Errorf(
			"%s %q on %q is reserved because it was previously removed, and protobuf refuses to reuse a reserved name; "+
				"pass --proto-on-name-reuse=release to drop the name reservation (its number stays reserved) "+
				"and abandon JSON-name compatibility for it",
			kind, name, owner)
	}
	res.dropName(name)
	b.warn(owner+"."+name, fmt.Sprintf(
		"%s %q reuses a reserved name; the name reservation was released and JSON-name compatibility for it is abandoned, which buf breaking WIRE_JSON reports once",
		kind, name))
	return nil
}

// tombstoneMessage retains a removed message with everything it ever held
// reserved. A message may be emptied completely, unlike an enum.
func tombstoneMessage(name string, state previousType) message {
	msg := message{
		Name:      name,
		Comment:   tombstoneComment,
		Tombstone: true,
		Reserved:  cloneReservations(state.Reserved),
	}
	for valueName, number := range state.Numbers {
		msg.Reserved.addNumber(number)
		msg.Reserved.addName(valueName)
	}
	return msg
}

// tombstoneEnum retains a removed enum. protoc rejects an enum with no values
// ("Enums must contain at least one value."), so the synthesized zero value is
// kept and everything else is reserved. That is safe because the zero value is
// generated by Ptah and checkValueCollisions rejects any schema whose labels
// collide with it.
func tombstoneEnum(name string, state previousType) enum {
	out := enum{
		Name:      name,
		Comment:   tombstoneComment,
		Tombstone: true,
		Reserved:  cloneReservations(state.Reserved),
	}
	zero := unspecifiedValueName(name)
	for valueName, number := range state.Numbers {
		if number == 0 {
			zero = valueName
			continue
		}
		out.Reserved.addNumber(number)
		out.Reserved.addName(valueName)
	}
	out.Values = append(out.Values, enumValue{Name: zero, Number: 0})
	return out
}

const tombstoneComment = "Removed from the source schema; retained for wire compatibility."

// isMessageTombstone reports whether a previous message is already a tombstone:
// no live fields, but numbers reserved. Detected structurally rather than from
// the comment, because comments are not part of the compatibility state.
func isMessageTombstone(state previousType) bool {
	return len(state.Numbers) == 0 && state.Reserved.hasNumbers()
}

// isEnumTombstone reports whether a previous enum is already a tombstone. An
// enum can never be emptied - protoc rejects an enum with no values - so a
// tombstoned enum retains exactly its synthesized zero value.
func isEnumTombstone(state previousType) bool {
	if !state.Reserved.hasNumbers() || len(state.Numbers) != 1 {
		return false
	}
	for _, number := range state.Numbers {
		return number == 0
	}
	return false
}

func cloneReservations(in reservations) reservations {
	return reservations{
		Ranges: append([]numberRange(nil), in.Ranges...),
		Names:  append([]string(nil), in.Names...),
	}
}

func describeType(shape previousField) string {
	if shape.Type == "" {
		return "(unknown)"
	}
	if shape.Repeated {
		return "repeated " + shape.Type
	}
	return shape.Type
}
