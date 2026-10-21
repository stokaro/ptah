package provider

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

const maxReservedIdentities = 4096

func validateReservedObjects(snapshot *protocol.Schema) error {
	var sources, normalized []*protocol.Reference
	if len(snapshot.GetReservedObjects()) > maxReservedIdentities {
		return fmt.Errorf("%w: too many reserved objects", ErrProtocol)
	}
	for _, record := range snapshot.GetReservedObjects() {
		if err := validateReservedObject(snapshot.GetEngine(), record); err != nil {
			return err
		}
		for _, kind := range record.GetConflictKinds() {
			if len(sources) == maxReservedIdentities {
				return fmt.Errorf("%w: too many reserved identities", ErrProtocol)
			}
			sources = append(sources, reservedReference(record.GetObject(), kind))
			normalized = append(normalized, reservedReference(record.GetNormalizedObject(), kind))
		}
	}
	for _, refs := range [][]*protocol.Reference{sources, normalized} {
		slices.SortFunc(refs, compareReferences)
		for i := 1; i < len(refs); i++ {
			if compareReferences(refs[i-1], refs[i]) == 0 {
				return fmt.Errorf("%w: overlapping reserved identities", ErrProtocol)
			}
		}
	}
	occupied := func(ref *protocol.Reference, refs []*protocol.Reference) bool {
		_, found := slices.BinarySearchFunc(refs, ref, compareReferences)
		return found
	}
	for _, table := range snapshot.GetTables() {
		if occupied(table.GetIdentity(), sources) || occupied(table.GetNormalizedIdentity(), normalized) {
			return fmt.Errorf("%w: reserved table also appears in managed observations", ErrProtocol)
		}
	}
	for _, value := range snapshot.GetNativeObjects() {
		if occupied(value.GetOwner(), sources) {
			return fmt.Errorf("%w: reserved object also appears in managed observations", ErrProtocol)
		}
	}
	// Coverage holds the normalized identity of every returned native value.
	// A conflicting positive claim would otherwise override the reservation.
	for _, subject := range snapshot.GetCoverage().GetSubjects() {
		if occupied(subject.GetNormalizedSubject(), normalized) && subject.GetKnowledge().GetState() != protocol.ModelKnowledge_UNREPRESENTABLE {
			return fmt.Errorf("%w: reserved identity has conflicting subject coverage", ErrProtocol)
		}
	}
	return nil
}

func validateReservedObject(engine string, record *protocol.ReservedObject) error {
	source, normalized := record.GetObject(), record.GetNormalizedObject()
	if !validReservedReference(source, engine) || !validReservedReference(normalized, engine) ||
		source.GetKind() != normalized.GetKind() || len(source.GetPath()) != len(normalized.GetPath()) {
		return fmt.Errorf("%w: invalid reserved object identity", ErrProtocol)
	}
	switch record.GetPurpose() {
	case protocol.ReservedObject_HISTORY, protocol.ReservedObject_LOCK, protocol.ReservedObject_LOG, protocol.ReservedObject_TAG:
	default:
		return fmt.Errorf("%w: invalid reserved object purpose", ErrProtocol)
	}
	if !slices.Contains(record.GetConflictKinds(), source.GetKind()) {
		return fmt.Errorf("%w: reservation excludes its own kind", ErrProtocol)
	}
	for _, kind := range record.GetConflictKinds() {
		if !reservedText(kind) || kind == "column" {
			return fmt.Errorf("%w: invalid reserved conflict kind", ErrProtocol)
		}
		if kind == "table" && (source.GetCatalog() != "" || normalized.GetCatalog() != "" || len(source.GetPath()) != 0) {
			return fmt.Errorf("%w: reserved common table has unsupported identity components", ErrProtocol)
		}
	}
	return nil
}

func validReservedReference(ref *protocol.Reference, engine string) bool {
	if !validValueReference(ref, engine) || !reservedText(ref.GetKind()) || !reservedText(ref.GetName()) || len(ref.GetOverload()) != 0 {
		return false
	}
	for _, part := range []string{ref.GetDatabase(), ref.GetCatalog(), ref.GetSchema()} {
		if part != "" && !reservedText(part) {
			return false
		}
	}
	return slices.IndexFunc(ref.GetPath(), func(part string) bool { return !reservedText(part) }) < 0
}

func reservedText(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func reservedReference(ref *protocol.Reference, kind string) *protocol.Reference {
	result := proto.CloneOf(ref)
	result.Kind = kind
	return result
}

func validateReservedModels(description *protocol.Description, snapshot *protocol.Schema) error {
	for _, record := range snapshot.GetReservedObjects() {
		for _, kind := range record.GetConflictKinds() {
			if kind == "table" {
				continue
			}
			if findValueModel(description, snapshot.GetEngine(), kind, protocol.ValueInput_OBSERVED) == nil {
				return fmt.Errorf("%w: reserved kind has no observed model", ErrProtocol)
			}
			source, normalized := reservedReference(record.GetObject(), kind), reservedReference(record.GetNormalizedObject(), kind)
			if !slices.ContainsFunc(snapshot.GetCoverage().GetSubjects(), func(subject *protocol.ModelSubjectCoverage) bool {
				return subject.GetKind() == kind && compareReferences(subject.GetSubject(), source) == 0 &&
					compareReferences(subject.GetNormalizedSubject(), normalized) == 0 &&
					subject.GetKnowledge().GetState() == protocol.ModelKnowledge_UNREPRESENTABLE && strings.TrimSpace(subject.GetKnowledge().GetReason()) != ""
			}) {
				return fmt.Errorf("%w: reserved native identity lacks explicit unrepresentable coverage", ErrProtocol)
			}
		}
	}
	return nil
}
