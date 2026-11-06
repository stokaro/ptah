package provider

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SharedCoverageFeature lets the host and the provider send a list of
// coverage kinds once per message, however many coverages of the message
// repeat it. Each state of each table names its coverage of every model the
// engine declares for its representation, the same list for every table of a
// read or a declaration, so a request of 500 tables repeated 16 kinds for
// each of 1,000 states, about nine tenths of its bytes, and each side copied,
// walked and checked every one. Contract 25.27. It is a two-way feature of
// the provider alone: a provider lists it, the host lists it in its host
// features, and the SDK's client and server share and resolve the lists only
// when the negotiated description lists it. Services never see a shared
// list: the receiving side gives each coverage its kinds back before a
// service or a validator reads the message, and the coverages that repeated
// one list then hold the same kind messages.
const SharedCoverageFeature = "transport.shared-coverage"

// ShareCoverage returns message with each list of coverage kinds that an
// earlier coverage of message already carries replaced by that coverage's
// key: the first coverage of a list keeps its kinds and gains a kinds_key,
// and each later one carries kinds_of instead of the kinds. Two lists are
// one list when they hold, in order, kinds of the same value model messages
// with equal sources, so sharing never merges kinds that differ. It returns
// message itself when no list repeats. Otherwise it returns a copy that
// shares every part of message the change does not reach; message is never
// changed, and the copy is for sending, not for changing.
func ShareCoverage(message proto.Message) proto.Message {
	sharing := &coverageSharing{counts: make(map[string]int), keys: make(map[string]uint32), kinds: make(map[*protocol.ComparisonKindCoverage]string)}
	if !sharing.repeats(message.ProtoReflect()) {
		return message
	}
	replaced, changed := sharing.share(message.ProtoReflect())
	if !changed {
		return message
	}
	return replaced.Interface()
}

// ResolveSharedCoverage gives, in message, each coverage that names another
// coverage's kinds by kinds_of those kinds, as the same kind messages, and
// clears every kinds_key and kinds_of. It refuses a key two coverages
// define, a key a coverage defines without kinds, a coverage that carries
// both kinds and kinds_of, and a kinds_of that names no key of message; then
// message may hold some coverages resolved and others not.
func ResolveSharedCoverage(message proto.Message) error {
	var coverages []*protocol.ComparisonCoverage
	collectCoverages(message.ProtoReflect(), &coverages)
	defined := make(map[uint32][]*protocol.ComparisonKindCoverage)
	for _, coverage := range coverages {
		key := coverage.GetKindsKey()
		if key == 0 {
			continue
		}
		if _, twice := defined[key]; twice || len(coverage.GetKinds()) == 0 || coverage.GetKindsOf() != 0 {
			return fmt.Errorf("%w: shared coverage key %d is defined twice or without kinds", ErrProtocol, key)
		}
		defined[key] = coverage.GetKinds()
	}
	for _, coverage := range coverages {
		of := coverage.GetKindsOf()
		if of == 0 {
			continue
		}
		kinds, found := defined[of]
		if !found || len(coverage.GetKinds()) != 0 {
			return fmt.Errorf("%w: shared coverage names no kinds of the message under key %d", ErrProtocol, of)
		}
		coverage.Kinds = slices.Clone(kinds)
	}
	for _, coverage := range coverages {
		coverage.KindsKey, coverage.KindsOf = 0, 0
	}
	return nil
}

// validateUnsharedCoverage refuses a coverage that still names a shared list:
// the SDK resolves every one before a validator reads the message, so one
// that remains was sent without the feature or never resolved.
func validateUnsharedCoverage(coverage *protocol.ComparisonCoverage) error {
	if coverage.GetKindsKey() != 0 || coverage.GetKindsOf() != 0 {
		return fmt.Errorf("%w: comparison coverage names a shared list of kinds the SDK did not resolve", ErrProtocol)
	}
	return nil
}

// coverageSharing assigns each list of kinds that more than one coverage
// carries the coverage that carries it first. counts maps a list's identity
// to the coverages that carry it, keys to its key once a coverage defines
// it, and kinds each kind message to its identity.
type coverageSharing struct {
	counts map[string]int
	keys   map[string]uint32
	kinds  map[*protocol.ComparisonKindCoverage]string
	// last is the key the latest list defined.
	last uint32
}

// listIdentity is the identity of a list of kinds: the identity of each kind
// in order. A kind is identified by the value model message it names and by
// its sources, so two kinds of one model with equal sources are one kind.
func (s *coverageSharing) listIdentity(kinds []*protocol.ComparisonKindCoverage) string {
	var identity strings.Builder
	for _, kind := range kinds {
		known, seen := s.kinds[kind]
		if !seen {
			sources, err := proto.MarshalOptions{Deterministic: true}.Marshal(&protocol.ComparisonKindCoverage{Sources: kind.GetSources()})
			if err != nil {
				// A kind that does not marshal is kept apart from every other.
				sources = []byte(fmt.Sprintf("%p", kind))
			}
			known = fmt.Sprintf("%p:%s", kind.GetModel(), sources)
			s.kinds[kind] = known
		}
		identity.WriteString(strconv.Itoa(len(known)))
		identity.WriteByte(':')
		identity.WriteString(known)
	}
	return identity.String()
}

// repeats reports whether two coverages of message carry one list of kinds,
// so a message without a repetition is sent as it is.
func (s *coverageSharing) repeats(message protoreflect.Message) bool {
	var coverages []*protocol.ComparisonCoverage
	collectCoverages(message, &coverages)
	repeated := false
	for _, coverage := range coverages {
		if len(coverage.GetKinds()) == 0 {
			continue
		}
		identity := s.listIdentity(coverage.GetKinds())
		s.counts[identity]++
		repeated = repeated || s.counts[identity] > 1
	}
	return repeated
}

// share returns the message that stands for message once each repeated list
// is replaced by its key, and whether that is a new message. A new message
// shares the fields no replacement reaches.
func (s *coverageSharing) share(message protoreflect.Message) (protoreflect.Message, bool) {
	if coverage, isCoverage := message.Interface().(*protocol.ComparisonCoverage); isCoverage {
		if len(coverage.GetKinds()) == 0 {
			return message, false
		}
		identity := s.listIdentity(coverage.GetKinds())
		if s.counts[identity] < 2 {
			return message, false
		}
		copied := &protocol.ComparisonCoverage{Subjects: coverage.GetSubjects()}
		if key, defined := s.keys[identity]; defined {
			copied.KindsOf = key
		} else {
			s.last++
			key = s.last
			s.keys[identity] = key
			copied.Kinds, copied.KindsKey = coverage.GetKinds(), key
		}
		copied.ProtoReflect().SetUnknown(coverage.ProtoReflect().GetUnknown())
		return copied.ProtoReflect(), true
	}
	if !holdsModels(message.Descriptor().FullName()) {
		return message, false
	}
	var copied protoreflect.Message
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		held := fieldMessage(field)
		if held == nil || !holdsModels(held.FullName()) {
			return true
		}
		replaced, changed := s.shareField(message, field, value)
		if changed {
			if copied == nil {
				copied = shallowCopy(message)
			}
			copied.Set(field, replaced)
		}
		return true
	})
	if copied == nil {
		return message, false
	}
	return copied, true
}

// shareField is share over the value of one field of message.
func (s *coverageSharing) shareField(message protoreflect.Message, field protoreflect.FieldDescriptor, value protoreflect.Value) (protoreflect.Value, bool) {
	switch {
	case field.IsList():
		list := value.List()
		var copied protoreflect.List
		for i := range list.Len() {
			replaced, changed := s.share(list.Get(i).Message())
			if !changed {
				continue
			}
			if copied == nil {
				copied = newListLike(message, field, list)
			}
			copied.Set(i, protoreflect.ValueOfMessage(replaced))
		}
		return protoreflect.ValueOfList(copied), copied != nil
	case field.IsMap():
		entries := value.Map()
		var copied protoreflect.Map
		entries.Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
			replaced, changed := s.share(entry.Message())
			if changed {
				if copied == nil {
					copied = newMapLike(message, field, entries)
				}
				copied.Set(key, protoreflect.ValueOfMessage(replaced))
			}
			return true
		})
		return protoreflect.ValueOfMap(copied), copied != nil
	default:
		replaced, changed := s.share(value.Message())
		return protoreflect.ValueOfMessage(replaced), changed
	}
}

// collectCoverages appends every coverage message holds to coverages, in the
// order a walk of its fields meets them.
func collectCoverages(message protoreflect.Message, coverages *[]*protocol.ComparisonCoverage) {
	if coverage, isCoverage := message.Interface().(*protocol.ComparisonCoverage); isCoverage {
		if coverage != nil {
			*coverages = append(*coverages, coverage)
		}
		return
	}
	if !message.IsValid() || !holdsModels(message.Descriptor().FullName()) {
		return
	}
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		held := fieldMessage(field)
		if held == nil || !holdsModels(held.FullName()) {
			return true
		}
		switch {
		case field.IsList():
			list := value.List()
			for i := range list.Len() {
				collectCoverages(list.Get(i).Message(), coverages)
			}
		case field.IsMap():
			value.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
				collectCoverages(entry.Message(), coverages)
				return true
			})
		default:
			collectCoverages(value.Message(), coverages)
		}
		return true
	})
}
