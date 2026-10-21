package providerhost

import (
	"fmt"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
)

func (r reader) reservedNames(snapshot *protocol.Schema) ([]catalog.ReservedName, error) {
	var result []catalog.ReservedName
	seen := make(map[objectidentity.Key]bool)
	for _, record := range snapshot.GetReservedObjects() {
		source, normalized := record.GetObject(), record.GetNormalizedObject()
		if source.GetDatabase() != r.session.remote.GetDatabase() || normalized.GetDatabase() != source.GetDatabase() {
			return nil, fmt.Errorf("%w: reserved storage belongs to another database", provider.ErrProtocol)
		}
		for _, kind := range record.GetConflictKinds() {
			before, after := proto.CloneOf(source), proto.CloneOf(normalized)
			before.Kind, after.Kind = kind, kind
			identity, err := r.observedIdentity(before, after)
			if err != nil {
				return nil, err
			}
			if seen[identity.Key()] {
				return nil, fmt.Errorf("%w: reserved names collide after session binding", provider.ErrProtocol)
			}
			seen[identity.Key()] = true
			result = append(result, catalog.ReservedName{ProviderID: r.session.target.host.description.GetProviderId(),
				Engine: snapshot.GetEngine(), Purpose: strings.ToLower(record.GetPurpose().String()), Identity: identity})
		}
	}
	// Binding the default schema can reveal collisions not visible in wire tuples.
	for _, table := range snapshot.GetTables() {
		identity, err := r.observedIdentity(table.GetIdentity(), table.GetNormalizedIdentity())
		if err != nil {
			return nil, err
		}
		if seen[identity.Key()] {
			return nil, fmt.Errorf("%w: managed table occupies a reserved name", provider.ErrProtocol)
		}
	}
	return result, nil
}
