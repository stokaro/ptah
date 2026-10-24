package providerhost

import (
	"fmt"
	"slices"

	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

// EmbeddingStore is an engine's embedding store as its provider serves it
// (ADR 0032): the client the calls go through, the engine, and the consistency
// modes the engine declares.
type EmbeddingStore struct {
	// Client validates every call against the contract both sides negotiated.
	Client provider.EmbeddingStore
	// Engine is the engine's ID, which an offline request names.
	Engine string
	// ConsistencyModes are the modes a generation on the engine can be built
	// with, by the host's names.
	ConsistencyModes []string
	// SessionID is the session the calls act on. It is empty for a store
	// reached without a connection, which answers offline requests only.
	SessionID string
}

// embeddingStore returns the store of the target's engine, or refuses an
// engine whose provider does not serve one to this host.
func (t *target) embeddingStore() (EmbeddingStore, error) {
	if !t.offers(provider.EmbeddingStoreFeature) {
		return EmbeddingStore{}, fmt.Errorf("%w: the %s provider keeps no embedding store for engine %q (%s)",
			ptaherr.ErrUnsupportedFeature, t.host.description.GetProviderId(), t.engine.GetId(), provider.EmbeddingStoreFeature)
	}
	return EmbeddingStore{Client: t.host.client, Engine: t.engine.GetId(),
		ConsistencyModes: slices.Clone(t.engine.GetEmbeddings().GetConsistencyModes())}, nil
}

// EmbeddingStore returns the embedding store of one of the provider's
// engines, for the requests that need no session.
func (h *Host) EmbeddingStore(engine string) (EmbeddingStore, error) {
	selected, found := h.targets[engine]
	if !found {
		return EmbeddingStore{}, fmt.Errorf("the provider serves no engine %q", engine)
	}
	return selected.embeddingStore()
}

// EmbeddingSession returns the embedding store of the provider session a
// connection holds. A connection through a database/sql driver holds none,
// and a session whose engine keeps no store is refused by name.
//
// The session is the connection's writer: a provider session writes schema
// through itself, so the writer of a provider connection is the session.
func EmbeddingSession(connection *dbschema.DatabaseConnection) (EmbeddingStore, error) {
	owner, ok := connection.SchemaWriter().(*session)
	if !ok {
		return EmbeddingStore{}, fmt.Errorf("%w: the connection is not a provider session", ptaherr.ErrUnsupportedFeature)
	}
	store, err := owner.target.embeddingStore()
	if err != nil {
		return EmbeddingStore{}, err
	}
	store.SessionID = owner.remote.GetId()
	return store, nil
}
