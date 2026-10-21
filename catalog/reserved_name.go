package catalog

import "ptah.run/core/objectidentity"

// ReservedName records an occupied name outside desired schema management.
// The reader verifies ownership and storage layout before declaring a name.
// Shared engine namespaces produce one record per conflicting object kind.
// Identity retains source and normalized components; its immutable path parts
// allow ordinary catalog copies to retain the observation without aliasing.
type ReservedName struct {
	// ProviderID identifies the provider that owns the operational storage.
	ProviderID string `json:"provider_id"`
	// Engine binds the observation to the inspected engine.
	Engine string `json:"engine"`
	// Purpose names the operational use: history, lock, log, or tag.
	Purpose string `json:"purpose"`
	// Identity names an object that cannot be managed by a desired schema.
	Identity objectidentity.ID `json:"identity"`
}
