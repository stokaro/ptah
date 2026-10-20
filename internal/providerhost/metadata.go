package providerhost

// Metadata identifies a provider that completed authenticated negotiation.
// It reports a process contract, not the capabilities of a database server.
type Metadata struct {
	ProviderID       string `json:"provider_id"`
	Version          string `json:"version"`
	ProtocolRevision uint32 `json:"protocol_revision"`
	SemanticIdentity string `json:"semantic_identity"`
}

// Metadata returns negotiated identity without opening a database session.
func (h *Host) Metadata() Metadata {
	return Metadata{ProviderID: h.description.GetProviderId(), Version: h.description.GetVersion(),
		ProtocolRevision: h.description.GetRevision(), SemanticIdentity: h.description.GetSemanticIdentity()}
}
