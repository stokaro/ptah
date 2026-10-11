package pgextension

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
)

// PlacementSchema is the schema the declaration installs the extension into,
// empty for the connection's default; see [schemaext.SchemaPlacement]. An
// extension's identity names no schema, so the subject adds nothing.
func (v *DesiredExtension) PlacementSchema(objectidentity.ID) string {
	if v == nil {
		return ""
	}
	return v.Schema
}

// ProvidedCapabilities returns caps with what the extension subject names
// provides once it is installed, such as TimescaleDB's hypertables; see
// [schemaext.CapabilityProvider].
func (v *DesiredExtension) ProvidedCapabilities(subject objectidentity.ID, caps capability.Capabilities) capability.Capabilities {
	return capability.WithDeclaredExtensions(caps, []string{Name(subject)})
}

// PlacementSchema is the schema the change puts its extension into: the
// installation schema of one it installs, and the schema one it moves goes
// to. It is empty for a drop, a change that leaves the schema, and an
// installation into the connection's default.
func (v *Change) PlacementSchema(objectidentity.ID) string {
	switch {
	case v == nil || v.After == nil:
		return ""
	case v.Before == nil:
		return v.After.Schema
	default:
		return v.Schema
	}
}

// ProvidedCapabilities returns caps with what the extension subject names
// provides when the change installs it, and caps unchanged otherwise: an
// extension a plan updates, moves or drops was there when the connection
// reported its capabilities, or is gone after the plan.
func (v *Change) ProvidedCapabilities(subject objectidentity.ID, caps capability.Capabilities) capability.Capabilities {
	if v == nil || v.Before != nil || v.After == nil {
		return caps
	}
	return capability.WithDeclaredExtensions(caps, []string{Name(subject)})
}
