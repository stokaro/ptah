package schemaext

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
)

// CapabilityProvider is a feature value or change whose object, once
// created, makes capabilities available to the statements after it, such as
// a PostgreSQL extension that installs TimescaleDB. A host that renders the
// statements of a plan or a render widens the target's capabilities with
// what the objects it creates provide: a capability set read from a
// connection opened before the object existed answers about the past, and
// would skip the statements that need the object.
//
// ProvidedCapabilities returns caps widened by what the object subject
// names provides, and caps unchanged where it provides nothing, as a change
// that drops its object does. The subject is the object's identity, which
// holds what a value alone does not, such as an extension's name. It must not
// change caps.
type CapabilityProvider interface {
	ProvidedCapabilities(subject objectidentity.ID, caps capability.Capabilities) capability.Capabilities
}

// SchemaPlacement is a feature value or change that puts its object into a
// named schema, as an extension's installation schema does. A host that
// creates the schemas a plan or a render puts objects into creates this one
// first. An empty answer names no schema: the object goes where the
// connection's default puts it, and no schema has to be created for it.
type SchemaPlacement interface {
	PlacementSchema() string
}

// ProvidedByChanges returns caps widened by every change whose value is a
// [CapabilityProvider], in order; any other change provides nothing. caps is
// not changed.
func ProvidedByChanges(caps capability.Capabilities, changes []ChangeRecord) capability.Capabilities {
	caps = caps.Clone()
	for _, change := range changes {
		if provider, ok := change.Value.(CapabilityProvider); ok {
			caps = provider.ProvidedCapabilities(change.Subject, caps)
		}
	}
	return caps
}

// ProvidedByObjects returns caps widened by every object whose value is a
// [CapabilityProvider], in identity order; any other object provides
// nothing. A whole-schema render, which has no connection to ask, reads what
// the objects it declares will provide this way. caps is not changed. The
// error is a value that cannot be cloned out of the collection.
func ProvidedByObjects(caps capability.Capabilities, objects Objects) (capability.Capabilities, error) {
	all, err := objects.All()
	if err != nil {
		return nil, err
	}
	caps = caps.Clone()
	for _, object := range all {
		if provider, ok := object.Value.(CapabilityProvider); ok {
			caps = provider.ProvidedCapabilities(object.Ref, caps)
		}
	}
	return caps, nil
}
