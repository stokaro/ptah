package provider

import (
	"slices"
	"strings"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// CommonNamespacesFeature carries an engine's namespaces on the common-object
// wire as the SCHEMA kind. A read that covers the kind lists every namespace
// of its scope in ObservedCommonObjects.namespaces, the default one included,
// so a namespace it does not list is absent. A plan creates a namespace with
// CommonObjectChange.create_schema and removes one with drop_schema, and the
// engine renders both. It is a two-way feature that requires
// CommonObjectsFeature: the provider lists it, and so does each engine that
// declares SCHEMA among its common kinds. Without it a host sends no
// namespace and a read reports none. Contract 25.29.
const CommonNamespacesFeature = "schema.common-namespaces"

// NamespaceKind is the reference kind of a namespace: an observed namespace's
// identity and the object of a change that creates or removes one.
const NamespaceKind = "schema"

// validateCommonNamespacesFeature checks an engine's namespace declaration:
// the feature needs the provider's and the engine's common objects, and the
// SCHEMA kind is declared exactly when the engine lists the feature.
func validateCommonNamespacesFeature(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), CommonNamespacesFeature)
	declared := slices.Contains(engine.GetCommonObjects().GetKinds(), protocol.CommonObjectSupport_SCHEMA)
	if listed && (!slices.Contains(description.GetFeatures(), CommonNamespacesFeature) || !slices.Contains(engine.GetFeatures(), CommonObjectsFeature)) {
		return commonObjectError("common namespaces require the provider's feature and the engine's common objects")
	}
	if listed != declared {
		return commonObjectError("an engine declares the SCHEMA kind exactly when it lists " + CommonNamespacesFeature)
	}
	return nil
}

// negotiateCommonNamespaces removes the SCHEMA kind from an engine whose host
// does not list CommonNamespacesFeature. An engine left with no kind carries
// no common object.
func negotiateCommonNamespaces(engine *protocol.Engine, hostFeatures []string) {
	if slices.Contains(hostFeatures, CommonNamespacesFeature) || engine.GetCommonObjects() == nil {
		return
	}
	engine.CommonObjects.Kinds = slices.DeleteFunc(engine.CommonObjects.Kinds, func(kind protocol.CommonObjectSupport_Kind) bool {
		return kind == protocol.CommonObjectSupport_SCHEMA
	})
	if len(engine.CommonObjects.Kinds) == 0 {
		engine.CommonObjects = nil
		engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool {
			return feature == CommonObjectsFeature || feature == CommonAccessFeature || feature == CommonViewFacetsFeature || feature == RoleScopeReasonsFeature
		})
	}
}

// validNamespaceReference reports whether ref names a namespace of engine:
// kind "schema" and a name, with no schema, catalog, path or overload.
func validNamespaceReference(ref *protocol.Reference, engine string) bool {
	return validValueReference(ref, engine) && ref.GetKind() == NamespaceKind && strings.TrimSpace(ref.GetName()) != "" && ref.GetSchema() == "" &&
		ref.GetCatalog() == "" && len(ref.GetPath()) == 0 && len(ref.GetOverload()) == 0
}

// validateObservedNamespaces checks the namespaces a read lists: only of a
// covered SCHEMA kind, each a namespace of the engine, and none twice under
// the engine's rules for table names, which name a namespace too.
func validateObservedNamespaces(engine *protocol.Engine, complete commonKinds, namespaces []*protocol.ObservedNamespace) error {
	seen := make(map[string]bool, len(namespaces))
	for _, namespace := range namespaces {
		if !complete[protocol.CommonObjectSupport_SCHEMA] {
			return commonObjectError("a namespace the read did not cover")
		}
		identity := namespace.GetIdentity()
		if !validNamespaceReference(identity, engine.GetId()) {
			return commonObjectError("a namespace needs an identity of kind schema and a name")
		}
		key := identity.GetDatabase() + "\x00" + sqlFold(engine.GetIdentifiers().GetTableNames(), identity.GetName())
		if seen[key] {
			return commonObjectError("a repeated namespace")
		}
		seen[key] = true
	}
	return nil
}

// namespaceChangeSubject handles the creation and the removal of a namespace,
// and reports whether the change is one of them.
func namespaceChangeSubject(engine string, kinds commonKinds, change *protocol.CommonObjectChange) (*protocol.Reference, bool, error) {
	switch operation := change.GetOperation().(type) {
	case *protocol.CommonObjectChange_CreateSchema:
		creation := operation.CreateSchema
		if err := kinds.require(protocol.CommonObjectSupport_SCHEMA); err != nil {
			return nil, true, err
		}
		if strings.TrimSpace(creation.GetName()) == "" {
			return nil, true, commonObjectError("a namespace creation needs a name")
		}
		return named(engine, NamespaceKind, "", creation.GetName()), true, kinds.commentCarried(creation.GetComment())
	case *protocol.CommonObjectChange_DropSchema:
		ref := operation.DropSchema
		if !validNamespaceReference(ref, engine) {
			return nil, true, commonObjectError("a namespace removal needs the namespace")
		}
		return ref, true, kinds.require(protocol.CommonObjectSupport_SCHEMA)
	default:
		return nil, false, nil
	}
}
