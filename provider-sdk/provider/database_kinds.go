package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// DatabaseKindsFeature enables Engine.database_kinds: the kinds of the
// engine's OBSERVED models whose objects belong to the session's database
// rather than to a namespace of it, such as resource pools (ADR 0065). A
// filtered read reports such objects beside its scope, named at the
// database: the database and no catalog, schema or path. The provider and
// the engine list it, and the host lists it in its host features. Contract
// 25.21.
const DatabaseKindsFeature = "schema.database-kinds"

func databaseKindsError(reason string) error {
	return fmt.Errorf("%w: database kinds: %s", ErrProtocol, reason)
}

// validateDatabaseKinds checks an engine's database kinds: listed exactly
// when the engine lists the feature, which the provider lists too; each the
// kind of an OBSERVED model of the engine, named once; and none a kind a
// planning service assesses as context, which a filtered read carries
// outside its scope in another field.
func validateDatabaseKinds(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), DatabaseKindsFeature)
	if !listed {
		if len(engine.GetDatabaseKinds()) != 0 {
			return databaseKindsError("an engine lists database kinds without the feature")
		}
		return nil
	}
	if !slices.Contains(description.GetFeatures(), DatabaseKindsFeature) || len(engine.GetDatabaseKinds()) == 0 {
		return databaseKindsError("the engine requires the provider's feature and at least one kind")
	}
	context := engineContextKinds(engine)
	for i, kind := range engine.GetDatabaseKinds() {
		if slices.Contains(engine.GetDatabaseKinds()[:i], kind) || slices.Contains(context, kind) ||
			findValueModel(description, engine.GetId(), kind, protocol.ValueInput_OBSERVED) == nil {
			return databaseKindsError("a kind must be an observed model of the engine, named once and assessed by no planning service as context")
		}
	}
	return nil
}

// databaseKind reports whether kind is a database kind of the negotiated
// engine, which the target features of the read list.
func databaseKind(engine *protocol.Engine, targetFeatures []string, kind string) bool {
	return slices.Contains(targetFeatures, DatabaseKindsFeature) && slices.Contains(engine.GetDatabaseKinds(), kind)
}

// databaseReference reports a reference named at a database: the database,
// of the kind given, and no catalog, schema, path or overload.
func databaseReference(ref *protocol.Reference, kind string) bool {
	return ref != nil && ref.GetKind() == kind && reservedText(ref.GetDatabase()) && ref.GetCatalog() == "" && ref.GetSchema() == "" &&
		len(ref.GetPath()) == 0 && len(ref.GetOverload()) == 0
}

// databaseNamespace reports the namespace of a whole database: the database,
// an empty catalog and an empty schema, and no path prefix.
func databaseNamespace(scope *protocol.ModelNamespace) bool {
	return scope.GetDatabase() != nil && reservedText(scope.GetDatabase().GetNormalized()) &&
		scope.GetCatalog() != nil && scope.GetCatalog().GetNormalized() == "" &&
		scope.GetSchema() != nil && scope.GetSchema().GetNormalized() == "" && len(scope.GetPathPrefix()) == 0
}
