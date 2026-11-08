package provider

import (
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// SQLUndeclaredFeature enables SQLDiagnostic.Code UNDECLARED in a
// ReadSQLSchema reply: an error for a statement that changes an object no
// document of the load declares, such as an ALTER TABLE of a table no CREATE
// TABLE of the load creates. The host reads it as a statement its load has no
// place for, and a load that sits beside a source in another format is told
// that its documents change only what its documents declare, as the host's
// own SQL reading tells it. Without the feature such a statement is an
// UNSUPPORTED or INVALID error, and the host cannot say why. A two-way
// feature: the provider lists it, and so does each engine whose SQL reading
// reports it, beside ReadSQLSchemaFeature. Contract 25.38.
const SQLUndeclaredFeature = "schema.read-sql-undeclared"

// validateSQLUndeclaredFeature refuses an engine that lists the feature
// without the provider's or without reading SQL documents itself.
func validateSQLUndeclaredFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), SQLUndeclaredFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), SQLUndeclaredFeature) {
		return sqlSchemaError("undeclared statements require the provider's " + SQLUndeclaredFeature)
	}
	if !slices.Contains(engine.GetFeatures(), ReadSQLSchemaFeature) {
		return sqlSchemaError("undeclared statements require the engine's SQL reading")
	}
	return nil
}

// sqlUndeclaredNegotiated reports whether a reading of request may report an
// UNDECLARED diagnostic: the provider and the request's engine list the
// feature, and so does the request's target.
func sqlUndeclaredNegotiated(description *protocol.Description, request *protocol.ReadSQLSchemaRequest) bool {
	return EngineOffers(description, request.GetEngine(), SQLUndeclaredFeature) && slices.Contains(request.GetTargetFeatures(), SQLUndeclaredFeature)
}
