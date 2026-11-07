package provider

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// SQLStatementFactsFeature enables the statement facts of
// SQLSchemaDeclaration: how the server of an engine whose SQL documents the
// host reads with the shared grammar (the FRAGMENTS mode) treats a statement
// a schema document writes, which the host's reading of the document follows.
// drop_view_if_exists_skips_other_kinds says that DROP VIEW IF EXISTS of a
// name another kind of object holds does nothing; false, the zero value,
// says that the server refuses it as it refuses a DROP VIEW without IF
// EXISTS, and the reading refuses it too. A host that does not list the
// feature reads every fact as false. A two-way feature: the provider lists
// it, and so does each engine that states the facts. Contract 25.33.
const SQLStatementFactsFeature = "source.sql-statement-facts"

// validateSQLStatementFactsFeature refuses an engine that lists the feature
// without the provider's or reads SQL in the DOCUMENT mode, where its own
// grammar reads every statement and the host reads none, and an engine that
// states a fact without listing the feature.
func validateSQLStatementFactsFeature(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), SQLStatementFactsFeature)
	if listed && !slices.Contains(description.GetFeatures(), SQLStatementFactsFeature) {
		return sqlSchemaError("statement facts require the provider's " + SQLStatementFactsFeature)
	}
	if listed && engine.GetSqlSchema().GetMode() == protocol.SQLSchemaDeclaration_DOCUMENT {
		return sqlSchemaError("statement facts describe the documents of a FRAGMENTS engine")
	}
	if !listed && statesSQLStatementFacts(engine.GetSqlSchema()) {
		return sqlSchemaError("an engine states statement facts only when it lists " + SQLStatementFactsFeature)
	}
	return nil
}

// statesSQLStatementFacts reports whether a declaration states any statement
// fact.
func statesSQLStatementFacts(declaration *protocol.SQLSchemaDeclaration) bool {
	return declaration.GetDropViewIfExistsSkipsOtherKinds()
}

// negotiateSQLStatementFacts removes the statement facts unless the host lists
// SQLStatementFactsFeature.
func negotiateSQLStatementFacts(engine *protocol.Engine, hostFeatures []string) {
	if !slices.Contains(hostFeatures, SQLStatementFactsFeature) && engine.GetSqlSchema() != nil {
		engine.SqlSchema.DropViewIfExistsSkipsOtherKinds = false
	}
}
