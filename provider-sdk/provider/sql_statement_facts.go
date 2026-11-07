package provider

// SQLStatementFactsFeature enables the statement facts of
// SQLSchemaDeclaration, which say how a FRAGMENTS engine's server reads a
// statement of a schema document, such as
// drop_view_if_exists_skips_other_kinds. A two-way feature: the provider
// lists it, and so does each engine that states the facts. Contract 25.33.
const SQLStatementFactsFeature = "source.sql-statement-facts"
