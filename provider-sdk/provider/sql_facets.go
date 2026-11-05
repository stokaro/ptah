package provider

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// SQLFacetsFeature enables SQLSchema.facets: a SQL schema reading carries the
// DESIRED facets of the tables it declares and of their columns and indexes,
// such as a YDB table's changefeeds, which a YAML or Go source of the same
// engine carries as facets too (ADR 0066). An engine that declares the DOCUMENT mode
// lists it beside ReadSQLSchemaFeature, the provider lists it, and the host
// lists it in its host features. Contract 25.23.
//
// A facet is an Envelope of a DESIRED model whose source formats include SQL.
// Its owner is a table of SQLSchema.tables, named by kind "table", the
// table's schema and its name, or a column or an index of one, named by kind
// "column" or "index" below its table as ColumnReference names a column. The reply's coverage covers its kind, and one SQLObjectTrace of
// collection FACETS points at the statement that declares it.
const SQLFacetsFeature = "schema.sql-facets"

// validateSQLFacetsFeature requires an engine that lists the feature to read
// its documents whole, and the provider to list the feature too.
func validateSQLFacetsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), SQLFacetsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), SQLFacetsFeature) || engine.GetSqlSchema().GetMode() != protocol.SQLSchemaDeclaration_DOCUMENT {
		return sqlSchemaError("facets require the provider's feature and the DOCUMENT mode")
	}
	return nil
}

// validateSQLFacets checks a complete reply's facets: negotiated for the
// request, each a DESIRED value of a covered SQL model of the engine, owned by
// a declared table or one of its columns or indexes, and at most one of a kind
// for an owner.
func validateSQLFacets(description *protocol.Description, engine *protocol.Engine, features map[string]bool, request *protocol.ReadSQLSchemaRequest, result *protocol.SQLSchema) error {
	if len(result.GetFacets()) == 0 {
		return nil
	}
	if !EngineOffers(description, engine.GetId(), SQLFacetsFeature) || !features[SQLFacetsFeature] {
		return sqlSchemaError("facets were not negotiated")
	}
	covered := make(map[string]bool)
	for _, record := range result.GetCoverage().GetKinds() {
		covered[record.GetModel().GetKind()] = true
	}
	owners := sqlFacetOwners(request.GetIdentifiers(), result.GetTables())
	seen := make(map[string]bool)
	for _, facet := range result.GetFacets() {
		if err := validateValueEnvelope(description.GetProviderId(), engine.GetId(), features, facet); err != nil {
			return err
		}
		model := findValueModel(description, engine.GetId(), facet.GetKind(), protocol.ValueInput_DESIRED)
		if !envelopeMatchesModel(facet, model) || !slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_SQL) || !covered[facet.GetKind()] {
			return sqlSchemaError("a facet is outside the negotiated and covered SQL models")
		}
		owner, ok := owners.key(request.GetIdentifiers(), engine.GetId(), facet.GetOwner())
		if !ok {
			return sqlSchemaError("a facet must belong to a declared table, column or index")
		}
		if seen[facet.GetKind()+"\x00"+owner] {
			return sqlSchemaError("an owner holds two facets of one kind")
		}
		seen[facet.GetKind()+"\x00"+owner] = true
	}
	return nil
}

// sqlFacetOwnerSet holds the declared tables and their columns and indexes,
// keyed under the identifier rules: a table by its schema and name, a column
// or an index by its table's key and its name.
type sqlFacetOwnerSet map[string]bool

// sqlFacetOwners returns the owners the declared tables offer a facet.
func sqlFacetOwners(identifiers *protocol.ComparisonIdentifiers, tables []*protocol.DeclaredTable) sqlFacetOwnerSet {
	owners := make(sqlFacetOwnerSet)
	rules := identifiers.GetRules()
	for _, table := range tables {
		key := sqlTableKey(identifiers, table.GetSchema(), table.GetName())
		owners["table\x00"+key] = true
		for _, column := range table.GetColumns() {
			owners["column\x00"+key+"\x00"+sqlFold(rules.GetColumnNames(), column.GetName())] = true
		}
		for _, index := range table.GetIndexes() {
			owners["index\x00"+key+"\x00"+sqlFold(rules.GetIndexNames(), index.GetName())] = true
		}
	}
	return owners
}

// key returns the owner's key when ref names a declared table or a column or
// an index of one, of the engine and with nothing beyond a schema, a table and
// the child's name.
func (o sqlFacetOwnerSet) key(identifiers *protocol.ComparisonIdentifiers, engine string, ref *protocol.Reference) (string, bool) {
	if ref.GetEngine() != engine || ref.GetDatabase() != "" || ref.GetCatalog() != "" || len(ref.GetOverload()) != 0 || !comparisonReason(ref.GetName()) {
		return "", false
	}
	var key string
	switch {
	case ref.GetKind() == "table" && len(ref.GetPath()) == 0:
		key = "table\x00" + sqlTableKey(identifiers, ref.GetSchema(), ref.GetName())
	case ref.GetKind() == "column" && len(ref.GetPath()) == 1:
		key = "column\x00" + sqlTableKey(identifiers, ref.GetSchema(), ref.GetPath()[0]) + "\x00" + sqlFold(identifiers.GetRules().GetColumnNames(), ref.GetName())
	case ref.GetKind() == "index" && len(ref.GetPath()) == 1:
		key = "index\x00" + sqlTableKey(identifiers, ref.GetSchema(), ref.GetPath()[0]) + "\x00" + sqlFold(identifiers.GetRules().GetIndexNames(), ref.GetName())
	default:
		return "", false
	}
	return key, o[key]
}

// sqlTableKey is a table's schema and name under the table rule, with an
// empty schema read as the default one.
func sqlTableKey(identifiers *protocol.ComparisonIdentifiers, schema, name string) string {
	if schema == "" {
		schema = identifiers.GetDefaultSchema()
	}
	rule := identifiers.GetRules().GetTableNames()
	return sqlFold(rule, schema) + "\x00" + sqlFold(rule, name)
}
