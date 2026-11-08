package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ViewQueryContextFeature enables CompareViewQueriesRequest.declared_columns
// and ComparedViewQuery.undecided. A server that rewrites a view's query, as
// Spanner names every column of a `SELECT *` and qualifies the columns of a
// UNION, keeps a query its owner cannot match from the two texts alone: the
// host sends the columns of every declared table, and an owner that still
// cannot decide a pair leaves it to the host's reading of the texts. A
// two-way feature: the host lists it, and so do the provider and each engine
// that compares view queries under it. Contract 25.37.
const ViewQueryContextFeature = "schema.view-query-context"

// validateViewQueryContextEngine refuses an engine that lists
// ViewQueryContextFeature without the provider's or without
// ViewQueryEquivalenceFeature, whose call the feature adds to.
func validateViewQueryContextEngine(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ViewQueryContextFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ViewQueryContextFeature) || !slices.Contains(engine.GetFeatures(), ViewQueryEquivalenceFeature) {
		return fmt.Errorf("%w: engine %s requires the provider's and %s", ErrProtocol, ViewQueryContextFeature, ViewQueryEquivalenceFeature)
	}
	return nil
}

// viewQueryContext reports whether the engine and the request's target
// negotiated ViewQueryContextFeature.
func viewQueryContext(description *protocol.Description, request *protocol.CompareViewQueriesRequest) bool {
	return EngineOffers(description, request.GetEngine(), ViewQueryContextFeature) && slices.Contains(request.GetTargetFeatures(), ViewQueryContextFeature)
}

// validateViewQueryColumns checks the declared columns of a request: sent
// only where ViewQueryContextFeature was negotiated, each named by a valid
// table identity, with a name and a type, and no column of a table named
// twice.
func validateViewQueryColumns(description *protocol.Description, request *protocol.CompareViewQueriesRequest) error {
	columns := request.GetDeclaredColumns()
	if len(columns) == 0 {
		return nil
	}
	if !viewQueryContext(description, request) {
		return viewQueryError("declared columns need " + ViewQueryContextFeature + " on both sides")
	}
	type tableColumn struct {
		table *protocol.ComparisonIdentity
		name  string
	}
	seen := make([]tableColumn, 0, len(columns))
	for _, column := range columns {
		if err := ValidateComparisonIdentity(column.GetTable()); err != nil {
			return err
		}
		if column.GetTable().GetKind() != "table" {
			return viewQueryError(fmt.Sprintf("declared column %q names a %q, not a table", column.GetName(), column.GetTable().GetKind()))
		}
		if column.GetName() == "" || column.GetType() == "" {
			return viewQueryError("a declared column requires a name and a type")
		}
		if slices.ContainsFunc(seen, func(other tableColumn) bool {
			return other.name == column.GetName() && CompareIdentities(other.table, column.GetTable()) == 0
		}) {
			return viewQueryError(fmt.Sprintf("declared column %q of table %q is named twice", column.GetName(), column.GetTable().GetName().GetSource()))
		}
		seen = append(seen, tableColumn{table: column.GetTable(), name: column.GetName()})
	}
	return nil
}

// validateUndecidedViewQueries checks the undecided answers of a reply: given
// only where ViewQueryContextFeature was negotiated, and never beside an
// equivalent answer.
func validateUndecidedViewQueries(description *protocol.Description, request *protocol.CompareViewQueriesRequest, result *protocol.ComparedViewQueries) error {
	negotiated := viewQueryContext(description, request)
	for i, answer := range result.GetQueries() {
		if !answer.GetUndecided() {
			continue
		}
		if !negotiated {
			return viewQueryError(fmt.Sprintf("answer %d is undecided, which needs %s on both sides", i, ViewQueryContextFeature))
		}
		if answer.GetEquivalent() {
			return viewQueryError(fmt.Sprintf("answer %d is undecided and equivalent", i))
		}
	}
	return nil
}
