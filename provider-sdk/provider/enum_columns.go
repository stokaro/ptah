package provider

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// EnumColumnsFeature enables Engine.enum_columns: how the engine builds a
// column a source declares as an enum, typed ENUM with its values or typed
// with the name of an enum the source declares. The engine states one form:
// VALUES_TYPE, a column whose type lists the values; CHECK, a column of
// check_type that holds the values under its CHECK; or REFUSED, no enum at
// all. A host builds every enum column of the engine's declarations in that
// form before it validates, compares, plans or renders them, so the provider
// receives an ordinary column and never an enum type, or refuses the
// declaration by table and column before any call. A host that does not list
// the feature keeps the enum a type of its own, as it does for an engine of
// contract 25.30. It is a two-way feature: the provider and the engine list
// it, and an engine states enum_columns exactly when it lists it. Contract
// 25.31.
const EnumColumnsFeature = "schema.enum-columns"

// validateEnumColumns checks an engine's enum columns: stated exactly when
// the engine lists the feature, with the provider's feature, in one of the
// three forms, and with a column type exactly for a CHECK.
func validateEnumColumns(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), EnumColumnsFeature)
	columns := engine.GetEnumColumns()
	if listed != (columns != nil) {
		return fmt.Errorf("%w: an engine states its enum columns exactly when it lists %s", ErrProtocol, EnumColumnsFeature)
	}
	if !listed {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), EnumColumnsFeature) {
		return fmt.Errorf("%w: enum columns require the provider's feature", ErrProtocol)
	}
	checkType := columns.GetCheckType()
	switch columns.GetForm() {
	case protocol.EnumColumns_CHECK:
		if checkType == "" || !utf8.ValidString(checkType) || strings.TrimSpace(checkType) != checkType || strings.ContainsAny(checkType, "\r\n") {
			return fmt.Errorf("%w: enum columns of engine %s are CHECK columns and need a column type on one line", ErrProtocol, engine.GetId())
		}
	case protocol.EnumColumns_VALUES_TYPE, protocol.EnumColumns_REFUSED:
		if checkType != "" {
			return fmt.Errorf("%w: enum columns of engine %s state a CHECK column type for a form without a CHECK", ErrProtocol, engine.GetId())
		}
	default:
		return fmt.Errorf("%w: enum columns of engine %s state no form", ErrProtocol, engine.GetId())
	}
	return nil
}

// negotiateEnumColumns removes the engine's enum columns unless the host
// lists EnumColumnsFeature.
func negotiateEnumColumns(engine *protocol.Engine, hostFeatures []string) {
	if !slices.Contains(hostFeatures, EnumColumnsFeature) {
		engine.EnumColumns = nil
	}
}
