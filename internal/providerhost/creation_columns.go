package providerhost

import (
	"fmt"
	"math"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
)

func projectedColumn(source *protocol.ObservedColumn) (catalog.Column, error) {
	result := catalog.Column{
		Name: source.GetName(), DataType: source.GetDataType(), UDTName: source.GetUdtName(), UDTSchema: source.GetUdtSchema(),
		FormattedType: source.GetFormattedType(), ColumnType: source.GetColumnType(), IsNullable: source.GetIsNullable(),
		ColumnDefault: copyPlanningPointer(source.DefaultExpression), Collate: source.GetCollation(), Comment: source.GetComment(),
		NotNullConstraintName: source.GetNotNullConstraintName(), IsAutoIncrement: source.GetAutoIncrement(), IsPrimaryKey: source.GetPrimaryKey(),
		IsUnique: source.GetUnique(), TypeIsDeclaredText: source.GetTypeIsDeclaredText(), DomainName: source.GetDomainName(), DomainSchema: source.GetDomainSchema(),
		GeneratedExpression: copyPlanningPointer(source.GeneratedExpression), GeneratedKind: source.GetGeneratedKind(),
		IdentityGeneration: source.GetIdentityGeneration(), IdentityStart: source.GetIdentityStart(), IdentityIncrement: source.GetIdentityIncrement(),
		SequenceRestart: source.GetSequenceRestart(), OwnedSequence: source.GetOwnedSequence(),
	}
	for _, pair := range []struct {
		source *int64
		target **int
	}{
		{source.CharacterMaxLength, &result.CharacterMaxLength}, {source.NumericPrecision, &result.NumericPrecision},
		{source.NumericScale, &result.NumericScale}, {source.DatetimePrecision, &result.DatetimePrecision},
	} {
		if pair.source != nil {
			value, err := projectedColumnInteger(*pair.source)
			if err != nil {
				return catalog.Column{}, err
			}
			*pair.target = new(value)
		}
	}
	var err error
	result.OrdinalPosition, err = projectedColumnInteger(source.GetOrdinalPosition())
	if err != nil {
		return catalog.Column{}, err
	}
	return result, nil
}

func projectedColumnInteger(value int64) (int, error) {
	if value < math.MinInt || value > math.MaxInt {
		return 0, fmt.Errorf("%w: column metadata exceeds the host integer range", provider.ErrProtocol)
	}
	return int(value), nil // #nosec G115 -- checked against the host integer range above
}
