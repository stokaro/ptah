package providerhost

import (
	"fmt"
	"maps"
	"math"
	"slices"

	"ptah.run/catalog"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func catalogColumn(source *protocol.ObservedColumn) (catalog.Column, error) {
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

// catalogIndex is the host's reading of a common index a provider reports in
// inspection. The SDK has checked it against its table: its name, its key and
// its columns.
func catalogIndex(source *protocol.ObservedIndex) (catalog.Index, error) {
	shards := source.GetHashShardBuckets()
	if shards > math.MaxInt {
		return catalog.Index{}, fmt.Errorf("%w: index shard count exceeds the host integer range", provider.ErrProtocol)
	}
	result := catalog.Index{
		Name: source.GetName(), TableName: source.GetTableName(), Schema: source.GetSchema(), Columns: slices.Clone(source.GetColumns()),
		IsUnique: source.GetUnique(), IsPrimary: source.GetPrimary(), Definition: source.GetDefinition(), KeyPartsIncomplete: source.GetKeyPartsIncomplete(),
		Condition: source.GetCondition(), Comment: source.GetComment(), Invisible: source.GetInvisible(), NullsDistinct: copyPlanningPointer(source.NullsDistinct),
		Method: source.GetMethod(), IncludeColumns: slices.Clone(source.GetIncludeColumns()), StorageParams: maps.Clone(source.GetStorageParameters()),
		PartitionAttached: source.GetPartitionAttached(), HashShardBuckets: int(shards),
	}
	if source.GetManagedBy() != nil {
		result.ManagedBy = boundIdentity(source.GetManagedBy())
	}
	for _, part := range source.GetParts() {
		result.Parts = append(result.Parts, catalog.IndexPart{Name: part.GetColumn(), Expr: part.GetExpression(), Operator: part.GetOperatorClass(),
			Prefix: part.GetPrefix(), Desc: part.GetDescending(), NullsOrder: part.GetNulls()})
	}
	for _, required := range source.GetRequires() {
		result.Requires = append(result.Requires, boundIdentity(required))
	}
	return result, nil
}
