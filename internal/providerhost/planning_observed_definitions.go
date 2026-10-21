package providerhost

import (
	"maps"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/catalog"
)

func observedPlanningColumn(source catalog.Column) (*protocol.ObservedColumn, error) {
	if err := onlyFields(
		source, "Facets", "Name", "DataType", "UDTName", "UDTSchema", "FormattedType", "ColumnType", "IsNullable", "ColumnDefault", "CharacterMaxLength",
		"Collate", "Comment", "NotNullConstraintName", "NumericPrecision", "NumericScale", "DatetimePrecision", "OrdinalPosition", "IsAutoIncrement",
		"IsPrimaryKey", "IsUnique", "TypeIsDeclaredText", "DomainName", "DomainSchema", "GeneratedExpression", "GeneratedKind", "IdentityGeneration",
		"IdentityStart", "IdentityIncrement", "SequenceRestart", "OwnedSequence",
	); err != nil {
		return nil, err
	}
	return &protocol.ObservedColumn{
		Name:                  source.Name,
		DataType:              source.DataType,
		UdtName:               source.UDTName,
		UdtSchema:             source.UDTSchema,
		FormattedType:         source.FormattedType,
		ColumnType:            source.ColumnType,
		IsNullable:            source.IsNullable,
		DefaultExpression:     copyPlanningPointer(source.ColumnDefault),
		CharacterMaxLength:    planningInt64Pointer(source.CharacterMaxLength),
		Collation:             source.Collate,
		Comment:               source.Comment,
		NotNullConstraintName: source.NotNullConstraintName,
		NumericPrecision:      planningInt64Pointer(source.NumericPrecision),
		NumericScale:          planningInt64Pointer(source.NumericScale),
		DatetimePrecision:     planningInt64Pointer(source.DatetimePrecision),
		OrdinalPosition:       int64(source.OrdinalPosition),
		AutoIncrement:         source.IsAutoIncrement,
		PrimaryKey:            source.IsPrimaryKey,
		Unique:                source.IsUnique,
		TypeIsDeclaredText:    source.TypeIsDeclaredText,
		DomainName:            source.DomainName,
		DomainSchema:          source.DomainSchema,
		GeneratedExpression:   copyPlanningPointer(source.GeneratedExpression),
		GeneratedKind:         source.GeneratedKind,
		IdentityGeneration:    source.IdentityGeneration,
		IdentityStart:         source.IdentityStart,
		IdentityIncrement:     source.IdentityIncrement,
		SequenceRestart:       source.SequenceRestart,
		OwnedSequence:         source.OwnedSequence,
	}, nil
}

func observedPlanningIndex(source catalog.Index) (*protocol.ObservedIndex, error) {
	if err := onlyFields(
		source, "Facets", "ManagedBy", "Name", "TableName", "Schema", "Columns", "Parts", "IsUnique", "IsPrimary", "Definition", "KeyPartsIncomplete",
		"Condition", "Comment", "Invisible", "NullsDistinct", "Method", "IncludeColumns", "StorageParams", "Requires", "PartitionAttached", "HashShardBuckets",
	); err != nil {
		return nil, err
	}
	return &protocol.ObservedIndex{
		ManagedBy:          optionalPlanningIdentity(source.ManagedBy),
		Name:               source.Name,
		TableName:          source.TableName,
		Schema:             source.Schema,
		Columns:            slices.Clone(source.Columns),
		Parts:              observedPlanningIndexParts(source.Parts),
		Unique:             source.IsUnique,
		Primary:            source.IsPrimary,
		Definition:         source.Definition,
		KeyPartsIncomplete: source.KeyPartsIncomplete,
		Condition:          source.Condition,
		Comment:            source.Comment,
		Invisible:          source.Invisible,
		NullsDistinct:      copyPlanningPointer(source.NullsDistinct),
		Method:             source.Method,
		IncludeColumns:     slices.Clone(source.IncludeColumns),
		StorageParameters:  maps.Clone(source.StorageParams),
		Requires:           comparisonIdentities(source.Requires),
		PartitionAttached:  source.PartitionAttached,
		HashShardBuckets:   int64(source.HashShardBuckets),
	}, nil
}

func observedPlanningConstraint(source catalog.Constraint) (*protocol.ObservedConstraint, error) {
	if err := onlyFields(
		source, "Facets", "Name", "TableName", "Schema", "Type", "ColumnName", "ColumnNames", "ForeignTable", "ForeignSchema", "ForeignColumn",
		"ForeignColumns", "DeleteRule", "UpdateRule", "Deferrable", "Initially", "Match", "NotEnforced", "NotValid", "CheckClause", "NullsDistinct",
		"IncludeColumns", "UsingMethod", "Requires", "Comment", "HashShardBuckets",
	); err != nil {
		return nil, err
	}
	return &protocol.ObservedConstraint{
		Name:             source.Name,
		TableName:        source.TableName,
		Schema:           source.Schema,
		Type:             source.Type,
		ColumnName:       source.ColumnName,
		ColumnNames:      slices.Clone(source.ColumnNames),
		ForeignTable:     copyPlanningPointer(source.ForeignTable),
		ForeignSchema:    source.ForeignSchema,
		ForeignColumn:    copyPlanningPointer(source.ForeignColumn),
		ForeignColumns:   slices.Clone(source.ForeignColumns),
		DeleteRule:       copyPlanningPointer(source.DeleteRule),
		UpdateRule:       copyPlanningPointer(source.UpdateRule),
		Deferrable:       source.Deferrable,
		Initially:        source.Initially,
		Match:            source.Match,
		NotEnforced:      source.NotEnforced,
		NotValid:         source.NotValid,
		CheckClause:      copyPlanningPointer(source.CheckClause),
		NullsDistinct:    copyPlanningPointer(source.NullsDistinct),
		IncludeColumns:   slices.Clone(source.IncludeColumns),
		UsingMethod:      copyPlanningPointer(source.UsingMethod),
		Requires:         comparisonIdentities(source.Requires),
		Comment:          source.Comment,
		HashShardBuckets: int64(source.HashShardBuckets),
	}, nil
}

func observedPlanningTrigger(source catalog.Trigger) (*protocol.CapturedTrigger, error) {
	if err := onlyFields(source, "Facets", "Name", "Schema", "Table", "Timing", "Event", "ForEach", "Body", "Comment", "When", "OldTable", "NewTable", "ExecuteFunction"); err != nil {
		return nil, err
	}
	return &protocol.CapturedTrigger{
		Name:            source.Name,
		Schema:          source.Schema,
		Table:           source.Table,
		Timing:          source.Timing,
		Event:           source.Event,
		ForEach:         source.ForEach,
		Body:            source.Body,
		Comment:         source.Comment,
		When:            source.When,
		OldTable:        source.OldTable,
		NewTable:        source.NewTable,
		ExecuteFunction: source.ExecuteFunction,
	}, nil
}
