package providerhost

import (
	"maps"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/schemamodel"
)

func declaredPlanningColumn(source schemamodel.Field) (*protocol.DeclaredColumn, error) {
	if err := onlyFields(
		source, "Facets", "StructName", "FieldName", "APIName", "APINames", "APIType", "APIExpose", "GeneratedFromEmbedded", "Name", "Type", "TypeRawSQL",
		"TypeIsDeclaredText", "Nullable", "Primary", "AutoInc", "IdentityGeneration", "IdentityStart", "IdentityIncrement", "IdentityOptions", "Unique",
		"UniqueExpr", "Default", "DefaultSet", "DefaultExpr", "Foreign", "ForeignKeyName", "OnDelete", "OnUpdate", "Deferrable", "Initially",
		"ForeignKeyMatch", "ForeignKeyNotEnforced", "Enum", "Check", "CheckName", "CheckNotEnforced", "NotNullConstraintName", "GeneratedExpression",
		"GeneratedKind", "Collate", "Comment",
	); err != nil {
		return nil, err
	}
	return &protocol.DeclaredColumn{
		Name:                  source.Name,
		Type:                  source.Type,
		TypeRawSql:            source.TypeRawSQL,
		TypeIsDeclaredText:    source.TypeIsDeclaredText,
		Nullable:              source.Nullable,
		Primary:               source.Primary,
		AutoIncrement:         source.AutoInc,
		IdentityGeneration:    source.IdentityGeneration,
		IdentityStart:         source.IdentityStart,
		IdentityIncrement:     source.IdentityIncrement,
		IdentityOptions:       source.IdentityOptions,
		Unique:                source.Unique,
		UniqueExpression:      source.UniqueExpr,
		DefaultValue:          source.Default,
		DefaultSet:            source.DefaultSet,
		DefaultExpression:     source.DefaultExpr,
		ForeignReference:      source.Foreign,
		ForeignKeyName:        source.ForeignKeyName,
		OnDelete:              source.OnDelete,
		OnUpdate:              source.OnUpdate,
		Deferrable:            source.Deferrable,
		Initially:             source.Initially,
		ForeignKeyMatch:       source.ForeignKeyMatch,
		ForeignKeyNotEnforced: source.ForeignKeyNotEnforced,
		EnumValues:            slices.Clone(source.Enum),
		Check:                 source.Check,
		CheckName:             source.CheckName,
		CheckNotEnforced:      source.CheckNotEnforced,
		NotNullConstraintName: source.NotNullConstraintName,
		GeneratedExpression:   source.GeneratedExpression,
		GeneratedKind:         source.GeneratedKind,
		Collation:             source.Collate,
		Comment:               source.Comment,
	}, nil
}

func declaredPlanningConstraint(source schemamodel.Constraint) (*protocol.DeclaredConstraint, error) {
	if err := onlyFields(
		source, "Facets", "StructName", "Name", "Type", "Table", "UsingMethod", "CheckExpression", "Columns", "IncludeColumns", "NullsDistinct",
		"ForeignTable", "ForeignColumn", "ForeignColumns", "OnDelete", "OnUpdate", "Deferrable", "Initially", "Match", "NotEnforced", "NotValid", "Requires",
		"Comment",
	); err != nil {
		return nil, err
	}
	return &protocol.DeclaredConstraint{
		Name:            source.Name,
		Type:            source.Type,
		Table:           source.Table,
		UsingMethod:     source.UsingMethod,
		CheckExpression: source.CheckExpression,
		Columns:         slices.Clone(source.Columns),
		IncludeColumns:  slices.Clone(source.IncludeColumns),
		NullsDistinct:   copyPlanningPointer(source.NullsDistinct),
		ForeignTable:    source.ForeignTable,
		ForeignColumn:   source.ForeignColumn,
		ForeignColumns:  slices.Clone(source.ForeignColumns),
		OnDelete:        source.OnDelete,
		OnUpdate:        source.OnUpdate,
		Deferrable:      source.Deferrable,
		Initially:       source.Initially,
		Match:           source.Match,
		NotEnforced:     source.NotEnforced,
		NotValid:        source.NotValid,
		Requires:        comparisonIdentities(source.Requires),
		Comment:         source.Comment,
	}, nil
}

func declaredPlanningIndex(source schemamodel.Index) (*protocol.DeclaredIndex, error) {
	if err := onlyFields(
		source, "Facets", "StructName", "Name", "Fields", "Parts", "Unique", "Comment", "Invisible", "NullsDistinct", "Type", "Condition", "Concurrently",
		"Operator", "IncludeColumns", "StorageParams", "TableName", "Requires",
	); err != nil {
		return nil, err
	}
	return &protocol.DeclaredIndex{
		Name:              source.Name,
		Fields:            slices.Clone(source.Fields),
		Parts:             declaredPlanningIndexParts(source.Parts),
		Unique:            source.Unique,
		Comment:           source.Comment,
		Invisible:         source.Invisible,
		NullsDistinct:     copyPlanningPointer(source.NullsDistinct),
		Type:              source.Type,
		Condition:         source.Condition,
		Concurrently:      source.Concurrently,
		Operator:          source.Operator,
		IncludeColumns:    slices.Clone(source.IncludeColumns),
		StorageParameters: maps.Clone(source.StorageParams),
		TableName:         source.TableName,
		Requires:          comparisonIdentities(source.Requires),
	}, nil
}

func declaredPlanningEnum(source schemamodel.Enum) (*protocol.DeclaredEnum, error) {
	if err := onlyFields(source, "Facets", "Name", "Schema", "Values", "Comment"); err != nil {
		return nil, err
	}
	return &protocol.DeclaredEnum{
		Name:    source.Name,
		Schema:  source.Schema,
		Values:  slices.Clone(source.Values),
		Comment: source.Comment,
	}, nil
}

func declaredPlanningTrigger(source schemamodel.Trigger) (*protocol.CapturedTrigger, error) {
	if err := onlyFields(source, "Facets", "StructName", "Name", "Table", "Timing", "Event", "ForEach", "Body", "Comment", "When", "OldTable", "NewTable", "ExecuteFunction", "Dialects"); err != nil {
		return nil, err
	}
	return &protocol.CapturedTrigger{
		Name:            source.Name,
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
		Targets:         slices.Clone(source.Dialects),
	}, nil
}
