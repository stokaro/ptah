package providerhost

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
)

func (c *planningCapture) commonStep(source featureplan.CommonStep) (*protocol.CommonPlanningStep, error) {
	if err := onlyFields(source, "ID", "Parent", "Effects", "Transaction", "Impact", "AddedColumn"); err != nil {
		return nil, err
	}
	metadata, err := planningMetadata(source)
	if err != nil {
		return nil, err
	}
	result := &protocol.CommonPlanningStep{Id: planningStepID(source.ID), Parent: optionalPlanningIdentity(source.Parent), Metadata: metadata}
	if source.AddedColumn != nil {
		result.AddedColumn, err = c.columnOperand(source.AddedColumn)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (c *planningCapture) columnOperand(source *ast.ColumnNode) (*protocol.PlanningColumn, error) {
	if err := onlyFields(
		*source, "Facets", "Name", "Type", "TypeRawSQL", "TypeIsDeclaredText", "EnumType", "Nullable", "Primary", "Unique", "AutoInc", "IdentityGeneration",
		"IdentityStart", "IdentityIncrement", "IdentityOptions", "Default", "Check", "CheckName", "CheckNotEnforced", "NotNullConstraintName",
		"GeneratedExpression", "GeneratedKind", "Collate", "Comment", "ForeignKey",
	); err != nil {
		return nil, err
	}
	result := &protocol.PlanningColumn{
		Name:                  source.Name,
		Type:                  source.Type,
		TypeRawSql:            source.TypeRawSQL,
		TypeIsDeclaredText:    source.TypeIsDeclaredText,
		EnumType:              source.EnumType,
		Nullable:              source.Nullable,
		Primary:               source.Primary,
		Unique:                source.Unique,
		AutoIncrement:         source.AutoInc,
		IdentityGeneration:    source.IdentityGeneration,
		IdentityStart:         source.IdentityStart,
		IdentityIncrement:     source.IdentityIncrement,
		IdentityOptions:       source.IdentityOptions,
		Check:                 source.Check,
		CheckName:             source.CheckName,
		CheckNotEnforced:      source.CheckNotEnforced,
		NotNullConstraintName: source.NotNullConstraintName,
		GeneratedExpression:   source.GeneratedExpression,
		GeneratedKind:         source.GeneratedKind,
		Collation:             source.Collate,
		Comment:               source.Comment,
	}
	var err error
	result.Facets, err = c.operandFacets(source.Facets)
	if err != nil {
		return nil, err
	}
	if source.Default != nil {
		if err := onlyFields(*source.Default, "Value", "ValueSet", "Expression"); err != nil {
			return nil, err
		}
		result.Default = &protocol.PlanningDefault{Value: source.Default.Value, ValueSet: source.Default.ValueSet, Expression: source.Default.Expression}
	}
	if source.ForeignKey != nil {
		result.ForeignKey, err = c.foreignKeyOperand(source.ForeignKey)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (c *planningCapture) foreignKeyOperand(source *ast.ForeignKeyRef) (*protocol.PlanningForeignKey, error) {
	if err := onlyFields(*source, "Table", "Column", "Columns", "OnDelete", "OnUpdate", "Name", "Deferrable", "Initially", "Match", "NotEnforced", "Facets"); err != nil {
		return nil, err
	}
	result := &protocol.PlanningForeignKey{
		Table:       source.Table,
		Column:      source.Column,
		Columns:     slices.Clone(source.Columns),
		OnDelete:    source.OnDelete,
		OnUpdate:    source.OnUpdate,
		Name:        source.Name,
		Deferrable:  source.Deferrable,
		Initially:   source.Initially,
		Match:       source.Match,
		NotEnforced: source.NotEnforced,
	}
	var err error
	result.Facets, err = c.operandFacets(source.Facets)
	return result, err
}
