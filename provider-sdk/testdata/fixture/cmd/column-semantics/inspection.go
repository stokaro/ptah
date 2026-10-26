package main

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func (c *columns) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := c.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Features = []string{provider.ObservedColumnsFeature}
	return session, nil
}

func (c *columns) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := c.Provider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot.TargetFeatures = []string{provider.ObservedColumnsFeature}
	for _, table := range snapshot.Tables {
		for i, column := range table.Columns {
			nullable := "NO"
			if column.Nullable {
				nullable = "YES"
			}
			observed := &protocol.ObservedColumn{Name: column.Name, DataType: column.Type, FormattedType: column.Type, IsNullable: nullable, PrimaryKey: column.PrimaryKey, DefaultExpression: column.DefaultExpression, OrdinalPosition: int64(i + 1)}
			column.Observed = observed
			column.Type = ""
			column.Nullable = false
			column.PrimaryKey = false
			column.DefaultExpression = nil
			switch table.Identity.Name {
			case "column_metadata":
				observed.DataType = "Int64"
				observed.FormattedType = "INT64"
				observed.ColumnType = "signed integer"
				observed.AutoIncrement = true
				observed.IdentityGeneration = "BY DEFAULT"
				observed.IdentityStart = "5"
				observed.IdentityIncrement = "2"
				observed.SequenceRestart = "11"
				observed.OwnedSequence = "private_sequence"
				observed.Comment = "captured column"
				observed.NotNullConstraintName = "id_present"
				observed.Unique = true
				observed.NumericPrecision = new(int64(19))
				observed.NumericScale = new(int64(0))
				observed.DatetimePrecision = new(int64(6))
				observed.CharacterMaxLength = new(int64(42))
				observed.UdtName = "int8"
				observed.UdtSchema = "types"
				observed.DomainName = "identifier"
				observed.DomainSchema = "domain_schema"
				observed.Collation = "case_sensitive"
				observed.TypeIsDeclaredText = true
			case "column_generated":
				observed.GeneratedExpression = new("lower(source)")
				observed.GeneratedKind = "STORED"
				observed.DefaultExpression = new("typed()")
			case "column_missing":
				column.Observed = nil
			case "column_legacy":
				column.Type = "INTEGER"
			case "column_name":
				observed.Name = "foreign"
			case "column_ordinal":
				observed.OrdinalPosition = 2
			case "column_nullable":
				observed.IsNullable = ""
			}
		}
	}
	return snapshot, nil
}
