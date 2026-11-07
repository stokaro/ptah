package main

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// Inspect reads every table of the database file with its columns, its
// foreign keys, CHECKs and UNIQUE constraints, and its indexes. The
// constraints come from the table text the fixture and its tests write,
// `CONSTRAINT "<name>" CHECK (<expression>)`, `CONSTRAINT "<name>" FOREIGN KEY
// ("<column>") REFERENCES "<table>" ("<column>") [ON DELETE <rule>]` and
// `CONSTRAINT "<name>" UNIQUE ("<column>", ...)`, which SQLite keeps as
// written; the index SQLite builds for a UNIQUE is its own, and stays out of
// the indexes.
// SQLite keeps no NULLS order in an index key, so every key part is in the
// default order. A partial index's predicate is the text after WHERE in the
// statement SQLite keeps, the spelling the fixture builds an index with.
func (o *owner) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	for _, schema := range request.GetSchemas() {
		if schema != "main" && !o.otherSchemas {
			return nil, fmt.Errorf("fixture supports only the main schema")
		}
	}
	reads := o.negotiatedReads(ctx)
	unique := slices.Contains(reads, provider.ObservedUniqueConstraintsFeature)
	result := &protocol.Schema{Complete: true, Engine: "fixture", TargetFeatures: reads,
		Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED}}
	err := o.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		tables, err := storedTables(ctx, access.Querier, "table", "")
		if err != nil {
			return err
		}
		for _, stored := range tables {
			table, err := inspectTable(ctx, access, stored, unique)
			if err != nil {
				return err
			}
			result.Tables = append(result.Tables, table)
		}
		for _, selector := range request.GetSchemas() {
			bound := &protocol.NamespaceComponent{}
			if selector != "main" {
				bound = &protocol.NamespaceComponent{Source: selector, Normalized: selector}
			}
			result.ScopeBindings = append(result.ScopeBindings, &protocol.InspectionScopeBinding{Selector: selector, Namespaces: []*protocol.ModelNamespace{{
				Database: &protocol.NamespaceComponent{Source: access.Path, Normalized: access.Path}, Catalog: &protocol.NamespaceComponent{}, Schema: bound,
			}}})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// stored is a schema object SQLite keeps: its name and the statement it was
// created with.
type stored struct{ name, sql string }

// storedTables lists the tables, or the indexes of one table, SQLite keeps,
// leaving out its own and the indexes it made for a key.
func storedTables(ctx context.Context, db backend.Querier, kind, table string) ([]stored, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, sql FROM sqlite_schema WHERE type = ? AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL AND (? = '' OR tbl_name = ?) ORDER BY name",
		kind, table, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []stored
	for rows.Next() {
		var value stored
		if err := rows.Scan(&value.name, &value.sql); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func inspectTable(ctx context.Context, access backend.Access, table stored, unique bool) (*protocol.Table, error) {
	identity := &protocol.Reference{Engine: "fixture", Database: access.Path, Kind: "table", Name: table.name, DisplayName: table.name}
	normalized := proto.CloneOf(identity)
	normalized.Name = backend.NormalizeCommonIdentifier(table.name)
	result := &protocol.Table{Identity: identity, NormalizedIdentity: normalized}
	columns, err := inspectColumns(ctx, access.Querier, table.name)
	if err != nil {
		return nil, err
	}
	result.Columns = columns
	result.Constraints, err = tableConstraints(table, unique)
	if err != nil {
		return nil, err
	}
	// A column a UNIQUE over it alone covers is unique, as the contract asks.
	for _, constraint := range result.Constraints {
		if constraint.GetType() != "UNIQUE" || len(constraint.GetColumnNames()) != 1 {
			continue
		}
		for _, column := range result.Columns {
			if column.GetName() == constraint.GetColumnNames()[0] {
				column.Observed.Unique = true
			}
		}
	}
	result.Indexes, err = inspectIndexes(ctx, access.Querier, table.name)
	return result, err
}

func inspectColumns(ctx context.Context, db backend.Querier, table string) ([]*protocol.Column, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?) ORDER BY cid", table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*protocol.Column
	for rows.Next() {
		var name, kind string
		var notNull, primary int
		var defaultValue sql.NullString
		if err := rows.Scan(&name, &kind, &notNull, &defaultValue, &primary); err != nil {
			return nil, err
		}
		nullable := "YES"
		if notNull != 0 || primary != 0 {
			nullable = "NO"
		}
		observed := &protocol.ObservedColumn{Name: name, DataType: kind, FormattedType: kind, IsNullable: nullable, PrimaryKey: primary != 0, OrdinalPosition: int64(len(result) + 1)}
		if defaultValue.Valid {
			observed.DefaultExpression = &defaultValue.String
		}
		result = append(result, &protocol.Column{Name: name, Observed: observed})
	}
	return result, rows.Err()
}

func inspectIndexes(ctx context.Context, db backend.Querier, table string) ([]*protocol.ObservedIndex, error) {
	indexes, err := storedTables(ctx, db, "index", table)
	if err != nil {
		return nil, err
	}
	result := make([]*protocol.ObservedIndex, 0, len(indexes))
	for _, index := range indexes {
		observed := &protocol.ObservedIndex{Name: index.name, TableName: table, Unique: strings.HasPrefix(index.sql, "CREATE UNIQUE INDEX"),
			Condition: indexPredicate(index.sql)}
		rows, err := db.QueryContext(ctx, "SELECT name, \"desc\" FROM pragma_index_xinfo(?) WHERE key = 1 ORDER BY seqno", index.name)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var column string
			var descending int
			if err := rows.Scan(&column, &descending); err != nil {
				_ = rows.Close()
				return nil, err
			}
			observed.Columns = append(observed.Columns, column)
			observed.Parts = append(observed.Parts, &protocol.CapturedIndexPart{Column: column, Descending: descending != 0})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		result = append(result, observed)
	}
	return result, nil
}
