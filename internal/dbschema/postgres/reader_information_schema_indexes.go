package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// informationSchemaIndexQuery reads indexes from the SQL-standard catalog
// instead of PostgreSQL's own.
//
// It exists for a server that speaks the PostgreSQL WIRE without implementing
// pg_catalog. Measured against a live Spanner endpoint through PGAdapter, the
// PostgreSQL-shaped read stops at its first join:
//
//	failed to read indexes: relation "pg_am" does not exist
//
// and removing that join moves it one step to
// `function pg_get_indexdef(bigint, bigint, boolean) does not exist`. The query
// is pg_index/pg_class/pg_am/pg_get_indexdef throughout, so there is nothing to
// degrade gracefully -- the read has to ask a different catalog
// (stokaro/ptah#942).
//
// A key part is a row with an ordinal position. A payload column has none:
// Spanner reports a STORING column with a NULL ordinal_position and a NULL
// ordering, and counting it as a key part would report a key the table does not
// have and plan a rebuild on every run.
const informationSchemaIndexQuery = `
		SELECT
			i.table_name,
			i.index_name,
			i.index_type,
			i.is_unique,
			c.column_name,
			c.ordinal_position,
			c.column_ordering
		FROM information_schema.indexes AS i
		JOIN information_schema.index_columns AS c
			ON c.table_schema = i.table_schema
			AND c.table_name = i.table_name
			AND c.index_name = i.index_name
		WHERE i.table_schema = $1
		ORDER BY i.table_name, i.index_name, c.ordinal_position`

// spannerPrimaryKeyIndexName is the name Spanner gives the index that IS the
// table's primary key. It is a property of the table rather than an object
// anyone created, and it is reported as such: IsPrimary, keeping the same shape
// the PostgreSQL reader gives an index whose pg_index.indisprimary is true.
const spannerPrimaryKeyIndexType = "PRIMARY_KEY"

// readInformationSchemaIndexes reads one schema's indexes from the SQL-standard
// catalog. See [informationSchemaIndexQuery].
func (r *Reader) readInformationSchemaIndexes(schemaName string) ([]types.DBIndex, error) {
	rows, err := r.db.Query(informationSchemaIndexQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	var (
		indexes []types.DBIndex
		byName  = map[string]int{}
	)
	for rows.Next() {
		var (
			tableName, indexName, indexType, columnName string
			// is_unique is the SQL-standard catalog's YES/NO rather than a
			// boolean, and a driver refuses to scan "YES" into a bool.
			isUnique string
			ordinal  sql.NullInt64
			ordering sql.NullString
		)
		if err := rows.Scan(
			&tableName, &indexName, &indexType, &isUnique,
			&columnName, &ordinal, &ordering,
		); err != nil {
			return nil, fmt.Errorf("failed to scan index row: %w", err)
		}

		key := tableName + "\x00" + indexName
		position, seen := byName[key]
		if !seen {
			position = len(indexes)
			byName[key] = position
			indexes = append(indexes, types.DBIndex{
				Name:      indexName,
				TableName: tableName,
				// See the same field on the constraint read: the default schema
				// is the empty string here, or the table lookups below it miss.
				Schema:    r.outputSchema(schemaName),
				IsUnique:  strings.EqualFold(strings.TrimSpace(isUnique), "YES"),
				IsPrimary: strings.EqualFold(indexType, spannerPrimaryKeyIndexType),
			})
		}
		addInformationSchemaIndexColumn(&indexes[position], columnName, ordinal, ordering)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate index rows: %w", err)
	}

	for position := range indexes {
		indexes[position].Definition = informationSchemaIndexDefinition(indexes[position])
	}
	return indexes, nil
}

// addInformationSchemaIndexColumn records one column of an index as a key part
// or as a payload column, from whether the catalog gave it a position.
func addInformationSchemaIndexColumn(
	index *types.DBIndex,
	columnName string,
	ordinal sql.NullInt64,
	ordering sql.NullString,
) {
	if !ordinal.Valid {
		index.IncludeColumns = append(index.IncludeColumns, columnName)
		return
	}
	index.Columns = append(index.Columns, columnName)
	index.Parts = append(index.Parts, types.DBIndexPart{
		Name: columnName,
		Desc: strings.EqualFold(strings.TrimSpace(ordering.String), "DESC"),
	})
}

// informationSchemaIndexDefinition writes the DDL the catalog described, the
// way the MySQL reader does for the same reason: this catalog reports an
// index's parts and never its text, and every surface below the model expects a
// definition to read.
func informationSchemaIndexDefinition(index types.DBIndex) string {
	parts := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		spelled := part.Name
		if part.Desc {
			spelled += " DESC"
		}
		parts = append(parts, spelled)
	}
	unique := ""
	if index.IsUnique {
		unique = "UNIQUE "
	}
	definition := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique, index.Name, index.TableName, strings.Join(parts, ", "))
	if len(index.IncludeColumns) > 0 {
		definition += " INCLUDE (" + strings.Join(index.IncludeColumns, ", ") + ")"
	}
	return definition
}
