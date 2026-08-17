package postgres

import (
	"database/sql"
	"fmt"

	"go.5x5.cz/ptah/dbschema/types"
)

// informationSchemaViewQuery reads views from the SQL-standard catalog.
//
// The pg_catalog read is a pg_class/pg_namespace/pg_get_viewdef query, and a
// server without those answers it `syntax error at or near "AS"` -- the shape
// of a query built out of relations it does not have. Unlike functions,
// materialized views and triggers, a view is not something a preset rules out:
// Spanner has views, so this read has to work rather than be skipped
// (stokaro/ptah#942).
//
// check_option is read where the catalog carries it. The SQL standard defines
// the column, and a server that leaves it NULL is reported as NONE, which is
// what a view with no WITH CHECK OPTION is.
const informationSchemaViewQuery = `
		SELECT table_name, view_definition, check_option
		FROM information_schema.views
		WHERE table_schema = $1
		ORDER BY table_name`

// readInformationSchemaViews reads one schema's views from the SQL-standard
// catalog. See [informationSchemaViewQuery].
func (r *Reader) readInformationSchemaViews(schemaName string) ([]types.DBView, error) {
	rows, err := r.db.Query(informationSchemaViewQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query views: %w", err)
	}
	defer rows.Close()

	var views []types.DBView
	for rows.Next() {
		var (
			name        string
			body        sql.NullString
			checkOption sql.NullString
		)
		if err := rows.Scan(&name, &body, &checkOption); err != nil {
			return nil, fmt.Errorf("failed to scan view row: %w", err)
		}
		views = append(views, types.DBView{
			Name:        name,
			Schema:      r.outputSchema(schemaName),
			Body:        body.String,
			CheckOption: informationSchemaCheckOption(checkOption),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate view rows: %w", err)
	}
	return views, nil
}

// informationSchemaCheckOption normalizes the catalog's check option, treating
// an absent one as the NONE a view without the clause has.
func informationSchemaCheckOption(value sql.NullString) string {
	if !value.Valid || value.String == "" {
		return "NONE"
	}
	return value.String
}
