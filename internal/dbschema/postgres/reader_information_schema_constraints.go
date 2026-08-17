package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// informationSchemaConstraintQuery reads constraints from the SQL-standard
// catalog instead of pg_constraint.
//
// It exists for the same server the standard index read exists for: one that
// speaks the PostgreSQL WIRE without implementing pg_catalog. Measured against
// a live Spanner endpoint through PGAdapter, the pg_constraint read is answered
//
//	ERROR: Aggregate functions with FILTER clauses are not supported
//
// so it cannot be repaired by dropping a join -- the aggregation is how that
// query pairs a constraint's local and foreign columns at all
// (stokaro/ptah#942).
//
// The key columns come from key_column_usage, the referenced side from
// referential_constraints joined to constraint_column_usage, and the clause
// from check_constraints. A constraint with no key columns -- every CHECK --
// still has to arrive, so the join to key_column_usage is a LEFT one.
const informationSchemaConstraintQuery = `
		SELECT
			tc.constraint_name,
			tc.table_name,
			tc.constraint_type,
			kcu.column_name,
			kcu.ordinal_position,
			ccu.table_name AS referenced_table,
			ccu.column_name AS referenced_column,
			rc.delete_rule,
			rc.update_rule,
			cc.check_clause
		FROM information_schema.table_constraints AS tc
		LEFT JOIN information_schema.key_column_usage AS kcu
			ON kcu.constraint_schema = tc.constraint_schema
			AND kcu.constraint_name = tc.constraint_name
			AND kcu.table_name = tc.table_name
		LEFT JOIN information_schema.referential_constraints AS rc
			ON rc.constraint_schema = tc.constraint_schema
			AND rc.constraint_name = tc.constraint_name
		LEFT JOIN information_schema.constraint_column_usage AS ccu
			ON ccu.constraint_schema = tc.constraint_schema
			AND ccu.constraint_name = tc.constraint_name
		LEFT JOIN information_schema.check_constraints AS cc
			ON cc.constraint_schema = tc.constraint_schema
			AND cc.constraint_name = tc.constraint_name
		WHERE tc.table_schema = $1
		ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position`

// notNullCheckPrefix is the name Spanner gives the CHECK constraint it
// materializes for every NOT NULL column.
const notNullCheckPrefix = "CK_IS_NOT_NULL_"

// readInformationSchemaConstraints reads one schema's constraints from the
// SQL-standard catalog. See [informationSchemaConstraintQuery].
func (r *Reader) readInformationSchemaConstraints(schemaName string) ([]types.DBConstraint, error) {
	rows, err := r.db.Query(informationSchemaConstraintQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query constraints: %w", err)
	}
	defer rows.Close()

	var (
		constraints []types.DBConstraint
		byName      = map[string]int{}
	)
	for rows.Next() {
		var (
			name, tableName, constraintType     string
			column, refTable, refColumn         sql.NullString
			deleteRule, updateRule, checkClause sql.NullString
			ordinal                             sql.NullInt64
		)
		if err := rows.Scan(
			&name, &tableName, &constraintType,
			&column, &ordinal, &refTable, &refColumn,
			&deleteRule, &updateRule, &checkClause,
		); err != nil {
			return nil, fmt.Errorf("failed to scan constraint row: %w", err)
		}
		if isMaterializedNotNullCheck(name, checkClause) {
			continue
		}

		key := tableName + "\x00" + name
		position, seen := byName[key]
		if !seen {
			position = len(constraints)
			byName[key] = position
			constraints = append(constraints, types.DBConstraint{
				Name:      name,
				TableName: tableName,
				// r.outputSchema is what every other read in this file uses: the
				// default schema is spelled as the empty string, and a qualified
				// name only where the read is scoped to another schema. Spelling
				// it "public" here made every constraint key "public.ch" while
				// the tables keyed "ch", so enhanceTablesWithConstraints matched
				// nothing and every primary key was silently dropped from the
				// rendered schema.
				Schema:        r.outputSchema(schemaName),
				Type:          constraintType,
				ForeignTable:  nullableString(refTable),
				ForeignColumn: nullableString(refColumn),
				DeleteRule:    declaredReferentialRule(deleteRule),
				UpdateRule:    declaredReferentialRule(updateRule),
				CheckClause:   nullableString(checkClause),
			})
		}
		addInformationSchemaConstraintColumn(&constraints[position], column, refColumn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate constraint rows: %w", err)
	}
	return constraints, nil
}

// isMaterializedNotNullCheck reports the CHECK constraint a server writes for
// every NOT NULL column rather than one the schema declared.
//
// Spanner materializes `NOT NULL` as `CK_IS_NOT_NULL_<table>_<column>` with the
// clause `<column> IS NOT NULL`. Reporting those would give every NOT NULL
// column a check constraint no schema ever wrote, and the comparator would then
// plan to create one on every other dialect.
//
// Both halves are required. The name alone is a convention a schema is free to
// collide with, and the clause alone is a thing somebody may legitimately have
// written -- though when they have, it says exactly what the column's own
// nullability already says, which the column read reports separately.
func isMaterializedNotNullCheck(name string, clause sql.NullString) bool {
	if !strings.HasPrefix(name, notNullCheckPrefix) || !clause.Valid {
		return false
	}
	return strings.HasSuffix(strings.ToUpper(strings.TrimSpace(clause.String)), " IS NOT NULL")
}

// addInformationSchemaConstraintColumn records one key column, and its
// referenced counterpart when the constraint has one.
func addInformationSchemaConstraintColumn(
	constraint *types.DBConstraint,
	column sql.NullString,
	refColumn sql.NullString,
) {
	if !column.Valid {
		return
	}
	if constraint.ColumnName == "" {
		constraint.ColumnName = column.String
	}
	constraint.ColumnNames = append(constraint.ColumnNames, column.String)
	if refColumn.Valid {
		constraint.ForeignColumns = append(constraint.ForeignColumns, refColumn.String)
	}
}

// nullableString is the pointer form the constraint model uses for a value the
// catalog may not have.
func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	held := value.String
	return &held
}

// declaredReferentialRule drops NO ACTION, which is the absence of a rule
// rather than a rule.
//
// The SQL-standard catalog prints the default rather than leaving it null, so a
// foreign key with no ON DELETE or ON UPDATE clause arrives carrying both. Read
// literally, that turns every plain foreign key into one the schema must
// re-declare -- and on Spanner it does not even render: the read failed with
// `spanner does not support ON UPDATE NO ACTION`, which is Ptah refusing to
// emit DDL the server would reject, on a rule the schema never wrote
// (stokaro/ptah#942).
func declaredReferentialRule(rule sql.NullString) *string {
	if !rule.Valid || strings.EqualFold(strings.TrimSpace(rule.String), "NO ACTION") {
		return nil
	}
	return nullableString(rule)
}
