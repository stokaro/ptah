package mssqlconnect

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/connector"
)

// ResolveIdentifierSemantics evaluates which of names the catalog treats as one
// identifier under CATALOG_DEFAULT. The connection's default schema joins the
// set whether the caller listed it or not.
func (Service) ResolveIdentifierSemantics(
	ctx context.Context,
	executor connector.Executor,
	info catalog.ServerInfo,
	names []string,
) (identifier.Semantics, error) {
	semantics := info.IdentifierSemantics
	if semantics.CatalogCollation == "" {
		return identifier.Semantics{}, fmt.Errorf(
			"resolve SQL Server identifier semantics: catalog collation is unavailable",
		)
	}

	names = append(names, semantics.DefaultSchema)
	names = connector.NormalizedNames(names)
	if err := validateSQLServerIdentifierNames(names); err != nil {
		return identifier.Semantics{}, err
	}
	payload, err := json.Marshal(names)
	if err != nil {
		return identifier.Semantics{}, fmt.Errorf(
			"resolve SQL Server identifier semantics: encode identifier set: %w",
			err,
		)
	}
	const query = `
WITH identifiers AS (
    SELECT
        CONVERT(int, [key]) AS position,
        CONVERT(nvarchar(4000), [value]) AS value
    FROM OPENJSON(@identifiers)
)
SELECT
    position,
    MIN(position) OVER (
        PARTITION BY value COLLATE CATALOG_DEFAULT
    ) AS equivalence_position
FROM identifiers
ORDER BY position`
	rows, err := executor.QueryContext(
		ctx,
		query,
		sql.Named("identifiers", string(payload)),
	)
	if err != nil {
		return identifier.Semantics{}, fmt.Errorf(
			"resolve SQL Server identifier semantics under CATALOG_DEFAULT: %w",
			err,
		)
	}
	defer rows.Close()

	resolved := make([]identifier.ResolvedName, 0, len(names))
	for expectedPosition := 0; rows.Next(); expectedPosition++ {
		var position, equivalencePosition int
		if err := rows.Scan(&position, &equivalencePosition); err != nil {
			return identifier.Semantics{}, fmt.Errorf(
				"resolve SQL Server identifier semantics: scan equivalence row: %w",
				err,
			)
		}
		if position != expectedPosition ||
			position >= len(names) ||
			equivalencePosition < 0 ||
			equivalencePosition >= len(names) {
			return identifier.Semantics{}, fmt.Errorf(
				"resolve SQL Server identifier semantics: invalid equivalence row position=%d key=%d",
				position,
				equivalencePosition,
			)
		}
		resolved = append(resolved, identifier.ResolvedName{
			Name: names[position],
			Key:  names[equivalencePosition],
		})
	}
	if err := rows.Err(); err != nil {
		return identifier.Semantics{}, fmt.Errorf(
			"resolve SQL Server identifier semantics: iterate equivalence rows: %w",
			err,
		)
	}
	if len(resolved) != len(names) {
		return identifier.Semantics{}, fmt.Errorf(
			"resolve SQL Server identifier semantics: expected %d equivalence rows, got %d",
			len(names),
			len(resolved),
		)
	}
	return semantics.WithResolvedNames(resolved), nil
}

// validateSQLServerIdentifierNames refuses a name SQL Server cannot hold.
//
// The limit and the unit come from [capability.Identifiers] rather than from a
// constant here. Carrying its own 128 and its own utf8.RuneCountInString would
// be a second copy of a rule the renderer also has: two places deciding whether
// a multibyte name fits, agreeing only until somebody changes one of them.
func validateSQLServerIdentifierNames(names []string) error {
	limit := capability.Identifiers(platform.SQLServer)
	for _, name := range names {
		if limit.Exceeds(name) {
			return fmt.Errorf(
				"resolve SQL Server identifier semantics: identifier %q exceeds %s",
				name,
				limit,
			)
		}
	}
	return nil
}
