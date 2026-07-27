package dbschema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"unicode/utf8"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/identifier"
)

// ResolveIdentifierSemantics resolves the finite identifier set against the
// connected catalog. SQL Server evaluates equivalence under CATALOG_DEFAULT;
// other dialects return their deterministic local semantics unchanged.
func (dc *DatabaseConnection) ResolveIdentifierSemantics(
	ctx context.Context,
	names []string,
) (identifier.Semantics, error) {
	if dc == nil {
		return identifier.Semantics{}, fmt.Errorf("resolve identifier semantics: database connection is nil")
	}
	info := dc.Info()
	if platform.NormalizeDialect(info.Dialect) != platform.SQLServer {
		return info.IdentifierSemantics.Normalize(info.Dialect), nil
	}
	semantics := info.IdentifierSemantics
	if semantics.CatalogCollation == "" {
		return identifier.Semantics{}, fmt.Errorf(
			"resolve SQL Server identifier semantics: catalog collation is unavailable",
		)
	}

	names = append(names, semantics.DefaultSchema)
	names = normalizedIdentifierNames(names)
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
	rows, err := dc.QueryContext(
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

func normalizedIdentifierNames(names []string) []string {
	names = slices.Clone(names)
	names = slices.DeleteFunc(names, func(name string) bool {
		return name == ""
	})
	slices.Sort(names)
	return slices.Compact(names)
}

func validateSQLServerIdentifierNames(names []string) error {
	const maxIdentifierCharacters = 128
	for _, name := range names {
		if utf8.RuneCountInString(name) > maxIdentifierCharacters {
			return fmt.Errorf(
				"resolve SQL Server identifier semantics: identifier %q exceeds %d characters",
				name,
				maxIdentifierCharacters,
			)
		}
	}
	return nil
}
