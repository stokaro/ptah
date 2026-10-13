package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/pgtablesource"
	"ptah.run/internal/sqlident"
)

// readPartitions captures keys only for inspected partitioned parents. Ordinary
// tables have complete absence coverage. The execution-facing parent flag stays
// independent, so an unreadable key cannot permit a concurrent parent index.
func (r *Reader) readPartitions(ctx context.Context, database *catalog.Database) error {
	if !pgtable.SupportsPartition(r.dialect) {
		return nil
	}
	identities := objectidentity.NewBuilder(identifier.ForDialect(r.dialect))
	subjects := make([]schemaext.SubjectCoverage, 0, len(database.Tables))
	for i := range database.Tables {
		table := &database.Tables[i]
		knowledge := schemaext.Knowledge{State: schemaext.Complete}
		if table.Partitioned {
			value, err := r.readPartition(ctx, *table)
			if err != nil {
				return err
			}
			if value == nil {
				knowledge = schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "PostgreSQL does not return a partition key for the partitioned parent"}
			} else {
				table.Facets, err = table.Facets.With(value)
				if err != nil {
					return err
				}
			}
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgtable.PartitionKind, Subject: identities.TableParts(table.Schema, table.Name), Knowledge: knowledge})
	}
	known, err := pgtable.PartitionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables have inspected partition keys"}, subjects)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}

func (r *Reader) readPartition(ctx context.Context, table catalog.Table) (*pgtable.ObservedPartition, error) {
	name := sqlident.Qualified(platform.Postgres, table.Schema, table.Name)
	rows, err := r.db.QueryContext(ctx, "SELECT pg_get_partkeydef($1::regclass)", name)
	if err != nil {
		return nil, fmt.Errorf("read partition key of %s: %w", name, err)
	}
	defer rows.Close()
	var definition sql.NullString
	for rows.Next() {
		if err := rows.Scan(&definition); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !definition.Valid || definition.String == "" {
		return nil, nil
	}
	value, err := pgtablesource.ParsePartitionSQL(definition.String)
	if err != nil {
		return nil, fmt.Errorf("read partition key of %s: %w", name, err)
	}
	return &pgtable.ObservedPartition{Type: value.Type, Parts: value.Parts}, nil
}
