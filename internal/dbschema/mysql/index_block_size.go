package mysql

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/mysqlindex"
)

// readIndexBlockSizes supplements STATISTICS with the hint only SHOW CREATE
// TABLE reports. It describes every index's hint, which the schema's coverage
// claims, and gives an index the MySQL owner's observation where it says more
// than its absence: a hint, or a MySQL table that keeps one (see
// [mysqlschema.ObservationNeeded]). The primary index's hint is returned by
// table name for its constraint. QueryRow completes before the next query, so
// a reader using one transaction does not hold an open result while asking
// for another.
func (r *Reader) readIndexBlockSizes(ctx context.Context, dbName string, schema *catalog.Database) (map[string]uint64, error) {
	claim, err := mysqlschema.IndexBlockSizeCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return nil, err
	}
	if schema.FeatureCoverage, err = schema.FeatureCoverage.Combine(claim); err != nil {
		return nil, err
	}
	if len(schema.Indexes) == 0 {
		return make(map[string]uint64), nil
	}
	var version string
	if err := r.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return nil, err
	}
	dialect := platform.MySQL
	if strings.Contains(strings.ToLower(version), "mariadb") {
		dialect = platform.MariaDB
	}
	retained := make(map[string]bool, len(schema.Tables))
	sizes := make(map[indexKey]uint64)
	primary := make(map[string]uint64)
	for _, table := range schema.Tables {
		if !mysqlindex.KeepsBlockSize(dialect, table.RowFormat) {
			continue
		}
		retained[table.Name] = true
		var name, ddl string
		query := "SHOW CREATE TABLE `" + strings.ReplaceAll(dbName, "`", "``") + "`.`" + strings.ReplaceAll(table.Name, "`", "``") + "`"
		if err := r.db.QueryRowContext(ctx, query).Scan(&name, &ddl); err != nil {
			return nil, fmt.Errorf("table %q: %w", table.Name, err)
		}
		read, err := mysqlindex.BlockSizes(ddl)
		if err != nil {
			return nil, fmt.Errorf("table %q: %w", table.Name, err)
		}
		for index, size := range read {
			sizes[indexKey{table: table.Name, index: index}] = size
		}
		primary[table.Name] = read["PRIMARY"]
	}
	for i := range schema.Indexes {
		index := &schema.Indexes[i]
		observed := mysqlschema.ObservedIndexBlockSize{
			KeyBlockSize: sizes[indexKey{table: index.TableName, index: index.Name}], Retained: retained[index.TableName],
		}
		if index.IsPrimary || !mysqlschema.ObservationNeeded(dialect, observed) {
			continue
		}
		facets, err := mysqlschema.WithObservedIndexBlockSize(index.Facets, observed)
		if err != nil {
			return nil, fmt.Errorf("index %q on table %q: %w", index.Name, index.TableName, err)
		}
		index.Facets = facets
	}
	return primary, nil
}

// carryPrimaryKeyOptions keeps the primary index's comment and block-size
// hint on the constraint that owns it. Primary indexes are excluded from
// ordinary index comparison.
func carryPrimaryKeyOptions(schema *catalog.Database, blockSizes map[string]uint64) {
	comments := make(map[string]string)
	for _, index := range schema.Indexes {
		if index.IsPrimary {
			comments[index.TableName] = index.Comment
		}
	}
	for i := range schema.Constraints {
		constraint := &schema.Constraints[i]
		if comment, ok := comments[constraint.TableName]; ok && constraint.Type == "PRIMARY KEY" {
			constraint.Comment, constraint.KeyBlockSize = comment, blockSizes[constraint.TableName]
		}
	}
}
