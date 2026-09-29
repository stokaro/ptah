package mysql

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/mysqlindex"
)

// readIndexBlockSizes supplements STATISTICS with the hint only SHOW CREATE
// TABLE reports. QueryRow completes before the next query, so a reader using
// one transaction does not hold an open result while asking for another.
func (r *Reader) readIndexBlockSizes(ctx context.Context, dbName string, schema *catalog.Database) error {
	if len(schema.Indexes) == 0 {
		return nil
	}
	var version string
	if err := r.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return err
	}
	dialect := platform.MySQL
	if strings.Contains(strings.ToLower(version), "mariadb") {
		dialect = platform.MariaDB
	}
	positions := make(map[indexKey]int, len(schema.Indexes))
	for i, index := range schema.Indexes {
		positions[indexKey{table: index.TableName, index: index.Name}] = i
	}
	for _, table := range schema.Tables {
		if !mysqlindex.KeepsBlockSize(dialect, table.RowFormat) {
			continue
		}
		var name, ddl string
		query := "SHOW CREATE TABLE `" + strings.ReplaceAll(dbName, "`", "``") + "`.`" + strings.ReplaceAll(table.Name, "`", "``") + "`"
		if err := r.db.QueryRowContext(ctx, query).Scan(&name, &ddl); err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
		sizes, err := mysqlindex.BlockSizes(ddl)
		if err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
		for name, size := range sizes {
			if i, ok := positions[indexKey{table: table.Name, index: name}]; ok {
				schema.Indexes[i].KeyBlockSize = size
			}
		}
	}
	return nil
}

// carryPrimaryKeyOptions keeps the primary index's metadata on the constraint
// that owns it. Primary indexes are excluded from ordinary index comparison.
func carryPrimaryKeyOptions(schema *catalog.Database) {
	indexes := make(map[string]catalog.Index)
	for _, index := range schema.Indexes {
		if index.IsPrimary {
			indexes[index.TableName] = index
		}
	}
	for i := range schema.Constraints {
		constraint := &schema.Constraints[i]
		if index, ok := indexes[constraint.TableName]; ok && constraint.Type == "PRIMARY KEY" {
			constraint.Comment, constraint.KeyBlockSize = index.Comment, index.KeyBlockSize
		}
	}
}
