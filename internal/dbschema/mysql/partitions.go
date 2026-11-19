package mysql

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/internal/mysqlpartition"
	"ptah.run/internal/readscope"
)

// unmodeledPartitionKind is the kind a report names a table's partitioning by.
const unmodeledPartitionKind = "table partitioning"

// readPartitionedTables names the tables of database that the server holds
// partitioned, for a report. Ptah does not model MySQL or MariaDB
// partitioning: the table is read and compared without its partitions, so a
// comparison reports a partitioned table synced with a declaration that has
// none, and SQL written from the read creates the table without them. The read
// does not refuse such a table, because a change Ptah plans for it, such as
// ADD COLUMN, keeps the partitions; it names it so the report does not read
// as covering them.
//
// A read of what this run created has nothing to report, since every source
// refuses partitioning, and neither has a run that left tables out.
func (r *Reader) readPartitionedTables(ctx context.Context, database string) ([]catalog.UnmodeledObject, error) {
	if readscope.IsAuthored(ctx) || !readscope.Needs(ctx, readscope.Tables) {
		return nil, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT DISTINCT TABLE_NAME FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND PARTITION_NAME IS NOT NULL ORDER BY TABLE_NAME`, database)
	if err != nil {
		return nil, fmt.Errorf("failed to read partitioned tables: %w", err)
	}
	defer rows.Close()
	var objects []catalog.UnmodeledObject
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to read partitioned tables: %w", err)
		}
		objects = append(objects, catalog.UnmodeledObject{
			Kind: unmodeledPartitionKind, Name: name, Table: name, Reason: mysqlpartition.ReadNote,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read partitioned tables: %w", err)
	}
	return objects, nil
}
