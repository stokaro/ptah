package mysql

import (
	"ptah.run/catalog"
	"ptah.run/internal/sqlqualifier"
)

// localColumnDefaults takes the name of the database a read of one database
// selected, current, off the objects each column default names, and returns
// tables. It leaves the defaults of a whole-server read, where current is
// empty, as the server reports them.
//
// MariaDB stores a default that reads a sequence with the sequence's database,
// however the statement spelled it: `DEFAULT NEXTVAL(s)` in `app` reads back
// as nextval(`app`.`s`), and lastval and setval the same way, measured on
// MariaDB 12.3. A column reference in a default is stored without a table, so
// a sequence is the only name a default qualifies. Without this call a
// derived rollback planned from a read of a shadow database creates the table
// with nextval(`<shadow>`.`s`): on the target it fails with error 1146, or,
// where the target's server holds a database of the shadow's name, the table
// draws from that database's sequence. A sequence of another database keeps
// its name.
func localColumnDefaults(tables []catalog.Table, dialect, current string) []catalog.Table {
	if current == "" {
		return tables
	}
	for i := range tables {
		for j := range tables[i].Columns {
			column := &tables[i].Columns[j]
			if column.ColumnDefault == nil {
				continue
			}
			local := sqlqualifier.Expression(*column.ColumnDefault, dialect, current)
			column.ColumnDefault = &local
		}
	}
	return tables
}
