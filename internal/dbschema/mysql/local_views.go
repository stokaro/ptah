package mysql

import (
	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlqualifier"
)

// localViewBodies takes the name of the database a read of one database
// selected, current, off the relations of each view body, and returns views.
// It leaves the bodies of a whole-server read, where current is empty, as the
// server reports them.
//
// The server stores a view body with every relation qualified by the database
// that holds the view: `CREATE VIEW v AS SELECT id FROM t` in `app` reads back
// as select `app`.`t`.`id` AS `id` from `app`.`t`, measured on MySQL 26.10 and
// MariaDB 12.3; the descriptor field Schemas.StoredViewBodyNamesDatabase states
// it. A view is one of the database's own objects, so a read of one database
// names it without a schema, as it names a table, a routine and a sequence.
// Its body has to read the same way. A plan that recreates the view from a read of a shadow
// or a dev database would otherwise read that database's tables from the
// target, and a migration written from a read of one database would read that
// database wherever the file runs.
//
// A relation of another database keeps its name, since that name is part of
// what the view reads.
func localViewBodies(views []catalog.View, dialect, current string) []catalog.View {
	if current == "" {
		return views
	}
	if described, _ := platform.Lookup(dialect); !described.Schemas.StoredViewBodyNamesDatabase {
		return views
	}
	for i := range views {
		views[i].Body = sqlqualifier.ViewBody(views[i].Body, dialect, current, "")
	}
	return views
}

// dialect is the dialect of the server the account is on: MariaDB or MySQL.
func (a sessionAccount) dialect() string {
	if a.mariaDB {
		return platform.MariaDB
	}
	return platform.MySQL
}
