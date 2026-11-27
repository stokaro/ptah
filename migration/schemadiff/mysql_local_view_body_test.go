package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A read of one MySQL or MariaDB database takes the database's qualifier off
// a stored view body. A declaration that names that database, as `schema
// inspect` of a whole server writes it, is the same query and plans nothing
// (stokaro/ptah#6710).
func TestMySQLLocalViewBody_MatchesTheViewsDatabase(t *testing.T) {
	for _, test := range []struct{ name, dialect, declared, held string }{
		{
			name: "inspected whole server", dialect: "mysql",
			declared: "select `app`.`t`.`id` AS `id` from `app`.`t`",
			held:     "select `t`.`id` AS `id` from `t`",
		},
		{
			name: "written by hand", dialect: "mysql",
			declared: "SELECT id FROM app.t",
			held:     "select `t`.`id` AS `id` from `t`",
		},
		{
			name: "function call", dialect: "mysql",
			declared: "select `app`.`f_double`(`app`.`t`.`n`) AS `d` from `app`.`t`",
			held:     "select `f_double`(`t`.`n`) AS `d` from `t`",
		},
		{
			name: "alias spelled as the database", dialect: "mysql",
			declared: "select `app`.`id` AS `id` from `app`.`t` `app`",
			held:     "select `app`.`id` AS `id` from `t` `app`",
		},
		{
			name: "mariadb", dialect: "mariadb",
			declared: "select `app`.`t`.`id` AS `id` from `app`.`t`",
			held:     "select `t`.`id` AS `id` from `t`",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, held := mysqlLocalViewBodies(test.declared, test.held)

			diff, err := schemadiff.CompareWithOptions(t.Context(), desired, held, mysqlLocalOptions(test.dialect), must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, 0)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// A declaration that names another database, or reads another relation, is
// another query, and the comparison still reports it.
func TestMySQLLocalViewBody_KeepsDifferentQueries(t *testing.T) {
	for _, test := range []struct{ name, dialect, declared, held string }{
		{
			name: "another database", dialect: "mysql",
			declared: "select `other`.`t`.`id` AS `id` from `other`.`t`",
			held:     "select `t`.`id` AS `id` from `t`",
		},
		{
			name: "another relation", dialect: "mysql",
			declared: "select `app`.`u`.`id` AS `id` from `app`.`u`",
			held:     "select `t`.`id` AS `id` from `t`",
		},
		{
			name: "mariadb another database", dialect: "mariadb",
			declared: "select `other`.`t`.`id` AS `id` from `other`.`t`",
			held:     "select `t`.`id` AS `id` from `t`",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, held := mysqlLocalViewBodies(test.declared, test.held)

			diff, err := schemadiff.CompareWithOptions(t.Context(), desired, held, mysqlLocalOptions(test.dialect), must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, 1)
		})
	}
}

// mysqlLocalViewBodies is a declared view v with the declared body, and the
// view v a read of database app reports with the held body.
func mysqlLocalViewBodies(declared, held string) (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{
		Views: []schemamodel.View{{Name: "v", Body: declared}},
	}, &catalog.Database{
		Views:           []catalog.View{{Name: "v", Body: held}},
		CurrentDatabase: "app",
	}
}

// mysqlLocalOptions compares for dialect on a connection that selected
// database app.
func mysqlLocalOptions(dialect string) *config.CompareOptions {
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = "app"
	return &config.CompareOptions{Dialect: dialect, IdentifierSemantics: &semantics}
}

// A read of a whole MySQL or MariaDB server keeps each view's database on its
// stored body, as the server stores it. A declaration that `schema inspect` of
// the same server wrote, or one written without the view's own database, is
// the same query and plans nothing, whatever other database the body reads.
func TestMySQLWholeServerViewBody_MatchesTheStoredBody(t *testing.T) {
	for _, test := range []struct{ name, dialect, declared, held string }{
		{
			name: "inspected two databases", dialect: "mysql",
			declared: "select `app2`.`t`.`id` AS `id`,`a`.`name` AS `name` from (`app2`.`t` join `crm2`.`accounts` `a` on((`a`.`id` = `app2`.`t`.`id`)))",
			held:     "select `app2`.`t`.`id` AS `id`,`a`.`name` AS `name` from (`app2`.`t` join `crm2`.`accounts` `a` on((`a`.`id` = `app2`.`t`.`id`)))",
		},
		{
			name: "written without its own database", dialect: "mysql",
			declared: "select `t`.`id` AS `id`,`a`.`name` AS `name` from (`t` join `crm2`.`accounts` `a` on((`a`.`id` = `t`.`id`)))",
			held:     "select `app2`.`t`.`id` AS `id`,`a`.`name` AS `name` from (`app2`.`t` join `crm2`.`accounts` `a` on((`a`.`id` = `app2`.`t`.`id`)))",
		},
		{
			name: "mariadb inspected two databases", dialect: "mariadb",
			declared: "select `app2`.`t`.`id` AS `id`,`a`.`name` AS `name` from (`app2`.`t` join `crm2`.`accounts` `a` on(`a`.`id` = `app2`.`t`.`id`))",
			held:     "select `app2`.`t`.`id` AS `id`,`a`.`name` AS `name` from (`app2`.`t` join `crm2`.`accounts` `a` on(`a`.`id` = `app2`.`t`.`id`))",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, held := mysqlWholeServerViewBodies(test.declared, test.held)

			diff, err := schemadiff.CompareWithOptions(t.Context(), desired, held, mysqlWholeServerOptions(test.dialect), must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, 0)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// On a whole server, a declaration that reads another relation of the other
// database is another query, and the comparison still reports it.
func TestMySQLWholeServerViewBody_KeepsDifferentQueries(t *testing.T) {
	c := qt.New(t)
	desired, held := mysqlWholeServerViewBodies(
		"select `app2`.`t`.`id` AS `id` from (`app2`.`t` join `crm2`.`contacts` `a` on((`a`.`id` = `app2`.`t`.`id`)))",
		"select `app2`.`t`.`id` AS `id` from (`app2`.`t` join `crm2`.`accounts` `a` on((`a`.`id` = `app2`.`t`.`id`)))",
	)

	diff, err := schemadiff.CompareWithOptions(t.Context(), desired, held, mysqlWholeServerOptions("mysql"), must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ViewsModified, qt.HasLen, 1)
}

// mysqlWholeServerViewBodies is a declared view app2.v with the declared body,
// and the view v of database app2 a read of the whole server reports with the
// held body.
func mysqlWholeServerViewBodies(declared, held string) (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{
		Views: []schemamodel.View{{Name: "app2.v", Body: declared}},
	}, &catalog.Database{
		Views: []catalog.View{{Schema: "app2", Name: "v", Body: held}},
	}
}

// mysqlWholeServerOptions compares for dialect on a connection that selected
// no database.
func mysqlWholeServerOptions(dialect string) *config.CompareOptions {
	semantics := identifier.ForDialect(dialect)
	return &config.CompareOptions{Dialect: dialect, IdentifierSemantics: &semantics}
}

// A server started with lower_case_table_names 1 or 2 reads `APP4.t` as the
// table t of app4, and stores the view body with app4 as DATABASE() spells it.
// There a declaration that names its own database in another case is the same
// query and plans nothing (stokaro/ptah#6756). A server that keeps names apart
// by case reads APP4 as another database, and so does a comparison whose
// server folding no connection read; neither refuses the body for it.
func TestCompareWithDatabaseInfo_ViewBodyNamesItsDatabaseInAnotherCase(t *testing.T) {
	for _, test := range []struct {
		name         string
		dialect      string
		folding      catalog.NameFolding
		wantModified int
	}{
		{name: "mysql lower_case_table_names 1 or 2", dialect: "mysql", folding: catalog.NameFoldingFolds, wantModified: 0},
		{name: "mariadb lower_case_table_names 1 or 2", dialect: "mariadb", folding: catalog.NameFoldingFolds, wantModified: 0},
		{name: "mysql lower_case_table_names 0", dialect: "mysql", folding: catalog.NameFoldingExact, wantModified: 1},
		{name: "mysql whose folding is unknown", dialect: "mysql", folding: catalog.NameFoldingUnknown, wantModified: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect(test.dialect)
			semantics.DefaultSchema = "app4"
			info := catalog.ServerInfo{Dialect: test.dialect, Schema: "app4", NameFolding: test.folding, IdentifierSemantics: semantics}
			desired := &schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: "SELECT id FROM APP4.t"}}}
			held := &catalog.Database{Views: []catalog.View{{Name: "v", Body: "select `t`.`id` AS `id` from `t`"}}, CurrentDatabase: "app4"}

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, held, info, nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, test.wantModified)
		})
	}
}
