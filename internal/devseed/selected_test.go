package devseed_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/devseed"
)

// filteredRead stands in for a provider's inspection of a database that holds
// held, filtered by schema selectors: a selector selects the schema of its
// name, a selector that names no schema selects nothing, and a read whose
// selectors name schema uninspectable fails, as the read of a schema whose
// objects the provider cannot inspect does. It records the selectors of each
// read in calls.
type filteredRead struct {
	held          *catalog.Database
	uninspectable string
	calls         [][]string
}

func (f *filteredRead) read(selectors []string) (*catalog.Database, error) {
	f.calls = append(f.calls, selectors)
	if slices.Contains(selectors, f.uninspectable) {
		return nil, fmt.Errorf("the provider cannot inspect the objects of schema %s", f.uninspectable)
	}
	read := &catalog.Database{}
	for _, schema := range f.held.Schemas {
		if slices.Contains(selectors, schema.Name) {
			read.Schemas = append(read.Schemas, schema)
		}
	}
	for _, table := range f.held.Tables {
		if slices.Contains(selectors, table.Schema) {
			read.Tables = append(read.Tables, table)
		}
	}
	for _, constraint := range f.held.Constraints {
		if slices.Contains(selectors, constraint.Schema) {
			read.Constraints = append(read.Constraints, constraint)
		}
	}
	return read, nil
}

// ordersDatabase holds app.orders, whose foreign key references
// identity.users, whose own foreign key references auth.accounts, and an
// archive schema the statements do not name.
func ordersDatabase() *catalog.Database {
	return &catalog.Database{
		Schemas: schemas("app", "identity", "auth", "archive"),
		Tables: []catalog.Table{
			table("app", "orders", column("id", "integer"), column("user_id", "integer")),
			table("identity", "users", column("id", "integer"), column("account_id", "integer")),
			table("auth", "accounts", column("id", "integer")),
			table("archive", "orders_2019", column("id", "integer")),
		},
		Constraints: []catalog.Constraint{
			foreignKey("app", "orders", "orders_user_fk", "user_id", "identity", "users"),
			foreignKey("identity", "users", "users_account_fk", "account_id", "auth", "accounts"),
		},
	}
}

// ordersSeed is the compared state of a selection of app.orders.
func ordersSeed() *catalog.Database {
	held := ordersDatabase()
	return &catalog.Database{Schemas: schemas("app"), Tables: held.Tables[:1], Constraints: held.Constraints[:1]}
}

var appOptions = devseed.Options{Dialect: "postgres", DefaultSchema: "app", SearchPath: []string{"app"}}

func TestSelected_HappyPath(t *testing.T) {
	t.Run("a dependency in another schema is read by its schema, and so is what it references in turn", func(t *testing.T) {
		c := qt.New(t)
		provider := &filteredRead{held: ordersDatabase(), uninspectable: "archive"}
		got, err := devseed.Closure(ordersSeed(), nil, renderRefusingDanglingKeys, devseed.Selected(provider.read, []string{"app"}), appOptions)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "table", Schema: "identity", Name: "users"},
			{Kind: "schema", Name: "identity"},
			{Kind: "table", Schema: "auth", Name: "accounts"},
			{Kind: "schema", Name: "auth"},
		})
		c.Assert(provider.calls, qt.DeepEquals, [][]string{{"app", "identity"}, {"app", "auth", "identity"}})
	})

	t.Run("a word that names no schema selects nothing", func(t *testing.T) {
		c := qt.New(t)
		provider := &filteredRead{held: ordersDatabase(), uninspectable: "archive"}
		executed := []string{"CREATE VIEW app.recent AS SELECT o.id FROM app.orders o"}
		got, err := devseed.Closure(ordersSeed(), executed, renderRefusingDanglingKeys, devseed.Selected(provider.read, []string{"app"}), appOptions)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.HasLen, 4)
		c.Assert(provider.calls, qt.DeepEquals, [][]string{{"app", "identity", "o"}, {"app", "auth", "identity", "o"}})
	})

	t.Run("the scope is read with every set of names", func(t *testing.T) {
		c := qt.New(t)
		provider := &filteredRead{held: ordersDatabase()}
		got, err := devseed.Selected(provider.read, []string{"app", ""})([]string{"identity", "app"})
		c.Assert(err, qt.IsNil)
		c.Assert(got.Tables, qt.HasLen, 2)
		c.Assert(provider.calls, qt.DeepEquals, [][]string{{"app", "identity"}})
	})

	t.Run("no name and no scope reads nothing", func(t *testing.T) {
		c := qt.New(t)
		provider := &filteredRead{held: ordersDatabase()}
		got, err := devseed.Selected(provider.read, []string{""})([]string{""})
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, &catalog.Database{})
		c.Assert(provider.calls, qt.HasLen, 0)
	})

	t.Run("a quoted schema name is sent folded to lower case", func(t *testing.T) {
		c := qt.New(t)
		provider := &filteredRead{held: ordersDatabase()}
		executed := []string{`CREATE VIEW app.old AS SELECT id FROM "Archive".orders_2019`}
		_, err := devseed.Closure(ordersSeed(), executed, renderRefusingDanglingKeys, devseed.Selected(provider.read, []string{"app"}), appOptions)
		c.Assert(err, qt.IsNil)
		c.Assert(provider.calls[0], qt.DeepEquals, []string{"app", "archive", "identity"})
	})
}

func TestSelected_FailurePath(t *testing.T) {
	t.Run("a statement that names a schema the provider cannot inspect fails with the provider's error", func(t *testing.T) {
		c := qt.New(t)
		provider := &filteredRead{held: ordersDatabase(), uninspectable: "archive"}
		executed := []string{"INSERT INTO archive.orders_2019 SELECT id FROM app.orders"}
		got, err := devseed.Closure(ordersSeed(), executed, renderRefusingDanglingKeys, devseed.Selected(provider.read, []string{"app"}), appOptions)
		c.Assert(err, qt.ErrorMatches, `read the target outside the compared scope: the provider cannot inspect the objects of schema archive`)
		c.Assert(got, qt.DeepEquals, devseed.Result{})
	})
}
