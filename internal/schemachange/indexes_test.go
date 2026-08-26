package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
)

// Indexes were the family the canonical model had no object for at all, and
// #1663 says why they are not constraints that happen to have columns: an index
// name is unique within its TABLE on some targets and within its SCHEMA on
// others, so a model that assumed either would merge two indexes on one target
// and split one on another.

func TestAnIndexIsPlanned(t *testing.T) {
	tests := []struct {
		name           string
		description    *schemamodel.Database
		currentCatalog *catalog.Database
		wantOperation  schemachange.Operation
		wantChanged    []string
	}{
		{
			name:           "declared and absent from the database",
			description:    indexedWidget(false, "a", "b"),
			currentCatalog: indexedWidgetCatalog(false),
			wantOperation:  schemachange.Add,
		},
		{
			name:           "present and no longer declared",
			description:    indexedWidget(false),
			currentCatalog: indexedWidgetCatalog(false, "a", "b"),
			wantOperation:  schemachange.Remove,
		},
		{
			name:           "declared over different columns",
			description:    indexedWidget(false, "a"),
			currentCatalog: indexedWidgetCatalog(false, "a", "b"),
			wantOperation:  schemachange.Modify,
			wantChanged:    []string{"columns"},
		},
		{
			name:           "declared unique where the database has it plain",
			description:    indexedWidget(true, "a", "b"),
			currentCatalog: indexedWidgetCatalog(false, "a", "b"),
			wantOperation:  schemachange.Modify,
			wantChanged:    []string{"uniqueness"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, test.description, test.currentCatalog)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
			c.Assert(changes[0].Operation, qt.Equals, test.wantOperation)
			c.Assert(changes[0].Changed, qt.DeepEquals, test.wantChanged)
			c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
		})
	}
}

// TestAnUnchangedIndexIsNotAChange is the control the rows above need: a
// comparison that reported a change for every index would satisfy all four and
// rebuild every index on every run.
func TestAnUnchangedIndexIsNotAChange(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c, indexedWidget(false, "a", "b"), indexedWidgetCatalog(false, "a", "b"))

	c.Assert(changes, qt.HasLen, 0)
}

// TestAKeyTheReaderCouldNotNameIsNotRebuilt pins the one property that is not
// symmetric.
//
// A MySQL functional key part -- `KEY idx ((b + 1))` -- has a NULL COLUMN_NAME
// in information_schema.STATISTICS, so the reader reports FEWER columns than
// the key has and says so. Comparing those against a declaration would plan a
// rebuild on every run for a key that never changed, and the rebuild would drop
// the part the reader could not see.
func TestAKeyTheReaderCouldNotNameIsNotRebuilt(t *testing.T) {
	c := qt.New(t)
	currentCatalog := indexedWidgetCatalog(false, "a")
	currentCatalog.Indexes[0].KeyPartsIncomplete = true

	changes := changesFor(c, indexedWidget(false, "a", "b"), currentCatalog)

	c.Assert(changes, qt.HasLen, 0)
}

// TestAnIndexInAnUndescribedSchemaIsNotDropped is the coverage gate, the same
// rule every other family in this comparison carries.
func TestAnIndexInAnUndescribedSchemaIsNotDropped(t *testing.T) {
	c := qt.New(t)
	description := indexedWidget(false)
	description.NotDescribed = coverage.Set{}.WithObject(coverage.Schema, "public")

	changes := changesFor(c, description, indexedWidgetCatalog(false, "a", "b"))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Undecidable)
	c.Assert(changes[0].Diagnostic, qt.Contains, "is not a request to drop it")
}

// indexedWidget is a table carrying an index over the given columns, or none
// when they are absent.
func indexedWidget(unique bool, columns ...string) *schemamodel.Database {
	description := describedTable(
		schemamodel.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		schemamodel.Field{StructName: "Widget", Name: "a", Type: "text", Nullable: true},
		schemamodel.Field{StructName: "Widget", Name: "b", Type: "text", Nullable: true},
	)
	return withDeclaredIndex(description, unique, columns)
}

func withDeclaredIndex(
	description *schemamodel.Database,
	unique bool,
	columns []string,
) *schemamodel.Database {
	return map[bool]func() *schemamodel.Database{
		true: func() *schemamodel.Database { return description },
		false: func() *schemamodel.Database {
			description.Indexes = append(description.Indexes, schemamodel.Index{
				StructName: "Widget", Name: "idx_widget_ab", Fields: columns, Unique: unique,
			})
			return description
		},
	}[len(columns) == 0]()
}

// indexedWidgetCatalog is that table as a currentCatalog read reports it.
func indexedWidgetCatalog(unique bool, columns ...string) *catalog.Database {
	currentCatalog := catalogTable(
		catalog.Column{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		catalog.Column{Name: "a", DataType: "text", IsNullable: "YES"},
		catalog.Column{Name: "b", DataType: "text", IsNullable: "YES"},
	)
	return withCatalogIndex(currentCatalog, unique, columns)
}

func withCatalogIndex(
	currentCatalog *catalog.Database,
	unique bool,
	columns []string,
) *catalog.Database {
	return map[bool]func() *catalog.Database{
		true: func() *catalog.Database { return currentCatalog },
		false: func() *catalog.Database {
			currentCatalog.Indexes = append(currentCatalog.Indexes, catalog.Index{
				Name: "idx_widget_ab", TableName: "widget", Schema: "public",
				Columns: columns, IsUnique: unique,
			})
			return currentCatalog
		},
	}[len(columns) == 0]()
}

// TestAConcurrentIndexNamesTheFactItNeeds is the item #1663 states as "CREATE
// INDEX CONCURRENTLY is a required target fact on the change, not a branch
// inside a dialect planner".
//
// The change names the capability. A target that has it renders the request; a
// target that does not REFUSES and says which measurement it is missing, rather
// than quietly rendering a locking build the author did not ask for.
func TestAConcurrentIndexNamesTheFactItNeeds(t *testing.T) {
	tests := []struct {
		name           string
		profile        schemastate.Profile
		wantStatus     schemachange.Status
		wantMissing    []capability.Capability
		wantStatements []string
	}{
		{
			name:       "a target that can build without locking",
			profile:    postgresProfile(),
			wantStatus: schemachange.Planned,
			wantStatements: []string{
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS \"idx_widget_ab\" ON \"widget\" (\"a\", \"b\");\n",
			},
		},
		{
			name:        "a target that cannot",
			profile:     concurrencylessPostgres(),
			wantStatus:  schemachange.Blocked,
			wantMissing: []capability.Capability{capability.CreateIndexConcurrently},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := indexedWidget(false, "a", "b")
			description.Indexes[0].Concurrently = true

			changes := changesForProfile(c, description, indexedWidgetCatalog(false), test.profile)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Status, qt.Equals, test.wantStatus)
			c.Assert(changes[0].MissingFacts, qt.DeepEquals, test.wantMissing)
			c.Assert(changes[0].RequiredFacts, qt.DeepEquals,
				[]capability.Capability{capability.CreateIndexConcurrently})
			c.Assert(statementsOrNothing(c, changes, test.profile), qt.DeepEquals, test.wantStatements)
		})
	}
}

// TestAnOrdinaryIndexNeedsNoFact is the control. A change that named the
// capability whatever the source asked for would block every index build on
// every target without it, which is most of them.
func TestAnOrdinaryIndexNeedsNoFact(t *testing.T) {
	c := qt.New(t)

	changes := changesForProfile(
		c, indexedWidget(false, "a", "b"), indexedWidgetCatalog(false), concurrencylessPostgres())

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
	c.Assert(changes[0].RequiredFacts, qt.HasLen, 0)
}

// concurrencylessPostgres is PostgreSQL 17 with the one capability removed, so
// the rows differ in that fact and in nothing else. Using another dialect would
// vary the renderer and the identifier semantics at the same time.
func concurrencylessPostgres() schemastate.Profile {
	profile := postgresProfile()
	profile.Capabilities = profile.Capabilities.With(capability.CreateIndexConcurrently, false)
	return profile
}

// statementsOrNothing renders a plan, or reports none for a change the planner
// refuses. Plan returns an error for a blocked change, which is the behaviour
// under test rather than a failure of the test.
func statementsOrNothing(
	c *qt.C,
	changes []schemachange.Change,
	profile schemastate.Profile,
) []string {
	c.Helper()
	operations, err := schemachange.Plan(changes, profile)
	return map[bool]func() []string{
		true:  func() []string { return nil },
		false: func() []string { return schemachange.Statements(operations) },
	}[err != nil]()
}
