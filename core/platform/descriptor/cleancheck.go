package descriptor

import "fmt"

// CleanCheck states how Ptah tells whether a database holds anything before
// it adopts the database for migrations or resets it as a dev database.
// internal/migrateclean reads it. The zero value is an engine neither check
// governs: `migrate apply` adopts any database, and a dev database is reset
// without a look at what it holds. A registry refuses a combination of values
// the reader would ignore.
type CleanCheck struct {
	// Adoption reports that `migrate apply` refuses to adopt a database that
	// holds a table its revision table does not account for, as the pinned
	// Atlas community binary was measured to refuse one on the engine.
	Adoption bool `json:"adoption,omitempty"`
	// AdoptionCounts reports that such a refusal states how many tables the
	// database holds, the revision table included, rather than naming the
	// first of them.
	AdoptionCounts bool `json:"adoption_counts,omitempty"`
	// Dev reports that a dev database is checked before it is reset, and is
	// refused when it holds anything the reset would drop.
	Dev bool `json:"dev,omitempty"`
	// Tables is the catalog query that lists the base tables a reset of the
	// connection's database would empty, on an engine whose URL selects no
	// schema: Schemas.Selection is empty or SchemaSelectionSingle. It binds no
	// parameter. It selects the schema and the name of each table, or the
	// name alone where Schemas.Selection is SchemaSelectionSingle. An engine
	// whose URL selects a schema leaves it empty: its tables are read through
	// the catalog its selection names, as internal/schemaselection reads that
	// engine's realm.
	Tables string `json:"tables,omitempty"`
	// CatalogOrder reports that a refusal names the first table in the
	// order the catalog query returns them, which is the order they were
	// created in, rather than the first in byte order.
	CatalogOrder bool `json:"catalog_order,omitempty"`
	// ConnectedSchema reports that a dev refusal of a URL that pinned a
	// schema says the object is "in connected schema" rather than naming the
	// schema, as the pinned binary words it on PostgreSQL.
	ConnectedSchema bool `json:"connected_schema,omitempty"`
	// ResetListing reports that a dev database is judged by what its
	// writer's reset would drop alone, as the writer lists it, with no
	// catalog query. The first object listed refuses the database.
	ResetListing bool `json:"reset_listing,omitempty"`
	// StartingPoint reports that a dev database an atlas.hcl docker block
	// provisioned is not judged: the claim records the state the block
	// leaves, and every reset returns the database to it.
	StartingPoint bool `json:"starting_point,omitempty"`
}

// validateCleanCheck refuses the clean-check values a reader would ignore
// without a word: a catalog query on an engine whose URL selects a schema,
// which is read through the catalog its selection names, a catalog query
// beside ResetListing, which reads none, and a counted adoption refusal on an
// engine whose adoption is not refused.
func (d Dialect) validateCleanCheck() error {
	check := d.CleanCheck
	selects := d.Schemas.Selection == SchemaSelectionSearchPath || d.Schemas.Selection == SchemaSelectionDatabase
	switch {
	case check.Tables != "" && selects:
		return fmt.Errorf("%w: dialect %q: a clean check states no tables query where the URL selects a schema by %q",
			ErrInvalidDescriptor, d.Name, d.Schemas.Selection)
	case check.Tables != "" && check.ResetListing:
		return fmt.Errorf("%w: dialect %q: a clean check that reads the reset listing states no tables query",
			ErrInvalidDescriptor, d.Name)
	case check.AdoptionCounts && !check.Adoption:
		return fmt.Errorf("%w: dialect %q: a counted adoption refusal needs a refused adoption",
			ErrInvalidDescriptor, d.Name)
	default:
		return nil
	}
}
