package descriptor

import "fmt"

// Cleanup states what a cleanup of a development database destroys on an
// engine and how it spells the statements, which internal/schemaclean reads,
// and where the engine's writer lists from its catalog what the schema reader
// does not describe, which the writer reads. The zero value is an engine the
// cleanup has not measured: its plan reports the tables every writer drops
// and nothing else.
type Cleanup struct {
	// ForeignKeys, Views, MaterializedViews, Enums, Domains, Composites,
	// Ranges and Functions are the kinds beyond tables the writer's cleanup
	// destroys and the reader's snapshot carries, so the plan lists them.
	ForeignKeys       bool `json:"foreign_keys,omitempty"`
	Views             bool `json:"views,omitempty"`
	MaterializedViews bool `json:"materialized_views,omitempty"`
	Enums             bool `json:"enums,omitempty"`
	Domains           bool `json:"domains,omitempty"`
	Composites        bool `json:"composites,omitempty"`
	Ranges            bool `json:"ranges,omitempty"`
	Functions         bool `json:"functions,omitempty"`
	// CatalogInventory reports that the cleanup was measured against the
	// engine's catalog as PostgreSQL keeps it: the PostgreSQL family's writer
	// then lists the runtime objects the snapshot does not carry, orders
	// views by their dependencies and locks the relations a scoped plan
	// drops.
	CatalogInventory bool `json:"catalog_inventory,omitempty"`
	// StoredObjects reports that the writer drops events and stored routines
	// the snapshot does not carry, which the MySQL family's writer then lists
	// from the catalog as MySQL keeps it.
	StoredObjects bool `json:"stored_objects,omitempty"`
	// RevisionTables reports that the writer drops Ptah's revision tables
	// while the reader hides them by name, so the cleanup finds them itself.
	RevisionTables bool `json:"revision_tables,omitempty"`
	// RevisionProbe is the catalog query that finds the revision tables in a
	// schema. Its first parameter is the schema and {names} stands for the
	// placeholders of the table names that follow. Empty means the writer
	// finds them by name itself.
	RevisionProbe string `json:"revision_probe,omitempty"`
	// RevisionTablesEverywhere reports that the reader leaves the revision
	// table names out of every directory, so each one the writer finds is
	// listed, two directories may each hold one.
	RevisionTablesEverywhere bool `json:"revision_tables_everywhere,omitempty"`
	// WriterListing reports that the writer lists what it drops, each object
	// with its drop statement, in its drop order, so the plan is that list.
	WriterListing bool `json:"writer_listing,omitempty"`
	// SchemaScoped reports that the writer drops only the objects of its own
	// schema, DefaultSchema when none is configured, and runs a scoped plan in
	// one transaction, each drop under a savepoint, so a refused drop is
	// retried once the drops after it are done. A schema-scoped dialect
	// states the savepoint's Create and Rollback (Dialect.Savepoint).
	SchemaScoped bool `json:"schema_scoped,omitempty"`
	// Restrict reports that a drop refuses dependents: DROP TABLE, DROP
	// VIEW and DROP SEQUENCE end with RESTRICT, and a routine is dropped by
	// its parameter list with RESTRICT.
	Restrict bool `json:"restrict,omitempty"`
	// Sync reports that DROP TABLE and DROP VIEW end with SYNC, so the drop
	// returns once the data is gone, and that a materialized view is dropped
	// with DROP VIEW, the engine's only statement for it.
	Sync bool `json:"sync,omitempty"`
	// ForeignKeyDrop is the clause that drops a foreign key from its table,
	// "DROP CONSTRAINT" when empty.
	ForeignKeyDrop string `json:"foreign_key_drop,omitempty"`
	// DropsDatabases reports that a schema the cleanup drops is a database,
	// dropped with DROP DATABASE.
	DropsDatabases bool `json:"drops_databases,omitempty"`
	// ViewsFirst reports that the writer drops views before the foreign keys
	// and tables they read, and routines after the tables.
	ViewsFirst bool `json:"views_first,omitempty"`
}

// validateCleanup checks the fields declared in this file.
func (d Dialect) validateCleanup() error {
	if d.Cleanup.SchemaScoped && (d.Savepoint.Create == "" || d.Savepoint.Rollback == "") {
		return fmt.Errorf("%w: dialect %q: a schema-scoped cleanup retries a refused drop under a savepoint, so the dialect states how a savepoint is created and rolled back",
			ErrInvalidDescriptor, d.Name)
	}
	return nil
}
