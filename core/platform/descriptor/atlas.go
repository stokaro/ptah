package descriptor

// Atlas states where an engine stands in the Atlas CLI contract ptah-compat
// implements: the names an Atlas dev URL gives the engine, and the parts of
// an Atlas HCL schema that belong to the engine alone. The values are measured
// on the pinned Atlas community binary. A new engine that wants a dev URL or
// an HCL attribute of its own on the compatibility surface states them here,
// and the packages that read them name no engine. The zero value is an engine
// no docker dev URL starts and whose HCL schema has nothing of its own.
type Atlas struct {
	// DockerImages are the names a docker://<name>/<tag>/<database> dev URL
	// gives the engine, such as "postgres" and "postgis" for PostgreSQL. They
	// are matched as written: the pinned community binary refuses
	// docker://POSTGRES/... as an unsupported image. A name belongs to one
	// dialect of a registry.
	DockerImages []string `json:"docker_images,omitempty"`
	// DockerDrivers are the drivers a docker+<driver>://_/<image> dev URL
	// names for the engine, of those the pinned community binary registers
	// after "docker+". It refuses every other driver as unknown. A driver
	// belongs to one dialect of a registry.
	DockerDrivers []string `json:"docker_drivers,omitempty"`
	// Extension reports that no Atlas edition has a driver for the engine, so
	// ptah-compat serves it only beyond the Atlas contract: strict
	// compatibility refuses its docker:// images, and a JSON schema inspection
	// warns about each property its document leaves out, since no Atlas
	// document of the engine exists whose shape the warnings depart from.
	Extension bool `json:"extension,omitempty"`
	// Schema is the schema Atlas names where the engine's URL and catalog
	// name none, such as SQLite's "main": the schema of the URL an Atlas
	// template prints, and of a table a JSON inspection reports. Empty names
	// none.
	Schema string `json:"schema,omitempty"`
	// CatalogForm is the form the pinned community binary's driver writes the
	// engine's planned statements in. The zero value says it has no driver for
	// the engine, so a plan keeps the planner's own statements.
	CatalogForm CatalogForm `json:"catalog_form,omitempty"`
	// HCL states the parts of an Atlas HCL schema that belong to the engine.
	HCL AtlasHCL `json:"hcl,omitzero"`
	// ChangeWords is the driver wording the pinned community binary names the
	// engine's changes in, in a migrate diff file's comments and a plan's
	// summary. The zero value is the PostgreSQL driver's wording.
	ChangeWords ChangeWords `json:"change_words,omitempty"`
}

// ChangeWords names whose words the pinned community binary uses for an
// engine's changes. Its drivers word one change differently: the MySQL driver
// writes a schema's sentences in lower case and names a dropped database, and
// the SQLite driver names each step of a table rebuild and a column's own
// change. An engine with no driver of its own takes the zero value.
type ChangeWords int

const (
	// PostgresDriverChangeWords is the PostgreSQL driver's wording, the zero
	// value.
	PostgresDriverChangeWords ChangeWords = iota
	// MySQLDriverChangeWords is the MySQL driver's wording.
	MySQLDriverChangeWords
	// SQLiteDriverChangeWords is the SQLite driver's wording.
	SQLiteDriverChangeWords
)

// CatalogForm names the driver whose statements a compatibility plan follows
// for an engine. Each driver writes a schema the way a read of its catalog prints
// one: its spelling of a type, the keys inside CREATE TABLE, and the action a server
// reports for a foreign key that declares none.
type CatalogForm int

const (
	// NoCatalogForm says the community binary has no driver for the engine, the
	// zero value.
	NoCatalogForm CatalogForm = iota
	// PostgresCatalogForm is the PostgreSQL driver's form.
	PostgresCatalogForm
	// MySQLCatalogForm is the MySQL driver's form on a server that reports NO
	// ACTION for a foreign key that declares no action.
	MySQLCatalogForm
	// MariaDBCatalogForm is the MySQL driver's form on a server that reports
	// RESTRICT for a foreign key that declares no action.
	MariaDBCatalogForm
	// SQLiteCatalogForm is the SQLite driver's form.
	SQLiteCatalogForm
)

// AtlasHCL states the parts of an Atlas HCL schema that belong to one engine.
// internal/atlashcl and internal/atlashclrender read it. Every name uses the
// alphabet of [Dialect.Name], and a list names each once.
type AtlasHCL struct {
	// TableProperties are the attributes of a table block the pinned
	// community binary reads from the engine's driver alone, such as MySQL's
	// charset and auto_increment. The HCL reader states each as a platform
	// property of every dialect that lists it, where the engine's owner reads
	// it, so two dialects of one family may list one attribute.
	TableProperties []string `json:"table_properties,omitempty"`
	// ColumnProperties are the attributes of a column block read the same
	// way, such as MySQL's on_update.
	ColumnProperties []string `json:"column_properties,omitempty"`
	// IndexProperties are the attributes of an index block read the same
	// way, such as the parser of a MySQL FULLTEXT index.
	IndexProperties []string `json:"index_properties,omitempty"`
	// RefusedBlocks are the top-level block types Ptah models and the pinned
	// community binary refuses in a whole document of the engine, such as
	// PostgreSQL's extension, policy and sequence. The compatibility surface
	// leaves each out of an inspection where nothing else in the document
	// names it. Empty refuses none.
	RefusedBlocks []string `json:"refused_blocks,omitempty"`
	// ColumnTypes are the column type names the pinned community binary
	// models as a bare HCL type expression for the engine, in lower case.
	// The HCL writer writes any other type as a sql() call. Empty means the
	// vocabulary is not measured, and every type is written as the model
	// holds it.
	ColumnTypes []string `json:"column_types,omitempty"`
}

// validateAtlas checks the names of [Atlas] one descriptor can check alone.
// The registry checks that no two dialects claim one docker image or driver.
func (d Dialect) validateAtlas() error {
	lists := []struct {
		kind  string
		names []string
	}{
		{"docker image", d.Atlas.DockerImages},
		{"docker driver", d.Atlas.DockerDrivers},
		{"HCL table property", d.Atlas.HCL.TableProperties},
		{"HCL column property", d.Atlas.HCL.ColumnProperties},
		{"HCL index property", d.Atlas.HCL.IndexProperties},
		{"refused HCL block", d.Atlas.HCL.RefusedBlocks},
		{"HCL column type", d.Atlas.HCL.ColumnTypes},
	}
	for _, list := range lists {
		if err := validNames(d.Name, list.kind, list.names); err != nil {
			return err
		}
	}
	return nil
}
