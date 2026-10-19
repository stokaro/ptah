package atlasreport

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

// atlasFact is what the reports read of one dialect's place in the Atlas
// contract (descriptor.Atlas).
type atlasFact struct {
	// name is the dialect's canonical name.
	name string
	// schema is the schema Atlas names where the URL and the catalog name
	// none, such as SQLite's main.
	schema string
	// extension reports that no Atlas edition has a driver for the dialect.
	extension bool
}

// atlasFacts holds the fact of every dialect. An unknown dialect gets the
// zero fact.
var atlasFacts = descriptortable.New(func(described descriptor.Dialect) atlasFact {
	return atlasFact{name: described.Name, schema: described.Atlas.Schema, extension: described.Atlas.Extension}
})
