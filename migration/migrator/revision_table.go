package migrator

import (
	"fmt"
	"strings"
)

const (
	defaultPtahMigrationsTable = "schema_migrations"
	defaultAtlasRevisionsTable = "atlas_schema_revisions"
)

// RevisionTableFormat selects the database table layout used for migration
// revision metadata.
type RevisionTableFormat string

const (
	// RevisionTableFormatPtah stores metadata in Ptah's native
	// schema_migrations table layout.
	RevisionTableFormatPtah RevisionTableFormat = "ptah"
	// RevisionTableFormatAtlas stores metadata in Atlas's
	// atlas_schema_revisions table layout.
	RevisionTableFormatAtlas RevisionTableFormat = "atlas"
)

// ParseRevisionTableFormat normalizes a revision table format value.
func ParseRevisionTableFormat(value string) (RevisionTableFormat, error) {
	switch RevisionTableFormat(strings.ToLower(strings.TrimSpace(value))) {
	case "", RevisionTableFormatPtah:
		return RevisionTableFormatPtah, nil
	case RevisionTableFormatAtlas:
		return RevisionTableFormatAtlas, nil
	default:
		return "", fmt.Errorf("unknown revision table format %q: expected ptah or atlas", value)
	}
}

func (f RevisionTableFormat) isAtlas() bool {
	return f == RevisionTableFormatAtlas
}
