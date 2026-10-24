package rundiag

import (
	"fmt"
	"log/slog"
)

// The categories a run aggregates. Each is defined here, once, so a warning
// about many objects reads the same whichever command and binary raised it.
var (
	// TruncatedNames is a name a SQL document writes longer than the
	// identifier limit of a server that cuts it, such as a 64-byte foreign
	// key name on PostgreSQL. The document is read under the name the server
	// keeps either way; two different names cut to one are refused, not
	// recorded here. Its records carry the limit as the "limit" attribute.
	TruncatedNames = &Category{
		Name: "truncated-name",
		Summary: func(records []Record) string {
			if limit, ok := Shared(records, "limit"); ok && limit.Kind() == slog.KindInt64 {
				return fmt.Sprintf("%d names are longer than %d bytes; the server keeps their first %d bytes, and they are read that way",
					len(records), limit.Int64(), limit.Int64())
			}
			return fmt.Sprintf("%d names are longer than the server's identifier limit; they are read the way the server stores them", len(records))
		},
	}

	// RedeclaredConstraints is a constraint a SQL document drops and adds
	// again under its name with another definition, such as a supplemental
	// file that copies an ORM's foreign key to change it: the copy goes on
	// deciding the key after the ORM changes it.
	RedeclaredConstraints = &Category{
		Name: "redeclared-constraint",
		Summary: func(records []Record) string {
			return fmt.Sprintf("%d constraints are dropped and added again with another definition; the later declarations decide them", len(records))
		},
	}

	// IgnoredProjectSettings is an atlas.hcl name the community binary
	// accepts without acting on.
	IgnoredProjectSettings = &Category{
		Name: "ignored-atlas-hcl",
		Summary: func(records []Record) string {
			return fmt.Sprintf("%d atlas.hcl settings are ignored for Atlas compatibility and have no effect", len(records))
		},
	}

	// UncoveredMigrationFiles is a SQL file in an Atlas migration directory
	// that atlas.sum does not cover, such as one below the top level, which
	// the community binary never runs.
	UncoveredMigrationFiles = &Category{
		Name: "uncovered-migration-file",
		Summary: func(records []Record) string {
			return fmt.Sprintf("%d SQL files are not covered by atlas.sum and will not run; Atlas migrations are top-level files named *.sql", len(records))
		},
	}

	// IgnoredSchemaFileSettings is a name in a schema file that is accepted
	// and dropped: an HCL block the community binary ignores, or a DBML
	// construct the schema model cannot carry.
	IgnoredSchemaFileSettings = &Category{
		Name: "ignored-schema-file",
		Summary: func(records []Record) string {
			return fmt.Sprintf("%d schema file constructs are ignored and have no effect", len(records))
		},
	}
)
