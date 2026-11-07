package rundiag

import (
	"fmt"
	"log/slog"
	"strings"
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

	// HCLLosses is a detail of an inspected schema the HCL document cannot
	// carry, such as the grantor of a permission, and leaves out.
	HCLLosses = &Category{
		Name: "hcl-loss",
		Summary: func(records []Record) string {
			return fmt.Sprintf("%d details of the inspected schema cannot be represented in HCL, and the document leaves them out", len(records))
		},
	}

	// AtlasOmittedBlocks is a block the Atlas-compatible HCL rendering leaves
	// out because the Atlas community CLI refuses a schema file that declares
	// one. Its records carry the variable that keeps every block as the
	// "variable" attribute.
	AtlasOmittedBlocks = &Category{
		Name: "atlas-omitted-block",
		Summary: func(records []Record) string {
			summary := fmt.Sprintf("%d blocks the Atlas community CLI refuses are omitted from the document", len(records))
			if variable, ok := Shared(records, "variable"); ok {
				summary += "; " + variable.String() + "=1 keeps every block Ptah models"
			}
			return summary
		},
	}

	// AtlasKeptBlocks is a block the Atlas community CLI refuses that the
	// Atlas-compatible HCL rendering keeps, because omitting it would lose
	// what another object of the document needs. The community CLI cannot
	// read such a document.
	AtlasKeptBlocks = &Category{
		Name: "atlas-kept-block",
		Summary: func(records []Record) string {
			return fmt.Sprintf("%d blocks the Atlas community CLI refuses are kept because other objects need them, "+
				"so the community CLI cannot read the document", len(records))
		},
	}

	// UnmodeledObjects is an object a read saw of a kind the server has and Ptah
	// does not model, which no comparison reads. Its records carry the object as
	// the "object" attribute and why nothing compares it as the "cause" attribute.
	// Like [UndecidedObjects], the summary names every object, since each is one
	// the run's answer does not cover.
	UnmodeledObjects = &Category{
		Name: "unmodeled-object",
		Summary: func(records []Record) string {
			return fmt.Sprintf("%d objects were left out: %s", len(records), objectsByCause(records))
		},
	}

	// UndecidedObjects is an object a comparison could not decide, so it plans
	// no change for it: a declared object of a kind the current side does not
	// describe, or a provider's object it could not compare. Its records carry
	// the object as the "object" attribute and why it was not decided as the
	// "cause" attribute.
	//
	// Unlike the other categories, the summary names every object. Each is a
	// change the plan leaves out, so the warning is written before the plan
	// it concerns rather than when the run ends; see [Settle].
	UndecidedObjects = &Category{
		Name: "undecided-object",
		Summary: func(records []Record) string {
			return fmt.Sprintf("%d objects get no planned change because the comparison could not decide them: %s",
				len(records), objectsByCause(records))
		},
	}
)

// objectsByCause names records' objects, those with one cause together and
// followed by it, the causes in the order they were first raised.
func objectsByCause(records []Record) string {
	var causes []string
	objects := make(map[string][]string)
	for _, record := range records {
		object, _ := attr(record, "object")
		cause, _ := attr(record, "cause")
		if _, seen := objects[cause.String()]; !seen {
			causes = append(causes, cause.String())
		}
		objects[cause.String()] = append(objects[cause.String()], object.String())
	}
	groups := make([]string, 0, len(causes))
	for _, cause := range causes {
		groups = append(groups, strings.Join(objects[cause], ", ")+" ("+cause+")")
	}
	return strings.Join(groups, "; ")
}
