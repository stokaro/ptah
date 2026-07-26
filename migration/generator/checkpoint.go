package generator

import (
	"fmt"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/capability"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// GenerateCheckpoint renders a full cumulative schema as a checkpoint migration
// body pair. The up body creates every object in dependency order; the down
// body drops them in reverse.
//
// It diffs the schema against an empty database, so it is deterministic and
// needs no live database connection. Callers obtain the schema either by
// introspecting a database that has the whole migration directory applied (and
// converting the result with internal/convert/dbschematogo) or from Go
// entities / schema files. An empty schema yields empty up and down bodies.
func GenerateCheckpoint(schema *goschema.Database, dialect string) (upSQL, downSQL string, err error) {
	if schema == nil {
		return "", "", fmt.Errorf("checkpoint schema is required")
	}

	empty := &dbschematypes.DBSchema{}
	diff := schemadiff.CompareWithDialect(schema, empty, dialect)
	spec, _, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
		Diff:         diff,
		Generated:    schema,
		DBSchema:     empty,
		Dialect:      dialect,
		Capabilities: capability.ForDialect(dialect),
	})
	if err != nil {
		return "", "", fmt.Errorf("generate checkpoint: %w", err)
	}
	return spec.UpSQL, spec.DownSQL, nil
}
