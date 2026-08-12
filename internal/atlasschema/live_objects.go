package atlasschema

import (
	"fmt"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/schemaclean"
)

// LiveSchemaObject is an object found by supplemental catalog inspection that
// the ordinary schema reader does not model completely.
type LiveSchemaObject struct {
	Kind             string
	Name             string
	ImplicitSequence bool
}

// LiveDatabaseValidator adapts an object-level policy to an open database
// source. Nil stays nil so full/default callers perform no supplemental
// catalog query.
func LiveDatabaseValidator(
	validate func(LiveSchemaObject) error,
) func(*dbschema.DatabaseConnection, []string) error {
	if validate == nil {
		return nil
	}
	return func(conn *dbschema.DatabaseConnection, schemas []string) error {
		return ValidateLiveObjects(conn, schemas, validate)
	}
}

// ValidateLiveObjects inventories catalog-only objects in the selected schema
// scope and applies validate before its caller publishes, compares, or mutates
// a schema. Nil performs no supplemental catalog query.
func ValidateLiveObjects(
	conn *dbschema.DatabaseConnection,
	schemas []string,
	validate func(LiveSchemaObject) error,
) error {
	if validate == nil {
		return nil
	}
	objects, err := schemaclean.InspectRuntimeObjects(conn, schemas)
	if err != nil {
		return fmt.Errorf("inventory live schema catalog: %w", err)
	}
	for _, object := range objects {
		if err := validate(LiveSchemaObject{
			Kind:             object.Type,
			Name:             object.Name,
			ImplicitSequence: object.Implicit,
		}); err != nil {
			return err
		}
	}
	return nil
}
