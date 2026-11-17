// Package pgpublication owns the PostgreSQL logical replication publication
// model: a publication as a database-wide feature object identified by its
// name, in a desired and an observed representation, the change between them,
// and the operations that carry a change to a renderer. It contains no
// provider selection, database access or SQL.
//
// The model keeps PostgreSQL's semantics. A publication names the rows a
// logical replication consumer can receive, in one of four membership forms
// whose behavior for a table created later differs: no tables, the tables it
// names, every table of the schemas it names (TABLES IN SCHEMA), and every
// table of the database (FOR ALL TABLES). The forms are kept apart rather than
// flattened into a table list, because a table created after the plan joins a
// schema-wide or database-wide publication and no other. A publication's
// publish set, publish_via_partition_root and publish_generated_columns are
// part of what it publishes. A row filter or a column list on a member is read
// and never written: a change that would keep such a member without its filter
// publishes more than the publication does, so the planner refuses it.
//
// Creating a publication creates no subscription, starts no consumer and
// replicates no DDL.
package pgpublication

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Owner is the provider identity that owns every publication codec and
// service. It is separate from the targets the services are registered for.
const Owner = "ptah.run/pgpublication"

const (
	// PublicationKind identifies one publication.
	PublicationKind schemaext.Kind = "ptah.run/pgpublication/publication"
	// ChangeKind identifies the transition of one publication.
	ChangeKind schemaext.Kind = "ptah.run/pgpublication/publication-change"
	// OperationKind identifies a statement that creates, alters or drops one
	// publication.
	OperationKind schemaext.Kind = "ptah.run/pgpublication/publication-operation"
)

// Ref builds the identity of the publication name, spelled as the server
// stores it: a reader folds an unquoted name before it builds the identity,
// so the identity compares the stored names exactly.
func Ref(name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(PublicationKind), Name: objectidentity.Part{Source: name, Normalized: name}}
}

// Name returns the publication name ref identifies.
func Name(ref objectidentity.ID) string { return ref.Name.Source }

// ValidateRef requires an identity [Ref] builds from a name that is valid
// text. A publication lives in no schema and on no table, so an identity that
// names either is refused.
func ValidateRef(ref objectidentity.ID) error {
	if err := schemaext.ValidText("publication name", ref.Name.Source); err != nil {
		return err
	}
	if ref.Name.Source == "" || ref != Ref(ref.Name.Source) {
		return fmt.Errorf("%w: a publication is identified by its name alone, not %s", schemaext.ErrInvalidValue, ref)
	}
	return nil
}

// validTexts checks field/value pairs in order with [schemaext.ValidText], so
// the first invalid field is the one a refusal names.
func validTexts(pairs ...string) error {
	for i := 0; i+1 < len(pairs); i += 2 {
		if err := schemaext.ValidText("publication "+pairs[i], pairs[i+1]); err != nil {
			return err
		}
	}
	return nil
}

// modelError names the model and representation a refusal is about, as the
// typed error the codec boundary and the census recognize.
func modelError(kind schemaext.Kind, representation schemaext.Representation, err error) error {
	return &schemaext.InvalidModelError{Kind: kind, Representation: representation, Message: err.Error()}
}
