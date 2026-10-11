// Package pgextension owns the PostgreSQL extension model: an extension as a
// database-wide feature object identified by its name, in a desired and an
// observed representation, the change between them, and the operations that
// carry a change to a renderer. It contains no provider selection, database
// access or SQL.
//
// The model keeps PostgreSQL's semantics. An extension is found by the
// control file of its exact name, is installed into one schema, and carries a
// version that moves in one direction only. Other objects use what an
// extension supplies -- types, functions, operator classes -- so a plan
// creates an extension before them and drops it after them. A plan step
// names the extension in its effects with [Subject], an identity of the
// common [objectidentity.KindExtension] kind, so another owner can order its
// own steps after the extension without importing this package.
package pgextension

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Owner is the provider identity that owns every extension codec and service.
// It is separate from the targets the services are registered for.
const Owner = "ptah.run/pgextension"

const (
	// ExtensionKind identifies one extension.
	ExtensionKind schemaext.Kind = "ptah.run/pgextension/extension"
	// ChangeKind identifies the transition of one extension.
	ChangeKind schemaext.Kind = "ptah.run/pgextension/extension-change"
	// OperationKind identifies a statement that creates, alters or drops one
	// extension.
	OperationKind schemaext.Kind = "ptah.run/pgextension/extension-operation"
	// CommentOperationKind identifies the statement that sets one
	// extension's comment.
	CommentOperationKind schemaext.Kind = "ptah.run/pgextension/extension-comment-operation"
)

// Ref builds the identity of the extension name. The name is compared
// exactly as written: PostgreSQL finds an extension by the control file of
// that name, so no folding makes two spellings one extension.
func Ref(name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(ExtensionKind), Name: objectidentity.Part{Source: name, Normalized: name}}
}

// Name returns the extension name ref identifies.
func Name(ref objectidentity.ID) string { return ref.Name.Source }

// Subject returns the identity a plan step's effect gives the extension ref
// identifies: the same name under the common extension kind, which another
// owner can name without this package.
func Subject(ref objectidentity.ID) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.KindExtension, Name: ref.Name}
}

// ValidateRef requires an identity [Ref] builds from a name that is valid
// text. An extension lives in no schema and on no table, so an identity that
// names either is refused.
func ValidateRef(ref objectidentity.ID) error {
	if err := schemaext.ValidText("extension name", ref.Name.Source); err != nil {
		return err
	}
	if ref.Name.Source == "" || ref != Ref(ref.Name.Source) {
		return fmt.Errorf("%w: an extension is identified by its name alone, not %s", schemaext.ErrInvalidValue, ref)
	}
	return nil
}

// validTexts checks field/value pairs in order with [schemaext.ValidText], so
// the first invalid field is the one a refusal names.
func validTexts(pairs ...string) error {
	for i := 0; i+1 < len(pairs); i += 2 {
		if err := schemaext.ValidText("extension "+pairs[i], pairs[i+1]); err != nil {
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
