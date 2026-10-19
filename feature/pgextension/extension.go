package pgextension

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// DesiredExtension is a declared extension. An empty field leaves its choice
// to the server: an empty Schema installs the extension into the
// connection's default schema, an empty Version installs the version the
// control file names as the default, and an empty Comment keeps the comment
// CREATE EXTENSION gives the extension from its control file.
type DesiredExtension struct {
	// Schema is the schema the extension is installed into.
	Schema string `json:"schema,omitempty"`
	// Version is the version the declaration asks for.
	Version string `json:"version,omitempty"`
	// Comment is the comment the declaration states.
	Comment string `json:"comment,omitempty"`
	// IfNotExists writes CREATE EXTENSION IF NOT EXISTS. It changes the
	// statement, not the extension: two declarations that differ only in it
	// declare the same extension.
	IfNotExists bool `json:"if_not_exists,omitempty"`
	// Provides is what a read reported the extension supplies, carried from
	// [ObservedExtension.Provides] when a read becomes a declaration. No
	// statement writes it and no comparison reads it: an export reads it to
	// tell an extension a document still depends on from one nothing uses.
	// An authored declaration leaves it empty.
	Provides []string `json:"provides,omitempty"`
}

// ObservedExtension is an installed extension as the catalog reports it.
type ObservedExtension struct {
	// Schema is the schema the extension's objects were installed into. It
	// is empty only where the current side is a document that left it to
	// the connection.
	Schema string `json:"schema"`
	// Version is the installed version, empty only where the current side is
	// a document that named none.
	Version string `json:"version"`
	// Relocatable reports whether the extension may be moved to another
	// schema, from pg_extension.extrelocatable. A move of one that is not is
	// refused by the server: `extension "plpgsql" does not support SET
	// SCHEMA`.
	Relocatable bool `json:"relocatable"`
	// Comment is the extension's comment, nil when it has none.
	Comment *string `json:"comment,omitempty"`
	// Provides lists the catalog names the extension supplies that do not
	// also resolve without it: its types, functions, relations, operator
	// classes and operator families, read from pg_depend. It answers what
	// would stop resolving were the extension dropped, which is rarely the
	// extension's own name: `isn` supplies the type `isbn`. Names pg_catalog
	// also supplies are left out, and so is a function named like a SQL
	// keyword when a type of the extension in its signature answers for it.
	// Empty means the read did not measure it.
	Provides []string `json:"provides,omitempty"`
}

// Kind returns the model identity.
func (*DesiredExtension) Kind() schemaext.Kind { return ExtensionKind }

// Redeclarable states that an extension is idempotent to declare: two
// packages that each use one may each declare it, and an equal declaration
// declares nothing new; see [schemaext.Redeclarable].
func (*DesiredExtension) Redeclarable() bool { return true }

// Guarded reports IfNotExists: the declaration admits an extension that is
// already installed.
func (v *DesiredExtension) Guarded() bool { return v != nil && v.IfNotExists }

// Kind returns the model identity.
func (*ObservedExtension) Kind() schemaext.Kind { return ExtensionKind }

// Clone returns an independent declaration.
func (v *DesiredExtension) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredExtension.Clone] without the interface. It shares no
// member list with v, and a nil receiver returns nil.
func (v *DesiredExtension) Copy() *DesiredExtension {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Provides = slices.Clone(v.Provides)
	return &copied
}

// Clone returns an independent observation.
func (v *ObservedExtension) Clone() schemaext.Value { return v.Copy() }

// InstalledSchema is the schema pg_extension reports the extension installed
// in; see [schemaext.SchemaInstallation]. An extension belongs to the whole
// database, so its identity names no schema.
func (v *ObservedExtension) InstalledSchema() string {
	if v == nil {
		return ""
	}
	return v.Schema
}

// Copy is [ObservedExtension.Clone] without the interface. It shares no
// comment or member list with v, and a nil receiver returns nil.
func (v *ObservedExtension) Copy() *ObservedExtension {
	if v == nil {
		return nil
	}
	copied := *v
	if v.Comment != nil {
		copied.Comment = new(*v.Comment)
	}
	copied.Provides = slices.Clone(v.Provides)
	return &copied
}

// Equal reports whether other declares the same extension in the same
// words, IfNotExists included, and carries the same members as a set. Two
// nil declarations are equal.
func (v *DesiredExtension) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredExtension)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Schema == w.Schema && v.Version == w.Version && v.Comment == w.Comment && v.IfNotExists == w.IfNotExists &&
		slices.Equal(sortedMembers(v.Provides), sortedMembers(w.Provides))
}

// Equal reports whether other observes the same extension in the same
// state. The members are a set: their order is the read's, not the
// extension's. Two nil observations are equal.
func (v *ObservedExtension) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedExtension)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Schema == w.Schema && v.Version == w.Version && v.Relocatable == w.Relocatable &&
		equalComment(v.Comment, w.Comment) && slices.Equal(sortedMembers(v.Provides), sortedMembers(w.Provides))
}

// sortedMembers returns the members in their canonical order, without
// changing members.
func sortedMembers(members []string) []string {
	sorted := slices.Clone(members)
	slices.Sort(sorted)
	return slices.Compact(sorted)
}

func equalComment(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// Desired is the declaration that keeps the observed extension as it is: in
// its schema, at its version, with its comment, and with the members the read
// reported. It writes IF NOT EXISTS, so a rollback that creates the extension
// again does not fail on one that survived.
func (v *ObservedExtension) Desired() *DesiredExtension {
	if v == nil {
		return nil
	}
	declared := &DesiredExtension{Schema: v.Schema, Version: v.Version, IfNotExists: true, Provides: slices.Clone(v.Provides)}
	if v.Comment != nil {
		declared.Comment = *v.Comment
	}
	return declared
}

// Observed is the observation a declaration predicts: the schema and the
// version it names, empty where it leaves them to the server, its comment, and
// the members it carries. Whether the extension may move is a fact of its
// control file that no declaration states, so the prediction says it may not.
func (v *DesiredExtension) Observed() *ObservedExtension {
	if v == nil {
		return nil
	}
	observed := &ObservedExtension{Schema: v.Schema, Version: v.Version, Provides: slices.Clone(v.Provides)}
	if v.Comment != "" {
		observed.Comment = new(v.Comment)
	}
	return observed
}

// DesiredObject validates a declaration and its identity and returns it as a
// feature object.
func DesiredObject(ref objectidentity.ID, extension DesiredExtension) (schemaext.Object, error) {
	if err := ValidateRef(ref); err != nil {
		return schemaext.Object{}, modelError(ExtensionKind, schemaext.Desired, err)
	}
	if err := ValidateDesired(&extension); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: &extension}, nil
}

// ObservedObject validates an observation and its identity and returns it as
// a feature object.
func ObservedObject(ref objectidentity.ID, extension ObservedExtension) (schemaext.Object, error) {
	if err := ValidateRef(ref); err != nil {
		return schemaext.Object{}, modelError(ExtensionKind, schemaext.Observed, err)
	}
	if err := ValidateObserved(&extension); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: extension.Copy()}, nil
}

// ValidateDesired refuses a nil declaration and text a statement or a
// catalog cannot carry.
func ValidateDesired(v *DesiredExtension) error {
	if v == nil {
		return modelError(ExtensionKind, schemaext.Desired, fmt.Errorf("%w: nil extension declaration", schemaext.ErrInvalidValue))
	}
	if err := validTexts("schema", v.Schema, "version", v.Version, "comment", v.Comment); err != nil {
		return modelError(ExtensionKind, schemaext.Desired, err)
	}
	if err := validMembers(v.Provides); err != nil {
		return modelError(ExtensionKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObserved refuses a nil observation and text a catalog cannot carry.
// An empty schema or version is accepted: a schema document compared as the
// current side states an extension the way a declaration does.
func ValidateObserved(v *ObservedExtension) error {
	if v == nil {
		return modelError(ExtensionKind, schemaext.Observed, fmt.Errorf("%w: nil extension observation", schemaext.ErrInvalidValue))
	}
	comment := ""
	if v.Comment != nil {
		comment = *v.Comment
	}
	if err := validTexts("schema", v.Schema, "version", v.Version, "comment", comment); err != nil {
		return modelError(ExtensionKind, schemaext.Observed, err)
	}
	if err := validMembers(v.Provides); err != nil {
		return modelError(ExtensionKind, schemaext.Observed, err)
	}
	return nil
}

// validMembers refuses a member a catalog cannot name: an empty one, and text
// no statement can carry.
func validMembers(members []string) error {
	for _, member := range members {
		if member == "" {
			return fmt.Errorf("%w: empty extension member", schemaext.ErrInvalidValue)
		}
		if err := validTexts("member", member); err != nil {
			return err
		}
	}
	return nil
}
