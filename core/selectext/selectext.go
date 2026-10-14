// Package selectext is the contract by which a feature owner says how a
// command's scope names its objects: the `--include`, `--exclude` and
// `--schema` selectors of the schema commands.
//
// An owner states, as data, the selector type that names an object of each
// of its kinds, how the selector reads the object's name, and the fields an
// exclusion may subtract from one. What only the owner can compute -- the
// object with a field cleared, and the objects a kept object requires,
// such as the resource pool a kept classifier routes to -- is one batched
// call to the owner's [Service] for each side a command filters. A [Set]
// freezes the owners one run selects; the filter imports no owner.
package selectext

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Naming says how a selector reads the name of an owner's object.
type Naming string

const (
	// InSchema names an object by its schema and its name. A selector matches
	// the bare name and the name qualified by its schema, or by the scope's
	// default schema when it has none, and an object leaves with its schema.
	InSchema Naming = "in-schema"
	// DatabaseWide names an object by its name alone, which is unique in the
	// database: a dot is part of the name, and no schema scopes the object.
	DatabaseWide Naming = "database-wide"
	// SchemaOrDatabase names an object that lives in a schema or on the
	// database itself, such as a SQL Server extended property. One with a
	// schema is named as [InSchema] names an object. One without a schema is
	// on the database: it is named by its name alone, as [DatabaseWide] names
	// an object, and no schema selects or removes it. InSchema cannot say
	// this, since an object with no schema is unqualified there and is read
	// under the scope's default schema.
	SchemaOrDatabase Naming = "schema-or-database"
	// SchemaWide names state that applies throughout an explicit schema, or
	// throughout the database when the schema is empty. It has no object selector.
	// Include selectors keep it only for explicitly selected schemas; a database
	// scope stays only when no include selector narrows the source.
	SchemaWide Naming = "schema-wide"
)

// Selection is how a scope names an owner's objects of one kind.
type Selection struct {
	// Kind is the owner's model.
	Kind schemaext.Kind
	// Type is the selector type that names an object of Kind, such as
	// "topic" in `*[type=topic]`. Several models may share a selector type
	// when its naming, relation behavior, and fields agree. SchemaWide leaves Type empty.
	Type string
	// Naming is how a selector reads the object's name.
	Naming Naming
	// Relation marks an object whose name is a relation's name, such as a
	// synonym: excluding the object excludes a table of its name too, since
	// both name one relation.
	Relation bool
	// Fields are the fields an exclusion may subtract from the object while
	// keeping it, such as "comment" in `*[type=synonym].comment`. The owner's
	// [Service] clears them.
	Fields []string
}

// ByNameAlone reports whether the selection names an object in schema by its
// name alone, with no schema to select or remove it: every object of a
// [DatabaseWide] selection, and one with no schema of a [SchemaOrDatabase]
// selection.
func (s Selection) ByNameAlone(schema string) bool {
	return s.Naming == DatabaseWide || (s.Naming == SchemaOrDatabase && schema == "")
}

// Extension is what one owner contributes to a scope.
type Extension struct {
	// Owner names the owner, as its provider ID does.
	Owner string
	// Selections are the owner's selections, one for each kind a scope names.
	// The provider that registers the extension owns their desired codecs.
	Selections []Selection
	// Service clears fields and names the objects kept objects require. It
	// is required when a selection has Fields; without it nothing is cleared
	// and a kept object requires nothing.
	Service Service
	// Scopes describes the schema and named roles of SchemaWide subjects in one
	// batch. It is required for those selections, including coverage-only subjects.
	Scopes ScopeService
	// Baseline subtracts members of starting-point objects. Without it, objects
	// are selected as a whole by identity.
	Baseline BaselineService
	// ChangeKinds names the changes Additive projects. Each kind belongs to
	// this provider and appears in at most one extension.
	ChangeKinds []schemaext.Kind
	// Additive keeps additions inside mixed changes. It is required exactly
	// when ChangeKinds is nonempty.
	Additive AdditiveService
}

// Service is the owner's batched part of a scope, called once for each side
// a command filters.
type Service interface {
	SelectObjects(ctx context.Context, request Request) (Result, error)
}

// Request is what one side keeps of an owner's objects once the selectors
// have been applied.
type Request struct {
	// Kept are the side's objects of the owner's kinds that the selectors
	// keep.
	Kept schemaext.Objects
	// Clears are the fields an exclusion subtracts from kept objects.
	Clears []Clear
}

// Clear is one field an exclusion subtracts from one kept object.
type Clear struct {
	// Object is the kept object.
	Object objectidentity.ID
	// Field is one of the object's selection's Fields.
	Field string
}

// Result is the owner's answer to a [Request].
type Result struct {
	// Cleared are the kept objects with their requested fields cleared, each
	// replacing the kept object of its identity.
	Cleared []schemaext.Object
	// Requires are objects of the owner's kinds that the kept objects need,
	// which a scope's selection keeps although no selector names them, when
	// the side holds them. An exclusion is authoritative and ignores them.
	Requires []objectidentity.ID
}

// ErrUnselected is returned for a [Set] nobody built: the zero value. A
// caller that wants no owner passes [None] instead.
var ErrUnselected = errors.New("no scope selection owners were selected")

// Set is the frozen collection of extensions one run selects. The zero value
// is unselected; [None] selects no owner. A Set is safe for concurrent use.
type Set struct {
	selected   bool
	extensions []Extension
	selections []Selection
}

// None returns a selected set without owners: a scope names the common
// objects alone.
func None() Set {
	return Set{selected: true}
}

// NewSet freezes extensions into a set. It refuses an extension without an
// owner, an invalid model kind, unknown naming, duplicate fields or kinds,
// conflicting definitions of a shared selector type, or fields without a Service.
// Named selections require a type. SchemaWide requires Scopes and allows no type,
// relation marker, or field selector. Each owner registers at most one Scopes
// contribution, which receives all of its schema-wide models in one batch.
// Additive projections require valid, unique change kinds and a service;
// baseline projections require model selections.
func NewSet(extensions ...Extension) (Set, error) {
	set := Set{selected: true}
	kinds := make(map[schemaext.Kind]string)
	types := make(map[string]Selection)
	scopeOwners := make(map[string]bool)
	changeKinds := make(map[schemaext.Kind]bool)
	for index, extension := range extensions {
		if strings.TrimSpace(extension.Owner) == "" {
			return Set{}, fmt.Errorf("scope selection extension %d names no owner", index)
		}
		if len(extension.schemaWideKinds()) > 0 {
			if scopeOwners[extension.Owner] {
				return Set{}, fmt.Errorf("%w: owner %q registers schema-wide scope services twice", schemaext.ErrDuplicate, extension.Owner)
			}
			scopeOwners[extension.Owner] = true
		}
		if err := validateProjections(extension, changeKinds); err != nil {
			return Set{}, err
		}
		extension.ChangeKinds = slices.Clone(extension.ChangeKinds)
		for _, selection := range extension.Selections {
			if err := validateSelection(extension, selection); err != nil {
				return Set{}, err
			}
			if owner, claimed := kinds[selection.Kind]; claimed {
				return Set{}, fmt.Errorf("%w: model %q is selected by %s and %s", schemaext.ErrDuplicate, selection.Kind, owner, extension.Owner)
			}
			selection.Fields = slices.Clone(selection.Fields)
			slices.Sort(selection.Fields)
			if selection.Type != "" {
				if existing, claimed := types[selection.Type]; claimed && !sameSelector(existing, selection) {
					return Set{}, fmt.Errorf("%w: selector type %q has conflicting definitions", schemaext.ErrDuplicate, selection.Type)
				}
				types[selection.Type] = selection
			}
			kinds[selection.Kind] = extension.Owner
			set.selections = append(set.selections, selection)
		}
		extension.Selections = cloneSelections(extension.Selections)
		set.extensions = append(set.extensions, extension)
	}
	slices.SortFunc(set.selections, func(a, b Selection) int {
		if compared := strings.Compare(a.Type, b.Type); compared != 0 {
			return compared
		}
		return strings.Compare(string(a.Kind), string(b.Kind))
	})
	return set, nil
}

// validateSelection refuses a selection a scope cannot apply.
func validateSelection(extension Extension, selection Selection) error {
	switch {
	case !selection.Kind.Valid() || (selection.Naming != SchemaWide && strings.TrimSpace(selection.Type) == ""):
		return fmt.Errorf("scope selection of %s names no kind or no selector type", extension.Owner)
	case selection.Naming == SchemaWide && (selection.Type != "" || selection.Relation || len(selection.Fields) != 0 || extension.Scopes == nil):
		return fmt.Errorf("scope selection of %s requires a schema-wide service and no selector type, relation or fields", extension.Owner)
	case selection.Naming != InSchema && selection.Naming != DatabaseWide && selection.Naming != SchemaOrDatabase && selection.Naming != SchemaWide:
		return fmt.Errorf("scope selection %q of %s has unknown naming %q", selection.Type, extension.Owner, selection.Naming)
	case len(selection.Fields) > 0 && extension.Service == nil:
		return fmt.Errorf("scope selection %q of %s names fields without a service to clear them", selection.Type, extension.Owner)
	}
	for i, field := range selection.Fields {
		if strings.TrimSpace(field) == "" || slices.Contains(selection.Fields[:i], field) {
			return fmt.Errorf("scope selection %q of %s names field %q empty or twice", selection.Type, extension.Owner, field)
		}
	}
	return nil
}

// Selected reports whether the set was built, by [NewSet] or [None].
func (s Set) Selected() bool {
	return s.selected
}

// Selections returns independent selections, ordered by selector type then kind.
func (s Set) Selections() []Selection { return cloneSelections(s.selections) }

// ForType returns every model named by a selector type, ordered by kind.
// The returned slice and fields are independent. An unknown type returns nil.
func (s Set) ForType(selectorType string) []Selection {
	var result []Selection
	for _, selection := range s.selections {
		if selectorType != "" && selection.Type == selectorType {
			result = append(result, selection)
		}
	}
	return cloneSelections(result)
}

func sameSelector(a, b Selection) bool {
	return a.Naming == b.Naming && a.Relation == b.Relation && slices.Equal(a.Fields, b.Fields)
}

func cloneSelections(selections []Selection) []Selection {
	result := slices.Clone(selections)
	for i := range result {
		result[i].Fields = slices.Clone(result[i].Fields)
	}
	return result
}

// SelectObjects hands each extension with a service the kept objects of its
// kinds and the clears of those objects, in one call per extension, and
// returns every cleared object and required identity, extension by
// extension. An extension whose kinds the side keeps nothing of and clears
// nothing of is not called. It refuses a clear of a field no selection names,
// a cleared object the extension was not handed or answered twice, and a
// required identity of a kind the extension does not select. An extension's
// own error is returned as it is.
func (s Set) SelectObjects(ctx context.Context, request Request) (Result, error) {
	if !s.selected {
		return Result{}, ErrUnselected
	}
	if err := s.checkClears(request.Clears); err != nil {
		return Result{}, err
	}
	var combined Result
	for _, extension := range s.extensions {
		if extension.Service == nil {
			continue
		}
		owned := extension.selectRequest(request)
		if len(owned.Kept.Refs()) == 0 && len(owned.Clears) == 0 {
			continue
		}
		result, err := extension.Service.SelectObjects(ctx, owned)
		if err != nil {
			return Result{}, err
		}
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		if err := extension.checkResult(owned, result); err != nil {
			return Result{}, err
		}
		combined.Cleared = append(combined.Cleared, result.Cleared...)
		combined.Requires = append(combined.Requires, result.Requires...)
	}
	return combined, nil
}

// checkClears refuses a clear of a field no selection of the object's kind
// names.
func (s Set) checkClears(clears []Clear) error {
	for _, removal := range clears {
		index := slices.IndexFunc(s.selections, func(selection Selection) bool {
			return selection.Kind == schemaext.Kind(removal.Object.Kind)
		})
		if index < 0 || !slices.Contains(s.selections[index].Fields, removal.Field) {
			return fmt.Errorf("%w: no scope selection of %s names field %q", schemaext.ErrInvalidValue, removal.Object.Kind, removal.Field)
		}
	}
	return nil
}

// owns reports whether the extension selects kind.
func (e Extension) owns(kind objectidentity.Kind) bool {
	return slices.ContainsFunc(e.Selections, func(selection Selection) bool { return selection.Kind == schemaext.Kind(kind) })
}

// selectRequest is what request holds of the extension's kinds.
func (e Extension) selectRequest(request Request) Request {
	owned := Request{Kept: request.Kept.Select(func(ref objectidentity.ID) bool { return e.owns(ref.Kind) })}
	for _, removal := range request.Clears {
		if e.owns(removal.Object.Kind) {
			owned.Clears = append(owned.Clears, removal)
		}
	}
	return owned
}

// checkResult refuses a cleared object the extension was not handed or
// answered twice, and a required identity of a kind it does not select.
func (e Extension) checkResult(request Request, result Result) error {
	cleared := make(map[objectidentity.Key]bool, len(result.Cleared))
	for _, object := range result.Cleared {
		key := object.Ref.Key()
		if _, held, err := request.Kept.Get(object.Ref); err != nil || !held || cleared[key] {
			return fmt.Errorf("%w: scope selection service of %s cleared %s, which it was not handed or cleared twice",
				schemaext.ErrInvalidValue, e.Owner, object.Ref)
		}
		cleared[key] = true
	}
	for _, ref := range result.Requires {
		if !e.owns(ref.Kind) {
			return fmt.Errorf("%w: scope selection service of %s requires %s, of a kind it does not select",
				schemaext.ErrInvalidValue, e.Owner, ref)
		}
	}
	return nil
}

// Runtime supplies the scope selections of one explicit provider selection,
// so a command scopes with the owners it reads and compares with.
// *ptah.run/engine.Runtime implements it.
type Runtime interface {
	Selections() Set
}

// validateProjections keeps projection registration independent of selector naming.
func validateProjections(extension Extension, changeKinds map[schemaext.Kind]bool) error {
	if (extension.Additive == nil) != (len(extension.ChangeKinds) == 0) {
		return fmt.Errorf("%w: additive selection requires change kinds and a service", schemaext.ErrInvalidValue)
	}
	for _, kind := range extension.ChangeKinds {
		if !kind.Valid() || changeKinds[kind] {
			return fmt.Errorf("%w: additive selection kind %q is invalid or repeated", schemaext.ErrInvalidValue, kind)
		}
		changeKinds[kind] = true
	}
	if extension.Baseline != nil && len(extension.Selections) == 0 {
		return fmt.Errorf("%w: baseline selection requires model selections", schemaext.ErrInvalidValue)
	}
	return nil
}
