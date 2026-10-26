package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/internal/tableref"
)

// scopeSelections is how a command's scope names the provider's objects, as
// its engines declare under scope.selections (ADR 0039): one selection
// extension the provider owns, and the naming every identity of a selected
// kind fits.
type scopeSelections struct {
	extension selectext.Extension
	selected  map[objectidentity.Kind]selectedKind
}

// selectedKind is one kind an engine selects: the declaration, read for the
// host, and the identifier rules of the engine that declares it, which read
// a grant's object name.
type selectedKind struct {
	declared  *protocol.ScopeSelection
	selection selectext.Selection
	engine    *protocol.Engine
	rules     identifier.Semantics
}

var selectionNamings = map[protocol.ScopeSelection_Naming]selectext.Naming{
	protocol.ScopeSelection_IN_SCHEMA:          selectext.InSchema,
	protocol.ScopeSelection_DATABASE_WIDE:      selectext.DatabaseWide,
	protocol.ScopeSelection_SCHEMA_OR_DATABASE: selectext.SchemaOrDatabase,
}

// newScopeSelections reads the scope selections the engines of description
// declare, and nil when none declares one. Each engine's list is valid on its
// own, which the transport checked. Engines that select one kind must declare
// it alike and read names by the same rules, and a grant object type names
// one kind, since a scope reads a kind one way whichever engine holds it.
func newScopeSelections(description *protocol.Description) (*scopeSelections, error) {
	selected := make(map[objectidentity.Kind]selectedKind)
	grants := make(map[string]objectidentity.Kind)
	var order []objectidentity.Kind
	for _, engine := range description.GetEngines() {
		if len(engine.GetScopeSelections()) == 0 {
			continue
		}
		rules, err := identifierRules(engine.GetIdentifiers(), engine.GetDefaultSchema())
		if err != nil {
			return nil, err
		}
		for _, declared := range engine.GetScopeSelections() {
			kind := objectidentity.Kind(declared.GetKind())
			if known, seen := selected[kind]; seen {
				sameRules := proto.Equal(known.engine.GetIdentifiers(), engine.GetIdentifiers()) && known.engine.GetDefaultSchema() == engine.GetDefaultSchema()
				if !sameSelection(known.declared, declared) || !sameRules {
					return nil, selectionError("engines declare the scope selection of %s differently", kind)
				}
				continue
			}
			if objectType := declared.GetGrantObjectType(); objectType != "" {
				if owner, claimed := grants[objectType]; claimed {
					return nil, selectionError("grant object type %s names %s and %s", objectType, owner, kind)
				}
				grants[objectType] = kind
			}
			selected[kind] = selectedKind{declared: declared, engine: engine, rules: rules, selection: selectext.Selection{Kind: schemaext.Kind(kind),
				Type: declared.GetSelectorType(), Naming: selectionNamings[declared.GetNaming()], Relation: declared.GetRelation()}}
			order = append(order, kind)
		}
	}
	if len(order) == 0 {
		return nil, nil
	}
	result := &scopeSelections{selected: selected, extension: selectext.Extension{Owner: description.GetProviderId(), Grants: grantObjects{selected}}}
	keeps := false
	for _, kind := range order {
		result.extension.Selections = append(result.extension.Selections, selected[kind].selection)
		keeps = keeps || len(selected[kind].declared.GetKeepsRequired()) > 0
	}
	if keeps {
		result.extension.Service = requiredObjects{selected}
	}
	return result, nil
}

func sameSelection(a, b *protocol.ScopeSelection) bool {
	return a.GetSelectorType() == b.GetSelectorType() && a.GetNaming() == b.GetNaming() && a.GetRelation() == b.GetRelation() &&
		a.GetGrantObjectType() == b.GetGrantObjectType() && slices.Equal(sortedCopy(a.GetKeepsRequired()), sortedCopy(b.GetKeepsRequired()))
}

func sortedCopy(values []string) []string {
	result := slices.Clone(values)
	slices.Sort(result)
	return result
}

func selectionError(format string, args ...any) error {
	return fmt.Errorf("%w: invalid scope selections: %s", provider.ErrProtocol, fmt.Sprintf(format, args...))
}

// grantObjectType is a grant's object type compared without regard to case
// or spacing, as a selection declares it.
func grantObjectType(objectType string) string {
	return strings.Join(strings.Fields(strings.ToUpper(objectType)), " ")
}

// fit refuses an identity of a selected kind that its naming cannot read: a
// scope names such an object by its schema and its name alone, so a
// database, a catalog, a path, a parent or an overload would go unread and
// two objects would answer to one selector. An object in a schema spells the
// default schema when its engine has one, as a grant that names it does
// without a schema; a database-wide object has no schema. An identity of
// another kind fits. A nil receiver selects nothing.
func (s *scopeSelections) fit(ref objectidentity.ID) error {
	if s == nil {
		return nil
	}
	selected, ok := s.selected[ref.Kind]
	if !ok {
		return nil
	}
	naming := selected.selection.Naming
	qualified := !ref.Database.Empty() || !ref.Catalog.Empty() || !ref.Namespace.IsZero() || !ref.Parent.Empty() || !ref.Signature.IsZero()
	unschemed := naming == selectext.InSchema && ref.Schema.Empty() && selected.rules.DefaultSchema != ""
	if qualified || unschemed || ref.Name.Empty() || (naming == selectext.DatabaseWide && !ref.Schema.Empty()) {
		return fmt.Errorf("%w: the provider reports %s %s, which its %s scope selection does not name",
			provider.ErrProtocol, ref.Kind, ref, naming)
	}
	return nil
}

func (s *scopeSelections) selects(kind objectidentity.Kind) bool {
	if s == nil {
		return false
	}
	_, found := s.selected[kind]
	return found
}

// inSession removes only the opened database's exact qualification. A foreign
// or differently normalized database cannot become a local selector identity.
func (s *scopeSelections) inSession(ref objectidentity.ID, database string) (objectidentity.ID, error) {
	if !s.selects(ref.Kind) || ref.Database.Empty() {
		return ref, nil
	}
	if database == "" || ref.Database.Source != database || ref.Database.Normalized != database {
		return objectidentity.ID{}, selectionError("observed %s belongs to a database outside the exact session binding", ref.Kind)
	}
	ref.Database = objectidentity.Part{}
	return ref, nil
}

// grantObjects reads the object a grant names when the grant's object type is
// a selection's grant object type, with the selection's naming and the
// identifier rules of its engine.
type grantObjects struct {
	selected map[objectidentity.Kind]selectedKind
}

// OwnsGrantType reports whether a selection declares objectType.
func (g grantObjects) OwnsGrantType(objectType string) bool {
	_, ok := g.kindOf(objectType)
	return ok
}

// GrantObject reads objectName with the naming of the selection objectType
// names: a schema and a name, where an unqualified name is in the default
// schema; a name alone, in which a dot is a literal character; or, for an
// object that may belong to the database, a name with an optional schema.
func (g grantObjects) GrantObject(objectType, objectName string) (objectidentity.ID, bool) {
	selected, ok := g.kindOf(objectType)
	if !ok {
		return objectidentity.ID{}, false
	}
	kind := objectidentity.Kind(selected.declared.GetKind())
	name := strings.TrimSpace(objectName)
	if selected.selection.Naming == selectext.DatabaseWide {
		return objectidentity.ID{Kind: kind, Name: objectidentity.Part{Source: name, Normalized: selected.rules.TableIdentityKey(name)}}, name != ""
	}
	ref, parsed := tableref.Parse(name)
	if !parsed {
		return objectidentity.ID{}, false
	}
	if selected.selection.Naming == selectext.SchemaOrDatabase && !ref.Qualified {
		return objectidentity.ID{Kind: kind, Name: objectidentity.Part{Source: ref.Name, Normalized: selected.rules.TableIdentityKey(ref.Name)}}, true
	}
	return objectidentity.NewBuilder(selected.rules).SchemaScopedParts(kind, ref.Schema, ref.Name), true
}

func (g grantObjects) kindOf(objectType string) (selectedKind, bool) {
	wanted := grantObjectType(objectType)
	for _, selected := range g.selected {
		if declared := selected.declared.GetGrantObjectType(); declared != "" && declared == wanted {
			return selected, true
		}
	}
	return selectedKind{}, false
}

// requiredObjects keeps what a kept object requires, of the kinds its
// selection names in keeps_required: the objects the provider lists in the
// value's required objects.
type requiredObjects struct {
	selected map[objectidentity.Kind]selectedKind
}

// SelectObjects names the required objects of each kept object whose
// selection keeps them. A kept value that is not the provider's model refuses
// the scope rather than lose the dependency. It clears nothing: a provider's
// selection has no fields.
func (r requiredObjects) SelectObjects(_ context.Context, request selectext.Request) (selectext.Result, error) {
	kept, err := request.Kept.All()
	if err != nil {
		return selectext.Result{}, err
	}
	var result selectext.Result
	for _, object := range kept {
		keeps := r.selected[object.Ref.Kind].declared.GetKeepsRequired()
		if len(keeps) == 0 {
			continue
		}
		requirer, ok := object.Value.(schemaext.Requirer)
		if !ok {
			return selectext.Result{}, fmt.Errorf("%w: %s holds %T, which is not the provider's model", schemaext.ErrInvalidValue, object.Ref, object.Value)
		}
		requiredObjects, err := requirer.Requires()
		if err != nil {
			return selectext.Result{}, err
		}
		for _, required := range requiredObjects {
			if slices.Contains(keeps, string(required.Kind)) {
				if !required.Database.Empty() {
					value, ok := object.Value.(*resolvedModel)
					if !ok {
						return selectext.Result{}, selectionError("a qualified requirement has no captured database binding")
					}
					database := value.normalized.GetValue().GetOwner().GetDatabase()
					if database == "" || value.normalized.GetNormalizedOwner().GetDatabase() != database ||
						required.Database.Source != database || required.Database.Normalized != database {
						return selectext.Result{}, selectionError("a kept requirement belongs to a database outside its object's binding")
					}
					required.Database = objectidentity.Part{}
				}
				result.Requires = append(result.Requires, required)
			}
		}
	}
	return result, nil
}

// scopeSelectionIdentity is one scope selection in an engine's contract
// digest: how a scope names the kind decides what a scoped plan holds.
type scopeSelectionIdentity struct {
	Kind            string   `json:"kind"`
	SelectorType    string   `json:"selector_type"`
	Naming          string   `json:"naming"`
	Relation        bool     `json:"relation,omitempty"`
	GrantObjectType string   `json:"grant_object_type,omitempty"`
	KeepsRequired   []string `json:"keeps_required,omitempty"`
}

// scopeSelectionIdentities is the digest projection of selections, in kind
// order, each keeps_required list sorted: neither order is semantic.
func scopeSelectionIdentities(selections []*protocol.ScopeSelection) []scopeSelectionIdentity {
	var result []scopeSelectionIdentity
	for _, selection := range selections {
		result = append(result, scopeSelectionIdentity{Kind: selection.GetKind(), SelectorType: selection.GetSelectorType(),
			Naming: string(selectionNamings[selection.GetNaming()]), Relation: selection.GetRelation(),
			GrantObjectType: selection.GetGrantObjectType(), KeepsRequired: sortedCopy(selection.GetKeepsRequired())})
	}
	slices.SortFunc(result, func(a, b scopeSelectionIdentity) int { return strings.Compare(a.Kind, b.Kind) })
	return result
}
