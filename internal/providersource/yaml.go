// Package providersource converts captured frontend data to the provider source
// contract. It does not interpret engine syntax or call a provider process.
package providersource

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/yamlext"
)

// CheckYAMLKeys refuses a YAML key claim the host cannot read. A key without a
// dot names a key of the document or of a table entry. A key with a dot claims
// a group of the table entry's platform key, so it has table scope, is
// "platform." and the name of an engine of the same provider, as the platform
// key spells the engine, and no other dotted key is read.
func CheckYAMLKeys(engines []*protocol.Engine) error {
	for _, engine := range engines {
		for _, claim := range engine.GetYamlSourceKeys() {
			if !strings.Contains(claim.GetKey(), ".") {
				continue
			}
			group, claimed := yamlext.PlatformGroup(claim.GetKey())
			switch {
			case !claimed:
				return fmt.Errorf("YAML key %q of engine %q is dotted but is not %q and a group name", claim.GetKey(), engine.GetId(), yamlext.PlatformKeyPrefix)
			case claim.GetScope() != protocol.YAMLSourceKey_TABLE:
				return fmt.Errorf("YAML key %q of engine %q claims a platform group, which a table entry holds; its scope is %s", claim.GetKey(), engine.GetId(), claim.GetScope())
			case !slices.ContainsFunc(engines, func(owner *protocol.Engine) bool {
				return owner.GetId() == group || slices.Contains(owner.GetAliases(), group)
			}):
				return fmt.Errorf("YAML key %q of engine %q claims the platform group %q, which names no engine of the provider", claim.GetKey(), engine.GetId(), group)
			}
		}
	}
	return nil
}

// YAML captures one engine's negotiated YAML keys and all common table bindings.
// Missing keys still enroll their models so the owner must state their coverage.
// Other engines' fragments are excluded, without guessing a default engine.
// The caller validates the complete request against the negotiated description.
func YAML(ctx context.Context, engine *protocol.Engine, input yamlext.Document) (*protocol.DecodeSourceRequest, error) {
	document := &protocol.SourceDocument{Id: "document", Format: protocol.SourceDocument_YAML,
		File: cmp.Or(input.Filename, "<unnamed YAML>"), ByteLength: uint64(len(input.Source))}
	for _, model := range engine.GetValueModels() {
		if model.GetRepresentation() == protocol.ValueInput_DESIRED && slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_YAML) {
			document.Models = append(document.Models, proto.CloneOf(model))
		}
	}
	for _, table := range input.Tables {
		document.Bindings = append(document.Bindings, &protocol.SourceBinding{Id: table.Key, EntryKey: table.Key, StructName: table.Struct,
			Object: &protocol.Reference{Engine: engine.GetId(), Kind: "table", Schema: table.Schema, Name: table.Name}})
	}
	walk := yamlWalk{ctx: ctx, source: input.Source, active: make(map[*yamlext.Node]bool)}
	for i, fragment := range input.Fragments {
		claim := yamlClaim(engine, fragment)
		if claim == nil {
			continue
		}
		value, err := walk.value(fragment.Node, 0)
		if err != nil {
			return nil, fmt.Errorf("YAML %s at %v: %w", document.GetFile(), fragment.Path, err)
		}
		captured := &protocol.SourceFragment{Id: strconv.Itoa(i), YamlKey: proto.CloneOf(claim), Selector: claim.GetSelector(),
			Span: value.GetSpan(), Content: &protocol.SourceFragment_Value{Value: value}}
		if fragment.Table >= 0 {
			if fragment.Table >= len(input.Tables) {
				return nil, fmt.Errorf("YAML fragment has no table binding")
			}
			table := input.Tables[fragment.Table]
			captured.BindingId = table.Key
			if kind, entry := entrySubjects[claim.GetScope()]; entry {
				captured.Subject = &protocol.Reference{Engine: engine.GetId(), Kind: kind, Schema: table.Schema, Path: []string{table.Name}, Name: fragment.Name}
			}
		}
		if captured.InheritedAttributes, err = walk.inherited(claim, fragment); err != nil {
			return nil, fmt.Errorf("YAML %s at %v: %w", document.GetFile(), fragment.Path, err)
		}
		document.Fragments = append(document.Fragments, captured)
	}
	request := sourceRequest(engine, document)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := provider.ValidateSourceDocuments(engine.GetId(), request.GetDocuments()); err != nil {
		return nil, err
	}
	return request, nil
}

// yamlScopes is the claim scope of the key of each of the frontend's entries.
var yamlScopes = map[string]protocol.YAMLSourceKey_Scope{
	"":                  protocol.YAMLSourceKey_DOCUMENT,
	yamlext.EntryTable:  protocol.YAMLSourceKey_TABLE,
	yamlext.EntryColumn: protocol.YAMLSourceKey_COLUMN,
	yamlext.EntryIndex:  protocol.YAMLSourceKey_INDEX,
}

// entrySubjects is the reference kind of the subject of a column or an index
// key, which a table and a document key have none of.
var entrySubjects = map[protocol.YAMLSourceKey_Scope]string{
	protocol.YAMLSourceKey_COLUMN: "column",
	protocol.YAMLSourceKey_INDEX:  "index",
}

func yamlClaim(engine *protocol.Engine, fragment yamlext.Fragment) *protocol.YAMLSourceKey {
	entry, key, valid := fragment.Entry()
	scope, read := yamlScopes[entry]
	if !valid || !read {
		return nil
	}
	for _, claim := range engine.GetYamlSourceKeys() {
		if claim.GetScope() == scope && claim.GetKey() == key {
			return claim
		}
	}
	return nil
}

// inherited is the mapping of the keys of its entry a fragment carries, in the
// order its claim lists them, or nil when it carries none. A key the entry
// leaves unwritten is absent.
func (w *yamlWalk) inherited(claim *protocol.YAMLSourceKey, fragment yamlext.Fragment) (*protocol.SourceMapping, error) {
	var mapping *protocol.SourceMapping
	for _, name := range claim.GetInherited() {
		node, written := fragment.Inherited[name]
		if !written {
			continue
		}
		value, err := w.value(node, 0)
		if err != nil {
			return nil, err
		}
		if mapping == nil {
			mapping = &protocol.SourceMapping{}
		}
		mapping.Entries = append(mapping.Entries, &protocol.SourceEntry{Key: name, Value: value})
	}
	for name := range fragment.Inherited {
		if !slices.Contains(claim.GetInherited(), name) {
			return nil, fmt.Errorf("YAML fragment carries the key %q of its entry, which its claim does not inherit", name)
		}
	}
	return mapping, nil
}
