// Package featurejson writes and reads JSON documents that carry feature data
// owned by schema extension providers: change records, facets, feature objects
// and source coverage. Those values refuse default JSON encoding on purpose,
// because only the codec registry of the runtime that produced them knows how
// to spell them. Everything else in a document is encoded exactly as
// encoding/json encodes it, with the same field names, order and options.
//
// The package builds a parallel "wire" type for a document's Go type in which
// each feature value is replaced by its explicit form:
//
//   - [schemaext.ChangeRecord] by [schemaext.EncodedChange];
//   - [schemaext.Facets] by a list of [schemaext.EncodedFacet];
//   - [schemaext.Objects] by a list of [schemaext.EncodedObject];
//   - [schemaext.Coverage] by a [schemaext.CoverageDocument].
//
// A type that holds no feature value is used as it is, so a document without
// feature data encodes byte for byte as json.Marshal encodes it.
package featurejson

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"ptah.run/core/schemaext"
)

// ErrUnsupportedShape reports a document type the wire projection cannot
// mirror: a struct that embeds another type on the path to a feature value.
var ErrUnsupportedShape = errors.New("featurejson: unsupported document shape")

// Marshal returns the JSON encoding of value. Feature values are encoded
// through registry; facets, feature objects and coverage use representation,
// which names whether the document describes a desired or an observed schema.
// A feature kind without a codec in registry returns the registry's
// [schemaext.UnknownCodecError], which names the kind. Errors and cancellation
// return no partial document.
func Marshal(ctx context.Context, registry schemaext.Registry, representation schemaext.Representation, value any) ([]byte, error) {
	wire, err := project(ctx, registry, representation, value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(wire)
}

// MarshalIndent is [Marshal] with the layout of json.MarshalIndent.
func MarshalIndent(ctx context.Context, registry schemaext.Registry, representation schemaext.Representation, value any, prefix, indent string) ([]byte, error) {
	wire, err := project(ctx, registry, representation, value)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(wire, prefix, indent)
}

// Unmarshal decodes data into the value target points to, reading feature
// values back through registry with the same representation the document was
// written with. It replaces the target's value rather than merging into it.
// An envelope whose kind, owner, version or definition the registry does not
// hold is refused with the registry's error, and the target is left
// unchanged. Fields whose types cannot be decoded by encoding/json, such as
// the name-only lists of a schema diff, must be left out of target's type.
func Unmarshal(ctx context.Context, registry schemaext.Registry, representation schemaext.Representation, data []byte, target any) error {
	if ctx == nil {
		return fmt.Errorf("%w: featurejson requires a context", schemaext.ErrInvalidValue)
	}
	pointer := reflect.ValueOf(target)
	if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
		return fmt.Errorf("%w: featurejson.Unmarshal requires a non-nil pointer, got %T", schemaext.ErrInvalidValue, target)
	}
	n, err := newBuilder().build(pointer.Elem().Type())
	if err != nil {
		return err
	}
	if n.kind == identity {
		return json.Unmarshal(data, target)
	}
	wire := reflect.New(n.wire)
	if err := json.Unmarshal(data, wire.Interface()); err != nil {
		return err
	}
	c := converter{ctx: ctx, registry: registry, representation: representation}
	decoded, err := c.decode(n, wire.Elem())
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pointer.Elem().Set(decoded)
	return nil
}

func project(ctx context.Context, registry schemaext.Registry, representation schemaext.Representation, value any) (any, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: featurejson requires a context", schemaext.ErrInvalidValue)
	}
	if value == nil {
		return nil, nil
	}
	source := reflect.ValueOf(value)
	n, err := newBuilder().build(source.Type())
	if err != nil {
		return nil, err
	}
	if n.kind == identity {
		return value, nil
	}
	c := converter{ctx: ctx, registry: registry, representation: representation}
	wire, err := c.encode(n, source)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return wire.Interface(), nil
}

type nodeKind int

const (
	identity nodeKind = iota
	change
	facets
	objects
	coverage
	pointer
	slice
	array
	mapping
	structure
)

var (
	changeRecordType = reflect.TypeFor[schemaext.ChangeRecord]()
	facetsType       = reflect.TypeFor[schemaext.Facets]()
	objectsType      = reflect.TypeFor[schemaext.Objects]()
	coverageType     = reflect.TypeFor[schemaext.Coverage]()

	encodedChangeType   = reflect.TypeFor[schemaext.EncodedChange]()
	encodedFacetsType   = reflect.TypeFor[[]schemaext.EncodedFacet]()
	encodedObjectsType  = reflect.TypeFor[[]schemaext.EncodedObject]()
	coverageDocumentPtr = reflect.TypeFor[*schemaext.CoverageDocument]()

	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
	zeroCheckerType     = reflect.TypeFor[interface{ IsZero() bool }]()
)

// node describes how one Go type maps onto its wire type.
type node struct {
	kind     nodeKind
	original reflect.Type
	wire     reflect.Type
	elem     *node
	fields   []field
}

// field is one struct field of a projected struct.
type field struct {
	index    int
	node     *node
	omitZero bool
}

type builder struct {
	nodes    map[reflect.Type]*node
	building map[reflect.Type]bool
}

func newBuilder() *builder {
	return &builder{nodes: make(map[reflect.Type]*node), building: make(map[reflect.Type]bool)}
}

// build returns the node of t. A type that encodes itself keeps doing so, and
// a recursive type is used as it is: the projection cannot build a recursive
// wire type, and the documents it serves have none on a path to feature data.
func (b *builder) build(t reflect.Type) (*node, error) {
	if n, found := b.nodes[t]; found {
		return n, nil
	}
	if b.building[t] || encodesItself(t) {
		return &node{kind: identity, original: t, wire: t}, nil
	}
	n := envelopeNode(t)
	if n == nil {
		b.building[t] = true
		var err error
		n, err = b.compose(t)
		delete(b.building, t)
		if err != nil {
			return nil, err
		}
	}
	n.original = t
	b.nodes[t] = n
	return n, nil
}

func envelopeNode(t reflect.Type) *node {
	switch t {
	case changeRecordType:
		return &node{kind: change, wire: encodedChangeType}
	case facetsType:
		return &node{kind: facets, wire: encodedFacetsType}
	case objectsType:
		return &node{kind: objects, wire: encodedObjectsType}
	case coverageType:
		return &node{kind: coverage, wire: coverageDocumentPtr}
	}
	return nil
}

// encodesItself reports a type that encoding/json hands to the type's own
// methods. The feature values refuse default encoding through exactly those
// methods, so they are matched before this check.
func encodesItself(t reflect.Type) bool {
	if envelopeNode(t) != nil {
		return false
	}
	pointerType := reflect.PointerTo(t)
	for _, method := range []reflect.Type{jsonMarshalerType, jsonUnmarshalerType, textMarshalerType, textUnmarshalerType} {
		if t.Implements(method) || pointerType.Implements(method) {
			return true
		}
	}
	return false
}

func (b *builder) compose(t reflect.Type) (*node, error) {
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		elem, err := b.build(t.Elem())
		if err != nil {
			return nil, err
		}
		if elem.kind == identity {
			return &node{kind: identity, wire: t}, nil
		}
		switch t.Kind() {
		case reflect.Pointer:
			return &node{kind: pointer, wire: reflect.PointerTo(elem.wire), elem: elem}, nil
		case reflect.Slice:
			return &node{kind: slice, wire: reflect.SliceOf(elem.wire), elem: elem}, nil
		case reflect.Array:
			return &node{kind: array, wire: reflect.ArrayOf(t.Len(), elem.wire), elem: elem}, nil
		default:
			// A map keeps its key type, so a key encoded as text stays so.
			return &node{kind: mapping, wire: reflect.MapOf(t.Key(), elem.wire), elem: elem}, nil
		}
	case reflect.Struct:
		return b.composeStruct(t)
	default:
		return &node{kind: identity, wire: t}, nil
	}
}

// composeStruct mirrors the JSON-visible fields of t in declaration order with
// their tags unchanged. Unexported fields and fields tagged "-" are left out:
// encoding/json writes neither.
func (b *builder) composeStruct(t reflect.Type) (*node, error) {
	var fields []field
	var wireFields []reflect.StructField
	var embedded []reflect.Type
	projected := false
	for i := range t.NumField() {
		declared := t.Field(i)
		tag := declared.Tag.Get("json")
		if tag == "-" || (!declared.IsExported() && !declared.Anonymous) {
			continue
		}
		n, err := b.build(declared.Type)
		if err != nil {
			return nil, err
		}
		projected = projected || n.kind != identity
		if declared.Anonymous {
			embedded = append(embedded, declared.Type)
			continue
		}
		fields = append(fields, field{index: i, node: n, omitZero: hasOption(tag, "omitzero")})
		wireFields = append(wireFields, reflect.StructField{Name: declared.Name, Type: n.wire, Tag: declared.Tag})
	}
	if !projected {
		return &node{kind: identity, wire: t}, nil
	}
	// encoding/json promotes the fields of an embedded struct, and a wire type
	// built here cannot reproduce that promotion faithfully.
	if len(embedded) > 0 {
		return nil, fmt.Errorf("%w: %s embeds %s beside feature data", ErrUnsupportedShape, t, embedded[0])
	}
	return &node{kind: structure, wire: reflect.StructOf(wireFields), fields: fields}, nil
}

func hasOption(tag, option string) bool {
	_, options, _ := strings.Cut(tag, ",")
	for candidate := range strings.SplitSeq(options, ",") {
		if candidate == option {
			return true
		}
	}
	return false
}

type converter struct {
	ctx            context.Context
	registry       schemaext.Registry
	representation schemaext.Representation
}

// encode converts an original value into its wire value. Nil pointers, slices
// and maps stay nil, so null and omitempty read as encoding/json writes them.
func (c converter) encode(n *node, source reflect.Value) (reflect.Value, error) {
	if err := c.ctx.Err(); err != nil {
		return reflect.Value{}, err
	}
	if n.kind == identity {
		return source, nil
	}
	target := reflect.New(n.wire).Elem()
	switch n.kind {
	case change:
		record, _ := reflect.TypeAssert[schemaext.ChangeRecord](source)
		encoded, err := c.registry.EncodeChanges(c.ctx, []schemaext.ChangeRecord{record})
		if err != nil {
			return reflect.Value{}, err
		}
		target.Set(reflect.ValueOf(encoded[0]))
	case facets:
		value, _ := reflect.TypeAssert[schemaext.Facets](source)
		if value.IsZero() {
			return target, nil
		}
		encoded, err := c.registry.EncodeFacets(c.ctx, c.representation, value)
		if err != nil {
			return reflect.Value{}, err
		}
		target.Set(reflect.ValueOf(encoded))
	case objects:
		value, _ := reflect.TypeAssert[schemaext.Objects](source)
		if value.IsZero() {
			return target, nil
		}
		encoded, err := c.registry.EncodeObjects(c.ctx, c.representation, value)
		if err != nil {
			return reflect.Value{}, err
		}
		target.Set(reflect.ValueOf(encoded))
	case coverage:
		value, _ := reflect.TypeAssert[schemaext.Coverage](source)
		if value.IsZero() {
			return target, nil
		}
		encoded, err := c.registry.EncodeCoverage(c.ctx, c.representation, value)
		if err != nil {
			return reflect.Value{}, err
		}
		target.Set(reflect.ValueOf(&encoded))
	default:
		return c.encodeComposite(n, source, target)
	}
	return target, nil
}

func (c converter) encodeComposite(n *node, source, target reflect.Value) (reflect.Value, error) {
	switch n.kind {
	case pointer:
		return c.encodePointer(n, source, target)
	case slice, array:
		return c.encodeSequence(n, source, target)
	case mapping:
		return c.encodeMap(n, source, target)
	default:
		return c.encodeStruct(n, source, target)
	}
}

func (c converter) encodePointer(n *node, source, target reflect.Value) (reflect.Value, error) {
	if source.IsNil() {
		return target, nil
	}
	elem, err := c.encode(n.elem, source.Elem())
	if err != nil {
		return reflect.Value{}, err
	}
	target.Set(reflect.New(n.elem.wire))
	target.Elem().Set(elem)
	return target, nil
}

func (c converter) encodeSequence(n *node, source, target reflect.Value) (reflect.Value, error) {
	if n.kind == slice && source.IsNil() {
		return target, nil
	}
	if n.kind == slice {
		target.Set(reflect.MakeSlice(n.wire, source.Len(), source.Len()))
	}
	for i := range source.Len() {
		elem, err := c.encode(n.elem, source.Index(i))
		if err != nil {
			return reflect.Value{}, err
		}
		target.Index(i).Set(elem)
	}
	return target, nil
}

func (c converter) encodeMap(n *node, source, target reflect.Value) (reflect.Value, error) {
	if source.IsNil() {
		return target, nil
	}
	target.Set(reflect.MakeMapWithSize(n.wire, source.Len()))
	for iter := source.MapRange(); iter.Next(); {
		elem, err := c.encode(n.elem, iter.Value())
		if err != nil {
			return reflect.Value{}, err
		}
		target.SetMapIndex(iter.Key(), elem)
	}
	return target, nil
}

func (c converter) encodeStruct(n *node, source, target reflect.Value) (reflect.Value, error) {
	for position, f := range n.fields {
		value := source.Field(f.index)
		if f.omitZero && reportsZero(value) {
			continue
		}
		wire, err := c.encode(f.node, value)
		if err != nil {
			return reflect.Value{}, err
		}
		target.Field(position).Set(wire)
	}
	return target, nil
}

// reportsZero is what omitzero asks of an original field whose wire type no
// longer has the field type's IsZero method.
func reportsZero(value reflect.Value) bool {
	if value.Type().Implements(zeroCheckerType) {
		if value.Kind() == reflect.Pointer && value.IsNil() {
			return true
		}
		checker, _ := reflect.TypeAssert[interface{ IsZero() bool }](value)
		return checker.IsZero()
	}
	return value.IsZero()
}

// decode converts a wire value back into the original type.
func (c converter) decode(n *node, source reflect.Value) (reflect.Value, error) {
	if err := c.ctx.Err(); err != nil {
		return reflect.Value{}, err
	}
	switch n.kind {
	case identity:
		return source, nil
	case change:
		encoded, _ := reflect.TypeAssert[schemaext.EncodedChange](source)
		decoded, err := c.registry.DecodeChanges(c.ctx, []schemaext.EncodedChange{encoded})
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(decoded[0]), nil
	case facets:
		encoded, _ := reflect.TypeAssert[[]schemaext.EncodedFacet](source)
		if len(encoded) == 0 {
			return reflect.ValueOf(schemaext.Facets{}), nil
		}
		decoded, err := c.registry.DecodeFacets(c.ctx, c.representation, encoded)
		return reflect.ValueOf(decoded), err
	case objects:
		encoded, _ := reflect.TypeAssert[[]schemaext.EncodedObject](source)
		if len(encoded) == 0 {
			return reflect.ValueOf(schemaext.Objects{}), nil
		}
		decoded, err := c.registry.DecodeObjects(c.ctx, c.representation, encoded)
		return reflect.ValueOf(decoded), err
	case coverage:
		encoded, _ := reflect.TypeAssert[*schemaext.CoverageDocument](source)
		if encoded == nil {
			return reflect.ValueOf(schemaext.Coverage{}), nil
		}
		decoded, err := c.registry.DecodeCoverage(c.ctx, *encoded)
		return reflect.ValueOf(decoded), err
	default:
		return c.decodeComposite(n, source)
	}
}

func (c converter) decodeComposite(n *node, source reflect.Value) (reflect.Value, error) {
	original := n.original
	target := reflect.New(original).Elem()
	switch n.kind {
	case pointer:
		if source.IsNil() {
			return target, nil
		}
		elem, err := c.decode(n.elem, source.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		target.Set(reflect.New(original.Elem()))
		target.Elem().Set(elem)
	case slice, array:
		if n.kind == slice && source.IsNil() {
			return target, nil
		}
		if n.kind == slice {
			target.Set(reflect.MakeSlice(original, source.Len(), source.Len()))
		}
		for i := range source.Len() {
			elem, err := c.decode(n.elem, source.Index(i))
			if err != nil {
				return reflect.Value{}, err
			}
			target.Index(i).Set(elem)
		}
	case mapping:
		if source.IsNil() {
			return target, nil
		}
		target.Set(reflect.MakeMapWithSize(original, source.Len()))
		for iter := source.MapRange(); iter.Next(); {
			elem, err := c.decode(n.elem, iter.Value())
			if err != nil {
				return reflect.Value{}, err
			}
			target.SetMapIndex(iter.Key(), elem)
		}
	case structure:
		for position, f := range n.fields {
			value, err := c.decode(f.node, source.Field(position))
			if err != nil {
				return reflect.Value{}, err
			}
			target.Field(f.index).Set(value)
		}
	}
	return target, nil
}
