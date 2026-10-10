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
//   - [schemaext.Object] by one [schemaext.EncodedObject], or null when it is
//     the zero value;
//   - [schemaext.Coverage] by a [schemaext.CoverageDocument].
//
// A type that holds no feature value is used as it is, so a document without
// feature data encodes byte for byte as json.Marshal encodes it. The wire type
// of a type is built once per process and shared by every call.
//
// Some shapes have no faithful wire type, and a document type with one on a
// path to feature data is refused with [ErrUnsupportedShape] before anything is
// encoded: a struct that embeds another one whose fields encoding/json would
// promote, an unexported field encoding/json would read, a recursive type, and
// the feature types with no wire form, [schemaext.RelationValue] and
// [schemaext.RelationSnapshot]. Feature data held in an interface value is not
// projected; encoding/json reaches it and its value refuses to be encoded.
package featurejson

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"

	"ptah.run/core/schemaext"
)

// ErrUnsupportedShape reports a document type the wire projection cannot
// mirror on a path to a feature value; the package documentation lists the
// shapes.
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
// written with. It replaces the target's value rather than merging into it,
// and on any error it leaves the target unchanged.
//
// Feature data is read strictly. An envelope with a field it does not define,
// or with a key given twice, is refused as [schemaext.Registry.Unmarshal]
// refuses it; facets, feature objects and coverage recorded under another
// representation are refused; and an envelope whose kind, owner, version or
// definition the registry does not hold is refused with the registry's error.
// The rest of the document is decoded as encoding/json decodes it. Fields whose
// types cannot be decoded by encoding/json, such as the name-only lists of a
// schema diff, must be left out of target's type.
func Unmarshal(ctx context.Context, registry schemaext.Registry, representation schemaext.Representation, data []byte, target any) error {
	if ctx == nil {
		return fmt.Errorf("%w: featurejson requires a context", schemaext.ErrInvalidValue)
	}
	pointer := reflect.ValueOf(target)
	if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
		return fmt.Errorf("%w: featurejson.Unmarshal requires a non-nil pointer, got %T", schemaext.ErrInvalidValue, target)
	}
	n, err := nodeOf(pointer.Elem().Type())
	if err != nil {
		return err
	}
	// A fresh wire value, even for a type without feature data, is what keeps
	// the target unchanged when decoding fails partway.
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
	n, err := nodeOf(source.Type())
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

// The wire forms of the envelopes. Each is the schemaext type under another
// name, so it encodes field for field as that type does, and it decodes
// strictly through [schemaext.DecodeJSON].
type (
	wireChange   schemaext.EncodedChange
	wireFacet    schemaext.EncodedFacet
	wireObject   schemaext.EncodedObject
	wireCoverage schemaext.CoverageDocument
)

// UnmarshalJSON decodes the change envelope strictly.
func (w *wireChange) UnmarshalJSON(data []byte) error {
	return decodeStrictly(data, (*schemaext.EncodedChange)(w))
}

// UnmarshalJSON decodes the facet envelope strictly.
func (w *wireFacet) UnmarshalJSON(data []byte) error {
	return decodeStrictly(data, (*schemaext.EncodedFacet)(w))
}

// UnmarshalJSON decodes the object envelope strictly.
func (w *wireObject) UnmarshalJSON(data []byte) error {
	return decodeStrictly(data, (*schemaext.EncodedObject)(w))
}

// UnmarshalJSON decodes the coverage document strictly.
func (w *wireCoverage) UnmarshalJSON(data []byte) error {
	return decodeStrictly(data, (*schemaext.CoverageDocument)(w))
}

func decodeStrictly[T any](data []byte, target *T) error {
	value, err := schemaext.DecodeJSON[T](data)
	if err != nil {
		return err
	}
	*target = value
	return nil
}

type nodeKind int

const (
	identity nodeKind = iota
	change
	facets
	objects
	object
	coverage
	pointer
	slice
	array
	mapping
	structure
)

var (
	changeRecordType     = reflect.TypeFor[schemaext.ChangeRecord]()
	facetsType           = reflect.TypeFor[schemaext.Facets]()
	objectsType          = reflect.TypeFor[schemaext.Objects]()
	objectType           = reflect.TypeFor[schemaext.Object]()
	coverageType         = reflect.TypeFor[schemaext.Coverage]()
	relationValueType    = reflect.TypeFor[schemaext.RelationValue]()
	relationSnapshotType = reflect.TypeFor[schemaext.RelationSnapshot]()

	wireChangeType  = reflect.TypeFor[wireChange]()
	wireFacetsType  = reflect.TypeFor[[]wireFacet]()
	wireObjectsType = reflect.TypeFor[[]wireObject]()
	wireObjectPtr   = reflect.TypeFor[*wireObject]()
	wireCoveragePtr = reflect.TypeFor[*wireCoverage]()

	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
	zeroCheckerType     = reflect.TypeFor[interface{ IsZero() bool }]()
)

// node describes how one Go type maps onto its wire type. A node is immutable
// once built.
type node struct {
	kind     nodeKind
	original reflect.Type
	wire     reflect.Type
	elem     *node
	fields   []field
}

// field is one struct field of a projected struct.
type field struct {
	index int
	node  *node
	// present marks a field holding feature data that carries omitzero. Its
	// wire field is a pointer that is nil exactly when the field is left out,
	// so the decision follows the original type's zero rules rather than the
	// wire type's.
	present   bool
	omitEmpty bool
}

// built caches the node of every type a call has built. Nodes are immutable,
// so concurrent calls share them; a type that cannot be built is not cached
// and fails the same way on every call.
var built sync.Map

func nodeOf(t reflect.Type) (*node, error) {
	if cached, found := built.Load(t); found {
		n, _ := cached.(*node)
		return n, nil
	}
	b := newBuilder()
	n, err := b.build(t)
	if err != nil {
		return nil, err
	}
	for nodeType, value := range b.nodes {
		built.LoadOrStore(nodeType, value)
	}
	return n, nil
}

type builder struct {
	nodes    map[reflect.Type]*node
	building map[reflect.Type]bool
	holds    map[reflect.Type]bool
}

func newBuilder() *builder {
	return &builder{
		nodes:    make(map[reflect.Type]*node),
		building: make(map[reflect.Type]bool),
		holds:    make(map[reflect.Type]bool),
	}
}

// build returns the node of t. A type that holds no feature data, including
// one that encodes itself, is used as it is. A type that does is projected,
// and one that reaches itself on the way is refused: its wire type would have
// to be recursive, and reflect cannot build one.
func (b *builder) build(t reflect.Type) (*node, error) {
	if n, found := b.nodes[t]; found {
		return n, nil
	}
	if cached, found := built.Load(t); found {
		n, _ := cached.(*node)
		return n, nil
	}
	if !b.holdsFeatures(t) {
		n := &node{kind: identity, original: t, wire: t}
		b.nodes[t] = n
		return n, nil
	}
	if t == relationValueType || t == relationSnapshotType {
		return nil, fmt.Errorf("%w: %s has no JSON form", ErrUnsupportedShape, t)
	}
	if b.building[t] {
		return nil, fmt.Errorf("%w: %s holds feature data through a recursive type", ErrUnsupportedShape, t)
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

// holdsFeatures reports whether encoding/json, encoding a value of t, would
// reach a feature value. A type that encodes itself is not looked into.
//
// A cycle makes a partial answer unreliable: a type explored before the cycle
// closes may reach feature data through a type still being explored. So an
// answer is kept only where it is certain: true for every type on the path
// that found feature data, and false for every type visited by a search that
// found none.
func (b *builder) holdsFeatures(root reflect.Type) bool {
	if known, found := b.holds[root]; found {
		return known
	}
	visited := make(map[reflect.Type]bool)
	var reaches func(t reflect.Type) bool
	reaches = func(t reflect.Type) bool {
		if known, found := b.holds[t]; found {
			return known
		}
		if visited[t] {
			return false
		}
		visited[t] = true
		if featureType(t) || (!encodesItself(t) && anyChild(t, reaches)) {
			b.holds[t] = true
			return true
		}
		return false
	}
	if reaches(root) {
		return true
	}
	for t := range visited {
		b.holds[t] = false
	}
	return false
}

// anyChild reports whether visit returns true for a type encoding/json
// encodes as part of t: the element of a pointer, slice, array or map, or the
// type of a field a struct encodes.
func anyChild(t reflect.Type, visit func(reflect.Type) bool) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		return visit(t.Elem())
	case reflect.Struct:
		for declared := range t.Fields() {
			if jsonVisible(declared) && visit(declared.Type) {
				return true
			}
		}
	default:
	}
	return false
}

// featureType reports a type whose JSON form only this package can give, or
// that has none.
func featureType(t reflect.Type) bool {
	return envelopeNode(t) != nil || t == relationValueType || t == relationSnapshotType
}

func envelopeNode(t reflect.Type) *node {
	switch t {
	case changeRecordType:
		return &node{kind: change, wire: wireChangeType}
	case facetsType:
		return &node{kind: facets, wire: wireFacetsType}
	case objectsType:
		return &node{kind: objects, wire: wireObjectsType}
	case objectType:
		return &node{kind: object, wire: wireObjectPtr}
	case coverageType:
		return &node{kind: coverage, wire: wireCoveragePtr}
	default:
		return nil
	}
}

// encodesItself reports a type that encoding/json hands to the type's own
// methods. The feature values refuse default encoding through exactly those
// methods, and a pointer to one has them too, so neither counts.
func encodesItself(t reflect.Type) bool {
	if featureType(t) || (t.Kind() == reflect.Pointer && featureType(t.Elem())) {
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
	default:
		// Only a container or a struct can hold feature data, and
		// holdsFeatures has established that t does.
		return b.composeStruct(t)
	}
}

// composeStruct mirrors the fields encoding/json encodes, in declaration
// order, with their tags unchanged. A field it leaves out, unexported or
// tagged "-", is left out of the mirror too.
func (b *builder) composeStruct(t reflect.Type) (*node, error) {
	fields := make([]field, 0, t.NumField())
	wireFields := make([]reflect.StructField, 0, t.NumField())
	for i := range t.NumField() {
		declared := t.Field(i)
		if !jsonVisible(declared) {
			continue
		}
		// encoding/json promotes the fields of an embedded struct, and a wire
		// type built here cannot reproduce that promotion faithfully; reflect
		// cannot mirror an unexported field at all.
		if promotes(declared) {
			return nil, fmt.Errorf("%w: %s embeds %s beside feature data", ErrUnsupportedShape, t, declared.Type)
		}
		if !declared.IsExported() {
			return nil, fmt.Errorf("%w: %s has the unexported field %s beside feature data", ErrUnsupportedShape, t, declared.Name)
		}
		n, err := b.build(declared.Type)
		if err != nil {
			return nil, err
		}
		tag := declared.Tag.Get("json")
		f := field{index: i, node: n}
		wireType := n.wire
		// A field without feature data has its own type in the mirror, so
		// encoding/json applies the field's options to it as it would to the
		// original.
		if n.kind != identity && hasOption(tag, "omitzero") {
			f.present, f.omitEmpty = true, hasOption(tag, "omitempty")
			wireType = reflect.PointerTo(n.wire)
		}
		fields = append(fields, f)
		wireFields = append(wireFields, reflect.StructField{Name: declared.Name, Type: wireType, Tag: declared.Tag})
	}
	return &node{kind: structure, wire: reflect.StructOf(wireFields), fields: fields}, nil
}

// jsonVisible reports a struct field encoding/json reads: an exported field, or an
// embedded struct whose exported fields it promotes, unless the field is
// tagged "-".
func jsonVisible(declared reflect.StructField) bool {
	if declared.Tag.Get("json") == "-" {
		return false
	}
	if !declared.Anonymous {
		return declared.IsExported()
	}
	t := declared.Type
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return declared.IsExported() || t.Kind() == reflect.Struct
}

// promotes reports an embedded field whose fields encoding/json writes into
// the enclosing object: a struct, or an unnamed pointer to one, with no valid
// JSON name in its tag. An embedded field with a name, or of another kind, is
// an ordinary field named by its tag or its type.
func promotes(declared reflect.StructField) bool {
	if !declared.Anonymous {
		return false
	}
	name, _, _ := strings.Cut(declared.Tag.Get("json"), ",")
	if validName(name) {
		return false
	}
	t := declared.Type
	if t.Name() == "" && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct
}

// validName is encoding/json's rule for a usable name in a tag.
func validName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", r):
		case !unicode.IsLetter(r) && !unicode.IsDigit(r):
			return false
		default:
		}
	}
	return true
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
	switch n.kind {
	case identity:
		return source, nil
	case change, facets, objects, object, coverage:
		return c.encodeEnvelope(n, source)
	case pointer:
		return c.encodePointer(n, source)
	case slice, array:
		return c.encodeSequence(n, source)
	case mapping:
		return c.encodeMap(n, source)
	default:
		return c.encodeStruct(n, source)
	}
}

func (c converter) encodeEnvelope(n *node, source reflect.Value) (reflect.Value, error) {
	target := reflect.New(n.wire).Elem()
	switch n.kind {
	case change:
		record, _ := reflect.TypeAssert[schemaext.ChangeRecord](source)
		encoded, err := c.registry.EncodeChanges(c.ctx, []schemaext.ChangeRecord{record})
		if err != nil {
			return reflect.Value{}, err
		}
		target.Set(reflect.ValueOf(wireChange(encoded[0])))
	case facets:
		value, _ := reflect.TypeAssert[schemaext.Facets](source)
		if value.IsZero() {
			return target, nil
		}
		encoded, err := c.registry.EncodeFacets(c.ctx, c.representation, value)
		if err != nil {
			return reflect.Value{}, err
		}
		wire := make([]wireFacet, len(encoded))
		for i, facet := range encoded {
			wire[i] = wireFacet(facet)
		}
		target.Set(reflect.ValueOf(wire))
	case objects:
		value, _ := reflect.TypeAssert[schemaext.Objects](source)
		if value.IsZero() {
			return target, nil
		}
		encoded, err := c.registry.EncodeObjects(c.ctx, c.representation, value)
		if err != nil {
			return reflect.Value{}, err
		}
		target.Set(reflect.ValueOf(wireObjectList(encoded)))
	case object:
		if source.IsZero() {
			return target, nil
		}
		value, _ := reflect.TypeAssert[schemaext.Object](source)
		collection, err := schemaext.NewObjects(value)
		if err != nil {
			return reflect.Value{}, err
		}
		encoded, err := c.registry.EncodeObjects(c.ctx, c.representation, collection)
		if err != nil {
			return reflect.Value{}, err
		}
		target.Set(reflect.ValueOf(&wireObjectList(encoded)[0]))
	default:
		value, _ := reflect.TypeAssert[schemaext.Coverage](source)
		if value.IsZero() {
			return target, nil
		}
		encoded, err := c.registry.EncodeCoverage(c.ctx, c.representation, value)
		if err != nil {
			return reflect.Value{}, err
		}
		target.Set(reflect.ValueOf(new(wireCoverage(encoded))))
	}
	return target, nil
}

func wireObjectList(encoded []schemaext.EncodedObject) []wireObject {
	wire := make([]wireObject, len(encoded))
	for i, value := range encoded {
		wire[i] = wireObject(value)
	}
	return wire
}

func (c converter) encodePointer(n *node, source reflect.Value) (reflect.Value, error) {
	target := reflect.New(n.wire).Elem()
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

func (c converter) encodeSequence(n *node, source reflect.Value) (reflect.Value, error) {
	target := reflect.New(n.wire).Elem()
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

func (c converter) encodeMap(n *node, source reflect.Value) (reflect.Value, error) {
	target := reflect.New(n.wire).Elem()
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

func (c converter) encodeStruct(n *node, source reflect.Value) (reflect.Value, error) {
	target := reflect.New(n.wire).Elem()
	for position, f := range n.fields {
		value := source.Field(f.index)
		if f.present && reportsZero(value) {
			continue
		}
		wire, err := c.encode(f.node, value)
		if err != nil {
			return reflect.Value{}, err
		}
		if !f.present {
			target.Field(position).Set(wire)
			continue
		}
		// omitempty reads the wire value: for a container its length is the
		// original's, and an empty feature list is left out as it always was.
		if f.omitEmpty && emptyValue(wire) {
			continue
		}
		holder := reflect.New(f.node.wire)
		holder.Elem().Set(wire)
		target.Field(position).Set(holder)
	}
	return target, nil
}

// reportsZero is encoding/json's omitzero rule for a field of value's type: an
// IsZero method decides when the type or a pointer to it has one, and a nil
// interface or pointer is zero without asking it.
func reportsZero(value reflect.Value) bool {
	t := value.Type()
	switch {
	case t.Kind() == reflect.Interface && t.Implements(zeroCheckerType):
		return value.IsNil() || (value.Elem().Kind() == reflect.Pointer && value.Elem().IsNil()) || callIsZero(value)
	case t.Kind() == reflect.Pointer && t.Implements(zeroCheckerType):
		return value.IsNil() || callIsZero(value)
	case t.Implements(zeroCheckerType):
		return callIsZero(value)
	case reflect.PointerTo(t).Implements(zeroCheckerType):
		if !value.CanAddr() {
			boxed := reflect.New(t).Elem()
			boxed.Set(value)
			value = boxed
		}
		return callIsZero(value.Addr())
	default:
		return value.IsZero()
	}
}

func callIsZero(value reflect.Value) bool {
	checker, _ := reflect.TypeAssert[interface{ IsZero() bool }](value)
	return checker.IsZero()
}

// emptyValue is encoding/json's omitempty rule.
func emptyValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return value.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return value.IsZero()
	default:
		return false
	}
}

// decode converts a wire value back into the original type.
func (c converter) decode(n *node, source reflect.Value) (reflect.Value, error) {
	if err := c.ctx.Err(); err != nil {
		return reflect.Value{}, err
	}
	switch n.kind {
	case identity:
		return source, nil
	case change, facets, objects, object, coverage:
		return c.decodeEnvelope(n, source)
	default:
		return c.decodeComposite(n, source)
	}
}

func (c converter) decodeEnvelope(n *node, source reflect.Value) (reflect.Value, error) {
	switch n.kind {
	case change:
		encoded, _ := reflect.TypeAssert[wireChange](source)
		decoded, err := c.registry.DecodeChanges(c.ctx, []schemaext.EncodedChange{schemaext.EncodedChange(encoded)})
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(decoded[0]), nil
	case facets:
		wire, _ := reflect.TypeAssert[[]wireFacet](source)
		if len(wire) == 0 {
			return reflect.ValueOf(schemaext.Facets{}), nil
		}
		encoded := make([]schemaext.EncodedFacet, len(wire))
		for i, facet := range wire {
			encoded[i] = schemaext.EncodedFacet(facet)
		}
		decoded, err := c.registry.DecodeFacets(c.ctx, c.representation, encoded)
		return reflect.ValueOf(decoded), err
	case objects:
		wire, _ := reflect.TypeAssert[[]wireObject](source)
		if len(wire) == 0 {
			return reflect.ValueOf(schemaext.Objects{}), nil
		}
		decoded, err := c.registry.DecodeObjects(c.ctx, c.representation, encodedObjects(wire))
		return reflect.ValueOf(decoded), err
	case object:
		wire, _ := reflect.TypeAssert[*wireObject](source)
		if wire == nil {
			return reflect.ValueOf(schemaext.Object{}), nil
		}
		decoded, err := c.registry.DecodeObjects(c.ctx, c.representation, encodedObjects([]wireObject{*wire}))
		if err != nil {
			return reflect.Value{}, err
		}
		values, err := decoded.All()
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(values[0]), nil
	default:
		wire, _ := reflect.TypeAssert[*wireCoverage](source)
		if wire == nil {
			return reflect.ValueOf(schemaext.Coverage{}), nil
		}
		// The registry restores a coverage document under the representation
		// it records, so the document's own claim is checked here, as the
		// facets and objects decoders check theirs.
		if wire.Representation != c.representation {
			return reflect.Value{}, fmt.Errorf("%w: coverage is recorded as %q, and the document is read as %q",
				schemaext.ErrInvalidValue, wire.Representation, c.representation)
		}
		decoded, err := c.registry.DecodeCoverage(c.ctx, schemaext.CoverageDocument(*wire))
		return reflect.ValueOf(decoded), err
	}
}

func encodedObjects(wire []wireObject) []schemaext.EncodedObject {
	encoded := make([]schemaext.EncodedObject, len(wire))
	for i, value := range wire {
		encoded[i] = schemaext.EncodedObject(value)
	}
	return encoded
}

func (c converter) decodeComposite(n *node, source reflect.Value) (reflect.Value, error) {
	switch n.kind {
	case pointer:
		return c.decodePointer(n, source)
	case slice, array:
		return c.decodeSequence(n, source)
	case mapping:
		return c.decodeMap(n, source)
	default:
		return c.decodeStruct(n, source)
	}
}

func (c converter) decodePointer(n *node, source reflect.Value) (reflect.Value, error) {
	target := reflect.New(n.original).Elem()
	if source.IsNil() {
		return target, nil
	}
	elem, err := c.decode(n.elem, source.Elem())
	if err != nil {
		return reflect.Value{}, err
	}
	target.Set(reflect.New(n.original.Elem()))
	target.Elem().Set(elem)
	return target, nil
}

func (c converter) decodeSequence(n *node, source reflect.Value) (reflect.Value, error) {
	target := reflect.New(n.original).Elem()
	if n.kind == slice && source.IsNil() {
		return target, nil
	}
	if n.kind == slice {
		target.Set(reflect.MakeSlice(n.original, source.Len(), source.Len()))
	}
	for i := range source.Len() {
		elem, err := c.decode(n.elem, source.Index(i))
		if err != nil {
			return reflect.Value{}, err
		}
		target.Index(i).Set(elem)
	}
	return target, nil
}

func (c converter) decodeMap(n *node, source reflect.Value) (reflect.Value, error) {
	target := reflect.New(n.original).Elem()
	if source.IsNil() {
		return target, nil
	}
	target.Set(reflect.MakeMapWithSize(n.original, source.Len()))
	for iter := source.MapRange(); iter.Next(); {
		elem, err := c.decode(n.elem, iter.Value())
		if err != nil {
			return reflect.Value{}, err
		}
		target.SetMapIndex(iter.Key(), elem)
	}
	return target, nil
}

func (c converter) decodeStruct(n *node, source reflect.Value) (reflect.Value, error) {
	target := reflect.New(n.original).Elem()
	for position, f := range n.fields {
		wire := source.Field(position)
		if f.present {
			if wire.IsNil() {
				continue
			}
			wire = wire.Elem()
		}
		value, err := c.decode(f.node, wire)
		if err != nil {
			return reflect.Value{}, err
		}
		target.Field(f.index).Set(value)
	}
	return target, nil
}
