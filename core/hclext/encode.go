package hclext

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Encoder writes the objects of an extension's Kinds a document holds as
// blocks for the HCL writer. It returns data and never HCL text: the writer
// quotes every value, writes every reference and places every block, so a
// document keeps one syntax whichever owner a block of it is.
type Encoder interface {
	EncodeHCL(ctx context.Context, request EncodeRequest) (EncodeResult, error)
}

// EncodeRequest is what one document holds of an extension's Kinds.
type EncodeRequest struct {
	// Objects are the document's objects of the extension's Kinds, in the
	// order of their identities.
	Objects []schemaext.Object
	// Tables are the document's tables that hold a facet of the extension's
	// Kinds, each with those facets alone, in the document's order.
	Tables []TableFacets
	// Columns, Indexes and Keys are the column, index, and primary key and
	// constraint blocks the writer writes that hold a facet of the
	// extension's Kinds, each with those facets alone, in the order the
	// writer writes them.
	Columns []SlotFacets
	Indexes []SlotFacets
	Keys    []SlotFacets
	// Routines are function and procedure blocks carrying owner facets.
	Routines []SlotFacets
	// Coverage is what the document knows of the extension's Kinds, such
	// as a limit the format may have no spelling for.
	Coverage schemaext.Coverage
}

// SlotKind is the kind of one of the writer's own blocks an owner adds to.
type SlotKind string

const (
	// SlotTable is a table block.
	SlotTable SlotKind = "table"
	// SlotColumn is a column block of a table.
	SlotColumn SlotKind = "column"
	// SlotIndex is an index block of a table.
	SlotIndex SlotKind = "index"
	// SlotKey is a table's primary key, or a constraint of it.
	SlotKey SlotKind = "key"
	// SlotFunction is a function block.
	SlotFunction SlotKind = "function"
	// SlotProcedure is a procedure block.
	SlotProcedure SlotKind = "procedure"
)

// Slot names one of the writer's own blocks: a table, or a column, an index,
// the primary key or a constraint of one, or a routine. It is comparable, and two blocks of
// one document do not share one.
type Slot struct {
	// Kind is the kind of block.
	Kind SlotKind
	// Table is the table the block is, or is in, as the writer names it.
	Table Table
	// Name is the name of a column, an index or a constraint, and empty for a
	// table, a primary key and a constraint without a name.
	Name string
	// Parameters distinguishes overloaded routine blocks. Other kinds leave it empty.
	Parameters string
	// Primary marks a table's primary key, a [SlotKey] without a name.
	Primary bool
}

// SlotFacets is one of the writer's own blocks and facets it holds.
type SlotFacets struct {
	// Slot is the block.
	Slot Slot
	// Facets are the facets.
	Facets schemaext.Facets
}

// SlotAttributes is what an owner adds to one of the writer's own blocks it
// was handed: attributes it writes there, and losses the block cannot say.
type SlotAttributes struct {
	// Slot is the block.
	Slot Slot
	// Attributes are written into the block. The writer places an attribute
	// whose name the Atlas layout fixes where that layout puts it, such as a
	// table's `engine` first and its `charset` after its names. A table's
	// `engine` takes the place of the engine the common model holds. An index
	// writes any other attribute after its `include`, in the owner's order,
	// and refuses one it writes itself, such as `columns`. Any other block
	// refuses an attribute it has no place for.
	Attributes []EncodedAttribute
	// Losses are what the document cannot say about the block's object. The
	// writer reports each where it writes the block, at the path it names the
	// block with in its own diagnostics, such as `indexes["t"]["i"]`: the
	// block is the writer's, and so is its name.
	Losses []string
}

// TableFacets is a table of a document and facets it holds.
type TableFacets struct {
	// Table is the table, as the writer names it.
	Table Table
	// Facets are the facets.
	Facets schemaext.Facets
}

// EncodeResult is an owner's writing of an [EncodeRequest].
type EncodeResult struct {
	// Blocks are the blocks the owner writes.
	Blocks []EncodedBlock
	// Header are lines the owner writes in the document's header comment,
	// after the document's `ptah:not-described` entries, each as one `// `
	// line, such as the captured `ptah:feature-coverage` claim the owner
	// reads back. A line holds no line break.
	Header []string
	// Attributes are what the owner adds to the writer's own blocks, one
	// entry for each block.
	Attributes []SlotAttributes
}

// Placement is where among the writer's own blocks an encoded block stands.
// Within one placement the writer orders blocks by type, schema, scope and
// name, so the order of a document does not depend on the order owners were
// registered in.
type Placement string

const (
	// BeforeTypes places a block before the types and the tables, among the
	// blocks the rest of a document may depend on, such as an extension that
	// supplies a type.
	BeforeTypes Placement = "before-types"
	// UserTypes places a block among the types a document declares, after
	// the sequences and before the enums, such as a PostgreSQL domain. The
	// writer orders the type blocks it writes itself among the owners' by
	// the same rule: by type, schema and name.
	UserTypes Placement = "user-types"
	// AfterTables places a block after the tables: a block that stands for
	// a table or is attached to one comes after the structure it points at.
	AfterTables Placement = "after-tables"
	// AfterTriggers places a block after the triggers, where the writer's
	// own access-control blocks begin, such as a row-security policy.
	AfterTriggers Placement = "after-triggers"
	// WithinTable places a block inside the table [EncodedBlock.Table]
	// names, after the table's own attributes and before its columns, as a
	// block the frontend reads at the [InTable] site. Such a block takes no
	// label.
	WithinTable Placement = "within-table"
)

// EncodedBlock is one top-level block an owner writes.
type EncodedBlock struct {
	// Type is the block's type, such as "synonym".
	Type string
	// Placement is where the block stands.
	Placement Placement
	// Schema is the block's schema, written as its `schema` reference. What
	// it means, and so how the writer writes it, is SchemaRole.
	Schema string
	// SchemaRole says what Schema is: the namespace the block's object is in
	// by default.
	SchemaRole SchemaRole
	// Name is the block's label. A block [WithinTable] has none.
	Name string
	// Scope is the name of the object the block's object is named inside,
	// such as the table of a policy; the writer orders blocks by it before
	// the name, and writes nothing for it.
	Scope string
	// Table is the table a [WithinTable] block stands in.
	Table *Table
	// Attributes are the attributes the block writes, in order. An attribute
	// the owner leaves out is not written.
	Attributes []EncodedAttribute
	// Children are blocks nested in the block, one level deep, such as the
	// fields of a composite type. The writer writes them after the
	// attributes, in the owner's order, and never sorts them. Only a
	// top-level block takes children.
	Children []EncodedChild
	// Path names the block in a diagnostic: the writer reports Losses at it,
	// and the Atlas-compatible surface's decision to leave the block out. It
	// is the owner's display path, such as `rls_policies.users.tenant`, and
	// not an identity: nothing parses it.
	Path string
	// Losses are what the document cannot say about the block's object. The
	// writer reports each at Path, where the block stands, before the block.
	Losses []string
	// Unwritten leaves the block out, and only its Losses are reported: an
	// object the format cannot spell at all.
	Unwritten bool
	// Provides are names of objects the block's object supplies, such as an
	// extension's types and functions. The Atlas-compatible surface keeps a
	// block it would leave out when the document names one of them.
	Provides []string
	// Object is the object the block writes, and Targets the targets reading
	// the block back binds that object to. An object whose declared target
	// scope is exactly Targets keeps its scope through the document; the
	// writer reports any other scope as one HCL does not represent.
	Object  objectidentity.ID
	Targets []string
}

// EncodedChild is a block nested in an [EncodedBlock]. It writes one label,
// its name, and its attributes in order, which may be none. No two children
// of a block share a type and a name.
type EncodedChild struct {
	// Type is the child's type, such as "field".
	Type string
	// Name is its label.
	Name string
	// Attributes are the attributes it writes, of the kinds a block's take.
	Attributes []EncodedAttribute
}

// SchemaRole is what an encoded block's schema means.
type SchemaRole string

const (
	// SchemaNamespace is the schema the object is in. The writer writes an empty
	// one, of a blank one, of an inspected document as the document's
	// default schema.
	SchemaNamespace SchemaRole = ""
	// SchemaAddress is a schema whose absence is an address of its own, such as a
	// SQL Server extended property of the database. The writer leaves a
	// blank one out.
	SchemaAddress SchemaRole = "address"
	// SchemaPlacement is where the object is installed rather than a namespace of
	// its name, such as an extension's schema. Only an empty one takes an
	// inspected document's default; a schema the target owns is written as
	// a string, so the document declares no schema block for it; and the
	// writer orders the block without it, since it is no part of the
	// object's identity.
	SchemaPlacement SchemaRole = "placement"
)

// EncodedAttribute is one attribute an encoded block writes.
type EncodedAttribute struct {
	// Name is the attribute's name.
	Name string
	// Value is its value. The writer quotes a [String], writes a [Bool] and
	// a [Number] bare, writes [SQL] text as `sql("<text>")`, writes a [Ref] to
	// a table as the reference the document resolves, and writes a [Refs]
	// list element by element: a role it declares as `role.<name>`, any other
	// role and an element of no Kind, such as a role keyword, as a string.
	Value Value
}

// Encodes reports whether an extension of the set writes objects of kind.
func (s Set) Encodes(kind schemaext.Kind) bool {
	return slices.ContainsFunc(s.extensions, func(extension Extension) bool {
		return extension.Encoder != nil && slices.Contains(extension.Kinds, kind)
	})
}

// Encode hands each extension that has an [Encoder] what document holds of
// its Kinds -- the objects, the facets of the writer's blocks and the
// coverage -- in one call per extension, also when the document holds none of
// them, since an owner may refuse a coverage record it cannot write. It
// returns every block and every attribute of the writer's blocks they write,
// extension by extension. It refuses a block without a type, a name or a
// known placement, an attribute without a name, written twice or of a kind
// the writer cannot write, a child of a block in a table, a child without a
// type or a name, written twice or under an attribute's name, a child's
// attribute refused as a block's is, a block that writes an object of a kind
// its extension does not claim, a header line that is empty or holds a line
// break, attributes for a block of the writer's the extension was not handed
// or written for one block twice, and an attribute two extensions write to
// one block. The header lines are returned in the order of the extensions.
// An encoder's own error is returned as it is: it refuses the document, as a
// value the owner cannot write would otherwise be written wrong.
func (s Set) Encode(ctx context.Context, document EncodeRequest) (EncodeResult, error) {
	var encoded EncodeResult
	writers := make(map[slotAttribute]string)
	for _, extension := range s.extensions {
		if extension.Encoder == nil {
			continue
		}
		request := extension.selectRequest(document)
		result, err := extension.Encoder.EncodeHCL(ctx, request)
		if err != nil {
			return EncodeResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return EncodeResult{}, err
		}
		if err := checkEncodeResult(extension, request, result); err != nil {
			return EncodeResult{}, err
		}
		if err := claimSlotAttributes(writers, extension, result.Attributes); err != nil {
			return EncodeResult{}, err
		}
		encoded.Blocks = append(encoded.Blocks, result.Blocks...)
		encoded.Header = append(encoded.Header, result.Header...)
		encoded.Attributes = append(encoded.Attributes, result.Attributes...)
	}
	return encoded, nil
}

// slotAttribute is one attribute of one of the writer's blocks.
type slotAttribute struct {
	slot Slot
	name string
}

// claimSlotAttributes records which extension writes each attribute of the
// writer's blocks, and refuses one another extension already writes.
func claimSlotAttributes(writers map[slotAttribute]string, extension Extension, slots []SlotAttributes) error {
	for _, slot := range slots {
		for _, attribute := range slot.Attributes {
			key := slotAttribute{slot: slot.Slot, name: attribute.Name}
			if owner, found := writers[key]; found {
				return fmt.Errorf("%w: HCL encoders of %s and %s both write attribute %q of %s",
					schemaext.ErrDuplicate, owner, extension.Owner, attribute.Name, slot.Slot)
			}
			writers[key] = extension.Owner
		}
	}
	return nil
}

// String names the block for a diagnostic, such as `index "i" of table
// "public.t"`.
func (s Slot) String() string {
	table := s.Table.Name
	if s.Table.Schema != "" {
		table = s.Table.Schema + "." + s.Table.Name
	}
	switch {
	case s.Kind == SlotTable:
		return fmt.Sprintf("table %q", table)
	case s.Kind == SlotFunction || s.Kind == SlotProcedure:
		return fmt.Sprintf("%s %s(%s)", s.Kind, s.Name, s.Parameters)
	case s.Primary:
		return fmt.Sprintf("the primary key of table %q", table)
	default:
		return fmt.Sprintf("%s %q of table %q", s.Kind, s.Name, table)
	}
}

// selectRequest is what document holds of the extension's Kinds: its objects,
// its tables with those facets alone, and its coverage of them.
func (e Extension) selectRequest(document EncodeRequest) EncodeRequest {
	request := EncodeRequest{Coverage: document.Coverage.SelectKinds(e.Kinds)}
	for _, object := range document.Objects {
		if slices.Contains(e.Kinds, schemaext.Kind(object.Ref.Kind)) {
			request.Objects = append(request.Objects, object)
		}
	}
	for _, table := range document.Tables {
		if facets := e.selectFacets(table.Facets); facets.Len() > 0 {
			request.Tables = append(request.Tables, TableFacets{Table: table.Table, Facets: facets})
		}
	}
	request.Columns = e.selectSlots(document.Columns)
	request.Indexes = e.selectSlots(document.Indexes)
	request.Keys = e.selectSlots(document.Keys)
	request.Routines = e.selectSlots(document.Routines)
	return request
}

// selectSlots are the slots that hold a facet of the extension's Kinds, each
// with those facets alone.
func (e Extension) selectSlots(slots []SlotFacets) []SlotFacets {
	var selected []SlotFacets
	for _, slot := range slots {
		if facets := e.selectFacets(slot.Facets); facets.Len() > 0 {
			selected = append(selected, SlotFacets{Slot: slot.Slot, Facets: facets})
		}
	}
	return selected
}

// selectFacets are the facets of the extension's Kinds.
func (e Extension) selectFacets(facets schemaext.Facets) schemaext.Facets {
	for _, kind := range facets.Kinds() {
		if !slices.Contains(e.Kinds, kind) {
			facets = facets.Without(kind)
		}
	}
	return facets
}

// checkEncodeResult refuses a result the writer cannot write: a block it
// cannot write, a header line that is empty or holds a line break, or
// attributes it cannot write to one of its blocks.
func checkEncodeResult(extension Extension, request EncodeRequest, result EncodeResult) error {
	for _, block := range result.Blocks {
		if err := checkEncodedBlock(extension, block); err != nil {
			return err
		}
	}
	for _, line := range result.Header {
		if strings.TrimSpace(line) == "" || strings.ContainsAny(line, "\r\n") {
			return fmt.Errorf("%w: HCL encoder of %s wrote a header line that is empty or holds a line break: %q",
				schemaext.ErrInvalidValue, extension.Owner, line)
		}
	}
	written := make(map[Slot]bool, len(result.Attributes))
	for _, slot := range result.Attributes {
		if !request.hands(slot.Slot) || written[slot.Slot] {
			return fmt.Errorf("%w: HCL encoder of %s wrote attributes for %s, which it was not handed or wrote twice",
				schemaext.ErrInvalidValue, extension.Owner, slot.Slot)
		}
		written[slot.Slot] = true
		if err := checkSlotAttributes(extension, slot); err != nil {
			return err
		}
	}
	return nil
}

// hands reports whether the request hands the encoder the writer's block slot.
func (r EncodeRequest) hands(slot Slot) bool {
	handed := func(slots []SlotFacets) bool {
		return slices.ContainsFunc(slots, func(entry SlotFacets) bool { return entry.Slot == slot })
	}
	switch slot.Kind {
	case SlotTable:
		return slot.Name == "" && !slot.Primary && slices.ContainsFunc(r.Tables, func(entry TableFacets) bool { return entry.Table == slot.Table })
	case SlotColumn:
		return handed(r.Columns)
	case SlotIndex:
		return handed(r.Indexes)
	case SlotKey:
		return handed(r.Keys)
	case SlotFunction, SlotProcedure:
		return handed(r.Routines)
	default:
		return false
	}
}

// checkSlotAttributes refuses an attribute without a name or written twice,
// and a value the writer cannot write.
func checkSlotAttributes(extension Extension, slot SlotAttributes) error {
	names := make(map[string]bool, len(slot.Attributes))
	for _, attribute := range slot.Attributes {
		if strings.TrimSpace(attribute.Name) == "" || names[attribute.Name] {
			return fmt.Errorf("%w: HCL encoder of %s wrote attribute %q of %s unnamed or twice",
				schemaext.ErrInvalidValue, extension.Owner, attribute.Name, slot.Slot)
		}
		names[attribute.Name] = true
		if !writable(attribute.Value) {
			return fmt.Errorf("%w: HCL encoder of %s wrote attribute %q of %s as %q, which the writer cannot write",
				schemaext.ErrInvalidValue, extension.Owner, attribute.Name, slot.Slot, attribute.Value.Kind)
		}
	}
	return nil
}

// checkEncodedBlock refuses a block the writer cannot write.
func checkEncodedBlock(extension Extension, block EncodedBlock) error {
	if err := checkPlacement(extension, block); err != nil {
		return err
	}
	if block.Object.Kind != "" && !slices.Contains(extension.Kinds, schemaext.Kind(block.Object.Kind)) {
		return fmt.Errorf("%w: HCL encoder of %s wrote an object of kind %q, which its extension does not declare",
			schemaext.ErrInvalidValue, extension.Owner, block.Object.Kind)
	}
	names := make(map[string]bool, len(block.Attributes))
	for _, attribute := range block.Attributes {
		if strings.TrimSpace(attribute.Name) == "" || names[attribute.Name] || attribute.Name == "schema" {
			return fmt.Errorf("%w: HCL encoder of %s wrote block %q with attribute %q unnamed, twice or as its naming's own",
				schemaext.ErrInvalidValue, extension.Owner, block.Type, attribute.Name)
		}
		names[attribute.Name] = true
		if !writable(attribute.Value) {
			return fmt.Errorf("%w: HCL encoder of %s wrote attribute %q of block %q as %q, which the writer cannot write",
				schemaext.ErrInvalidValue, extension.Owner, attribute.Name, block.Type, attribute.Value.Kind)
		}
	}
	return checkEncodedChildren(extension, block, names)
}

// checkEncodedChildren refuses children of a block in a table, a child
// without a type or a name, one written twice or whose type is the name of
// one of the block's attributes, and a child's attribute the writer cannot
// write. attributes are the names of the block's attributes.
func checkEncodedChildren(extension Extension, block EncodedBlock, attributes map[string]bool) error {
	if len(block.Children) > 0 && block.Placement == WithinTable {
		return fmt.Errorf("%w: HCL encoder of %s wrote block %q in a table with children, which only a top-level block takes",
			schemaext.ErrInvalidValue, extension.Owner, block.Type)
	}
	type key struct{ kind, name string }
	written := make(map[key]bool, len(block.Children))
	for _, child := range block.Children {
		if strings.TrimSpace(child.Type) == "" || strings.TrimSpace(child.Name) == "" || attributes[child.Type] {
			return fmt.Errorf("%w: HCL encoder of %s wrote block %q with child %q %q without a type or a name, or under an attribute's name",
				schemaext.ErrInvalidValue, extension.Owner, block.Type, child.Type, child.Name)
		}
		if written[key{child.Type, child.Name}] {
			return fmt.Errorf("%w: HCL encoder of %s wrote %s %q of block %q twice",
				schemaext.ErrDuplicate, extension.Owner, child.Type, child.Name, block.Type)
		}
		written[key{child.Type, child.Name}] = true
		names := make(map[string]bool, len(child.Attributes))
		for _, attribute := range child.Attributes {
			if strings.TrimSpace(attribute.Name) == "" || names[attribute.Name] {
				return fmt.Errorf("%w: HCL encoder of %s wrote %s %q of block %q with attribute %q unnamed or twice",
					schemaext.ErrInvalidValue, extension.Owner, child.Type, child.Name, block.Type, attribute.Name)
			}
			names[attribute.Name] = true
			if !writable(attribute.Value) {
				return fmt.Errorf("%w: HCL encoder of %s wrote attribute %q of %s %q of block %q as %q, which the writer cannot write",
					schemaext.ErrInvalidValue, extension.Owner, attribute.Name, child.Type, child.Name, block.Type, attribute.Value.Kind)
			}
		}
	}
	return nil
}

// checkPlacement refuses a block without a type, at an unknown placement, a
// top-level block without a name or with a table, a block in a table without
// the table or with a name, and an unknown schema role.
func checkPlacement(extension Extension, block EncodedBlock) error {
	switch {
	case strings.TrimSpace(block.Type) == "":
		return fmt.Errorf("%w: HCL encoder of %s wrote a block without a type or a name", schemaext.ErrInvalidValue, extension.Owner)
	case block.Placement == WithinTable:
		if block.Table == nil || block.Name != "" {
			return fmt.Errorf("%w: HCL encoder of %s wrote block %q in a table without the table or with a name",
				schemaext.ErrInvalidValue, extension.Owner, block.Type)
		}
	case block.Placement == BeforeTypes || block.Placement == UserTypes || block.Placement == AfterTables || block.Placement == AfterTriggers:
		if block.Name == "" {
			return fmt.Errorf("%w: HCL encoder of %s wrote a block without a type or a name", schemaext.ErrInvalidValue, extension.Owner)
		}
		if block.Table != nil {
			return fmt.Errorf("%w: HCL encoder of %s wrote top-level block %q with a table", schemaext.ErrInvalidValue, extension.Owner, block.Type)
		}
	default:
		return fmt.Errorf("%w: HCL encoder of %s placed block %q at unknown placement %q",
			schemaext.ErrInvalidValue, extension.Owner, block.Type, block.Placement)
	}
	if block.SchemaRole != SchemaNamespace && block.SchemaRole != SchemaAddress && block.SchemaRole != SchemaPlacement {
		return fmt.Errorf("%w: HCL encoder of %s wrote block %q with schema role %q", schemaext.ErrInvalidValue, extension.Owner, block.Type, block.SchemaRole)
	}
	return nil
}

// writable reports whether the writer can write value: a string, a bool,
// decimal digits, SQL text, a reference to a table, or a list of roles and of
// names of no kind. The writer quotes any SQL text inside its sql() call. A
// [Text] and a [TypeName] are what the reader reads, and an owner writes a
// type as SQL.
func writable(value Value) bool {
	switch value.Kind {
	case String, Bool, SQL:
		return true
	case Number:
		return value.Text != "" && strings.TrimFunc(value.Text, func(r rune) bool { return r >= '0' && r <= '9' }) == ""
	case Ref:
		return value.Reference != nil && value.Reference.Kind == "table" && value.Reference.Name != ""
	case Refs:
		return !slices.ContainsFunc(value.References, func(element Reference) bool {
			return element.Name == "" || (element.Kind != "" && element.Kind != "role")
		})
	default:
		return false
	}
}
