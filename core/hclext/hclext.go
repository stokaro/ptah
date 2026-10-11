// Package hclext is the contract between the HCL schema frontend and the
// feature owners whose models an HCL document can declare.
//
// The frontend, internal/atlashcl, reads the blocks every target shares. A
// block only one owner understands, such as a SQL Server synonym, is the
// owner's. Matching is data: an owner registers the shape of each block it
// reads -- its type, how its labels name the object it declares, and the
// attributes it takes and of which kind -- and the frontend checks a block
// against that shape with the refusals it gives every block, so a block's
// unknown names are refused, or dropped under the frontend's tolerance, the
// same way whoever owns it.
//
// An owner never sees HCL syntax. The frontend evaluates every attribute in
// its own evaluation context and hands the owner the result as data: the
// owner gets no hcl.Expression and no way to evaluate one, so variables,
// locals, functions and the confinement of file() to the project directory
// stay the frontend's. Once a document is read, the frontend hands each owner
// every block of its in one call, and adds the objects the owner returns.
//
// Writing is the mirror. An owner's [Encoder] returns its objects as
// [EncodedBlock] values, data the HCL writer, internal/atlashclrender,
// quotes, references and places, so the owner never writes HCL text.
//
// A [Set] freezes the owners one parse or render selects; neither the
// frontend nor the writer imports an owner.
package hclext

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
)

// ErrUnselected is returned for a [Set] nobody built: the zero value. A
// caller that wants no owner passes [None] instead, so a forgotten selection
// is refused rather than read as a decision to leave the owners' blocks
// unread.
var ErrUnselected = errors.New("no HCL schema owners were selected")

// Site is the place in a document where an owner's block may stand.
type Site string

const (
	// TopLevel is a block at the top of a document, beside `table` and
	// `schema`. Its [Declaration] declares objects of the owner, or a
	// setting of the table its labels name.
	TopLevel Site = "top-level"
	// IndexAttributes is the frontend's own `index` block inside a table,
	// whose attributes an owner extends. The [Block] names the attributes,
	// its Type is "index" and its Naming is empty. Every index block of a
	// document is claimed, with the index in [Claim.Index], because an
	// index's type may select the owner's reading without any of the
	// owner's attributes. Its [Declaration] carries facets of the index.
	IndexAttributes Site = "index-attributes"
	// InTable is a block inside a `table` block, beside its columns. The
	// [Block] has no naming, and the block takes no labels. Its claim carries
	// the table in [Claim.Table], and its [Declaration] carries facets of the
	// table. A block type the frontend reads itself inside a table is the
	// frontend's, whatever an owner registers.
	InTable Site = "in-table"
)

// Naming is how a block's labels name the object it declares.
type Naming string

const (
	// SchemaAndName names the object by its labels: one label is the name,
	// and two are the schema and the name. An optional `schema` attribute, a
	// reference such as `schema.app` or a string, names the schema of a
	// one-label block, and must agree with the schema label of a two-label
	// one. A block whose shape lists `schema` itself, as a [Ref] to `schema`,
	// reads that attribute as such a reference, and its claim carries the
	// reference too; see [Ref].
	SchemaAndName Naming = "schema-and-name"
	// SchemaAndQualifiedName is [SchemaAndName], and a name written
	// `schema.name` where nothing else names a schema names the schema too:
	// `app.hourly` is hourly in app, not a relation whose name holds a dot.
	SchemaAndQualifiedName Naming = "schema-and-qualified-name"
	// JoinedName names the object by its labels alone: one label is the
	// name, and two are joined with a dot into one. The block takes no
	// `schema` attribute, because the object is named inside the object a
	// [Ref] attribute names, as a policy is named on its table.
	JoinedName Naming = "joined-name"
)

// Kind is the kind of value an attribute takes. The frontend reads the
// attribute as that kind and refuses one it cannot; an owner receives only
// the result.
type Kind string

const (
	// String is a string. Any other value is refused.
	String Kind = "string"
	// Text is the text of any value, as the frontend writes it: a string as
	// it is, the SQL of an sql() call, an evaluated variable or function, and
	// otherwise the attribute's source text. It is text the frontend
	// produced, never an expression for the owner to evaluate.
	Text Kind = "text"
	// Bool is a bool. Null reads as if the attribute were absent, and any
	// other value is refused, positioned at the attribute.
	Bool Kind = "bool"
	// Ref is a reference to an object of the [Attribute.Refers] type,
	// read into a [Reference]: `table.users`, `table.app.users` or
	// `table["users"]`, or a name written as a string, an identifier or a
	// variable, where `app.users` names the schema too. A reference to a
	// `table` the document declares carries that table's schema; one that
	// several declared tables match is refused, naming their schemas, and
	// one that none matches is kept as written. The attribute is required: a
	// block that leaves it out, or writes a reference to another type, is
	// refused as `<type> "<name>" requires <attribute>`. An
	// [Attribute.Optional] reference a block leaves out has no entry.
	//
	// A reference to a `schema` names one schema: a string, a string
	// template, `schema.app` or `schema["app"]`. Anything else, a qualified
	// reference included, is refused as `<type> attribute "<attribute>" must
	// be a string template or exact schema reference`. A schema is no
	// relation of the document, so the reference is not resolved against its
	// tables.
	Ref Kind = "reference"
	// Refs is a list of references to objects of the
	// [Attribute.Refers] type, or a single one, each read into a
	// [Reference]: `role.app`, `role["app"]`, or a name written as a string
	// or an identifier, which is the name as written. An element that refers
	// to another type of object is refused, positioned at the attribute.
	Refs Kind = "references"
	// Number is decimal digits the writer writes bare, such as a table's
	// `auto_increment`. Only the writer takes it: a block an owner reads
	// takes no Number.
	Number Kind = "number"
	// SQL is a SQL expression the writer writes as an sql() call, such as a
	// column's `on_update`. The writer writes the text and never evaluates
	// it, as the reader hands an owner the text of an sql() call and never
	// the call. Only the writer takes it.
	SQL Kind = "sql"
)

// Attribute is one attribute a block takes.
type Attribute struct {
	// Name is the attribute's name.
	Name string
	// Kind is the kind of value it takes.
	Kind Kind
	// Refers is the block type a [Ref] or [Refs] attribute
	// refers to, such as "table" or "role". It is required for those kinds
	// and refused for the others.
	Refers string
	// Optional makes a [Ref] attribute one a block may leave out, as an
	// extension's installation `schema` is. It is refused on every other
	// kind, which a block may leave out already.
	Optional bool
}

// Block is the shape of one block an owner reads.
type Block struct {
	// Site is where the block stands.
	Site Site
	// Type is the block's type, such as "synonym".
	Type string
	// Naming is how its labels name the object it declares.
	Naming Naming
	// Attributes are the attributes it takes, in the order the frontend
	// reads them. Every other attribute and every nested block is an unknown
	// name. The naming's own attribute, such as `schema`, is not listed.
	Attributes []Attribute
}

// Value is an attribute's value, as the frontend read it.
type Value struct {
	// Kind is the kind the attribute was read as.
	Kind Kind
	// Text is the value of a [String] or a [Text] attribute.
	Text string
	// Bool is the value of a [Bool] attribute.
	Bool bool
	// Reference is the value of a [Ref] attribute.
	Reference *Reference
	// References is the value of a [Refs] attribute, in the order the
	// document writes them.
	References []Reference
}

// Reference is an object a [Ref] or [Refs] attribute refers to,
// as the frontend resolved it.
type Reference struct {
	// Kind is the block type it refers to, the attribute's Refers.
	Kind string
	// Schema is the schema it names, or empty for none.
	Schema string
	// Name is the object's name.
	Name string
}

// Claim is one block of a document an owner's shape matched.
type Claim struct {
	// Block is the block's type.
	Block string
	// Schema is the schema its labels name, or empty for none.
	Schema string
	// Name is the name its labels give the object.
	Name string
	// Attributes holds the value of each attribute the block writes. An
	// attribute the block leaves out has no entry.
	Attributes map[string]Value
	// Index is the index an [IndexAttributes] claim is about, whose name is
	// Name, and nil for a claim at another site.
	Index *Index
	// Table is the table an [InTable] claim stands in, and nil for a claim
	// at another site.
	Table *Table
	// At is where the block stands, as `<file>:<line>`, for an owner that
	// names two blocks in one refusal.
	At string
}

// Table is what the frontend read of the table an [InTable] claim stands in.
type Table struct {
	// Schema is the table's schema, or empty for none.
	Schema string
	// Name is the table's name.
	Name string
	// Struct is the Go struct name the frontend gives the table.
	Struct string
}

// Index is what the frontend read of an index an [IndexAttributes] claim is
// about.
type Index struct {
	// Table is the table the index is on, qualified as the frontend names
	// it.
	Table string
	// Type is the index's `type`, as written.
	Type string
	// Operator is the operator class the index names, as written.
	Operator string
}

// Request is every block of one document an owner's shapes matched, in the
// order the document holds them.
type Request struct {
	Claims []Claim
}

// Declaration is the owner's reading of one claim.
type Declaration struct {
	// Objects are the objects the block declares, each of one of the
	// extension's Kinds. The frontend adds them to the document and refuses
	// an object the document declares twice.
	Objects []schemaext.Object
	// TableSetting is a setting the block declares on the table its labels
	// name, such as a TimescaleDB hypertable, of one of the extension's
	// Kinds. The frontend attaches it once every table of the load is known,
	// so the block and the table may sit in different files of one schema
	// directory, and refuses a table the load does not declare and a second
	// setting of one table. Nil declares none.
	TableSetting schemaext.Value
	// Facets are settings of the claim's index, for an [IndexAttributes]
	// claim, or of its table, for an [InTable] claim, each of one of the
	// extension's Kinds. The frontend adds them to the index's or the
	// table's facets.
	Facets schemaext.Facets
}

// Result is an owner's reading of a [Request].
type Result struct {
	// Declarations holds one declaration for each claim, in the request's
	// order.
	Declarations []Declaration
}

// ClaimError is an owner's refusal of one claim. The frontend reports Err
// positioned at the claim's block, as it reports a refusal of its own.
type ClaimError struct {
	// Claim is the refused claim's index in the request.
	Claim int
	// Err says why.
	Err error
}

// Error says why the claim was refused.
func (e *ClaimError) Error() string {
	return fmt.Sprintf("claim %d: %v", e.Claim, e.Err)
}

// Unwrap returns the reason.
func (e *ClaimError) Unwrap() error {
	return e.Err
}

// Decoder reads the claims of one document. An owner refuses a block it
// cannot read with a [ClaimError], and refuses the first such block of the
// request, so a document is refused where it is first wrong. It checks every
// claim on its own before it refuses one claim against another, such as two
// blocks that declare one object: that refusal names no single block, so
// the frontend reports it once its own checks of the whole document pass.
type Decoder interface {
	DecodeHCL(ctx context.Context, request Request) (Result, error)
}

// Extension is what one feature owner contributes to the HCL frontend.
type Extension struct {
	// Owner names the owner, as its provider ID does.
	Owner string
	// Kinds are the models the blocks declare and the claim covers. The
	// provider that registers the extension owns their desired codecs.
	Kinds []schemaext.Kind
	// Blocks are the shapes of the blocks the owner reads.
	Blocks []Block
	// Decoder reads the blocks. It is required with Blocks.
	Decoder Decoder
	// Header lists the kinds a document's `ptah:not-described` header may
	// name that the owner reads as limits on its own claim, rather than as
	// the common coverage vocabulary, such as a YDB coordination node the
	// document leaves unmanaged.
	Header []coverage.Kind
	// CapturedCoverage reports that the owner reads a document's captured
	// `ptah:feature-coverage` header: the exact claim an export wrote. A
	// document carrying one is refused unless an owner of the set reads it.
	CapturedCoverage bool
	// Coverage is the knowledge every HCL document holds about Kinds, such
	// as that a document without a synonym block describes a database
	// without a synonym. An HCL document names no target, so the claim
	// holds for every document. It is handed the document's header entries
	// of the Header kinds, its captured header where CapturedCoverage is
	// set, and the objects the document declares. It is optional; without
	// it the owner claims nothing.
	Coverage func(request CoverageRequest) (schemaext.Coverage, error)
	// Encoder writes the objects of Kinds a document holds as blocks, for
	// the HCL writer; see [Set.Encode]. It is optional; without it the
	// writer reports an object of Kinds as one HCL does not represent.
	Encoder Encoder
}

// CoverageRequest is what a document says to one owner's claim.
type CoverageRequest struct {
	// Limits are the header's `ptah:not-described` entries of the owner's
	// Header kinds, in the header's order.
	Limits []coverage.Object
	// Captured are the bodies of the document's `ptah:feature-coverage`
	// header comments, marker included, in order: text, which
	// schemaext.Registry.DecodeCoverageHeaderBodies decodes. Only an owner
	// that sets CapturedCoverage receives them.
	Captured []string
	// Declared are the objects the document declares, the owners' and the
	// frontend's, such as the policies whose tables a claim leaves to a
	// default.
	Declared schemaext.Objects
}

// Set is the frozen collection of extensions one parse selects. The zero
// value is unselected and refused by the frontend; [None] selects no owner.
// A Set is safe for concurrent use.
type Set struct {
	selected   bool
	extensions []Extension
	// blocks holds, for each site and block type, the extension index that
	// reads it and the block's shape.
	blocks map[blockKey]shape
}

type blockKey struct {
	site  Site
	block string
}

type shape struct {
	extension int
	block     Block
}

// None returns a selected set without owners. A parse with it reads the
// frontend's own blocks alone.
func None() Set {
	return Set{selected: true}
}

// NewSet validates and freezes extensions. It refuses an extension without an
// owner, a model two extensions claim, a block without a known site, a type, a
// known naming or a decoder, an attribute without a name or a known kind, an
// attribute named twice or named as the naming's own, a block type two
// extensions read at one site, and an encoder without a model to encode.
func NewSet(extensions ...Extension) (Set, error) {
	set := Set{selected: true, blocks: make(map[blockKey]shape)}
	kinds := make(map[schemaext.Kind]string)
	for index, extension := range extensions {
		if strings.TrimSpace(extension.Owner) == "" {
			return Set{}, fmt.Errorf("HCL extension %d names no owner", index)
		}
		for _, kind := range extension.Kinds {
			if owner, claimed := kinds[kind]; claimed {
				return Set{}, fmt.Errorf("%w: model %q is claimed by %s and %s", schemaext.ErrDuplicate, kind, owner, extension.Owner)
			}
			kinds[kind] = extension.Owner
		}
		if len(extension.Blocks) > 0 && extension.Decoder == nil {
			return Set{}, fmt.Errorf("HCL extension of %s declares blocks without a decoder", extension.Owner)
		}
		if extension.Encoder != nil && len(extension.Kinds) == 0 {
			return Set{}, fmt.Errorf("HCL extension of %s declares an encoder without a model to encode", extension.Owner)
		}
		blocks := make([]Block, 0, len(extension.Blocks))
		for _, block := range extension.Blocks {
			if err := validateBlock(extension.Owner, block); err != nil {
				return Set{}, err
			}
			key := blockKey{site: block.Site, block: block.Type}
			if previous, claimed := set.blocks[key]; claimed {
				return Set{}, fmt.Errorf("%w: HCL %s block %q is read by %s and %s", schemaext.ErrDuplicate, block.Site, block.Type,
					set.extensions[previous.extension].Owner, extension.Owner)
			}
			block.Attributes = slices.Clone(block.Attributes)
			set.blocks[key] = shape{extension: index, block: block}
			blocks = append(blocks, block)
		}
		extension.Kinds = slices.Clone(extension.Kinds)
		extension.Header = slices.Clone(extension.Header)
		extension.Blocks = blocks
		set.extensions = append(set.extensions, extension)
	}
	return set, nil
}

func validateBlock(owner string, block Block) error {
	if strings.TrimSpace(block.Type) == "" {
		return fmt.Errorf("HCL extension of %s declares a block without a type", owner)
	}
	if err := validateNaming(owner, block); err != nil {
		return err
	}
	names := map[string]bool{"schema": block.Naming == SchemaAndName || block.Naming == SchemaAndQualifiedName}
	for _, attribute := range block.Attributes {
		if strings.TrimSpace(attribute.Name) == "" {
			return fmt.Errorf("HCL block %q of %s declares an attribute without a name", block.Type, owner)
		}
		if attribute.Name == "schema" && names["schema"] && NamesSchema(attribute) {
			// The naming's own schema, listed to be read as a reference.
			names[attribute.Name] = false
		}
		if names[attribute.Name] {
			return fmt.Errorf("%w: HCL block %q of %s declares attribute %q twice or as its naming's own",
				schemaext.ErrDuplicate, block.Type, owner, attribute.Name)
		}
		names[attribute.Name] = true
		if err := validateKind(owner, block, attribute); err != nil {
			return err
		}
	}
	return nil
}

// validateNaming refuses a site the frontend does not know, and a naming the
// site does not take: an index or a table block has none, and a top-level
// block one of the namings.
func validateNaming(owner string, block Block) error {
	switch block.Site {
	case TopLevel:
		if block.Naming != SchemaAndName && block.Naming != SchemaAndQualifiedName && block.Naming != JoinedName {
			return fmt.Errorf("HCL block %q of %s names unknown naming %q", block.Type, owner, block.Naming)
		}
	case IndexAttributes:
		if block.Type != "index" || block.Naming != "" {
			return fmt.Errorf("HCL %s block of %s must be of type \"index\" with no naming", block.Site, owner)
		}
	case InTable:
		if block.Naming != "" {
			return fmt.Errorf("HCL %s block %q of %s takes no naming", block.Site, block.Type, owner)
		}
	default:
		return fmt.Errorf("HCL block %q of %s names unknown site %q", block.Type, owner, block.Site)
	}
	return nil
}

// validateKind refuses an unknown kind, a reference without the type it
// refers to, and a type to refer to on another kind.
func validateKind(owner string, block Block, attribute Attribute) error {
	if attribute.Optional && attribute.Kind != Ref {
		return fmt.Errorf("HCL attribute %q of %s block %q is a %s, and only a reference is made optional", attribute.Name, owner, block.Type, attribute.Kind)
	}
	switch attribute.Kind {
	case String, Text, Bool:
		if attribute.Refers != "" {
			return fmt.Errorf("HCL attribute %q of %s block %q is a %s and refers to nothing", attribute.Name, owner, block.Type, attribute.Kind)
		}
	case Ref, Refs:
		if strings.TrimSpace(attribute.Refers) == "" {
			return fmt.Errorf("HCL attribute %q of %s block %q is a %s and names no type it refers to", attribute.Name, owner, block.Type, attribute.Kind)
		}
	default:
		return fmt.Errorf("HCL attribute %q of %s block %q names unknown kind %q", attribute.Name, owner, block.Type, attribute.Kind)
	}
	return nil
}

// NamesSchema reports whether attribute is a [Ref] to a `schema`, which a
// [SchemaAndName] block may list as its naming's own `schema`.
func NamesSchema(attribute Attribute) bool {
	return attribute.Kind == Ref && attribute.Refers == "schema"
}

// Selected reports whether the set was built by [NewSet] or [None].
func (s Set) Selected() bool {
	return s.selected
}

// Shape is the shape of a block an extension of the set reads.
type Shape struct {
	// Block is the block's shape.
	Block     Block
	extension int
}

// Lookup returns the shape of the block of type an extension reads at site,
// and false for a block no extension reads there.
func (s Set) Lookup(site Site, block string) (Shape, bool) {
	found, ok := s.blocks[blockKey{site: site, block: block}]
	if !ok {
		return Shape{}, false
	}
	return Shape{Block: found.block, extension: found.extension}, true
}

// Match is a claim the frontend made for a shape the set returned.
type Match struct {
	// Shape is the shape [Set.Lookup] returned.
	Shape Shape
	// Claim is what the frontend read from the block.
	Claim Claim
}

// Decode hands each extension the claims its shapes matched, in one call per
// extension and in the order of matches, and returns the declarations in the
// order of matches. A decoder that fails, or answers with another number of
// declarations than it was given claims, fails the decode. A [ClaimError] a
// decoder returns is returned with Claim rewritten to the claim's index in
// matches. Of the refusals of several decoders, the [ClaimError] of the
// earliest claim is returned, and a refusal that is no ClaimError only where
// no decoder refused a claim.
func (s Set) Decode(ctx context.Context, matches []Match) ([]Declaration, error) {
	declarations := make([]Declaration, len(matches))
	positions := make(map[int][]int)
	var order []int
	for position, match := range matches {
		if _, seen := positions[match.Shape.extension]; !seen {
			order = append(order, match.Shape.extension)
		}
		positions[match.Shape.extension] = append(positions[match.Shape.extension], position)
	}
	var earliest *ClaimError
	var unpositioned error
	for _, index := range order {
		extension := s.extensions[index]
		request := Request{Claims: make([]Claim, 0, len(positions[index]))}
		for _, position := range positions[index] {
			request.Claims = append(request.Claims, matches[position].Claim)
		}
		result, err := extension.Decoder.DecodeHCL(ctx, request)
		if refused, ok := errors.AsType[*ClaimError](err); ok && refused.Claim >= 0 && refused.Claim < len(request.Claims) {
			// Another owner may refuse a claim the document holds earlier,
			// and the document is refused where it is first wrong.
			if claim := positions[index][refused.Claim]; earliest == nil || claim < earliest.Claim {
				earliest = &ClaimError{Claim: claim, Err: refused.Err}
			}
			continue
		}
		if err != nil {
			// A refusal that names no claim yields to one that does: the
			// document is refused where it is first wrong, and the first
			// refusal of a claim is earlier than one of the document as a
			// whole.
			if unpositioned == nil {
				unpositioned = err
			}
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(result.Declarations) != len(request.Claims) {
			return nil, fmt.Errorf("%w: HCL decoder of %s answered %d claims with %d declarations",
				schemaext.ErrInvalidValue, extension.Owner, len(request.Claims), len(result.Declarations))
		}
		for i, position := range positions[index] {
			if err := checkDeclaration(extension, result.Declarations[i]); err != nil {
				return nil, err
			}
			declarations[position] = result.Declarations[i]
		}
	}
	if earliest != nil {
		return nil, earliest
	}
	if unpositioned != nil {
		return nil, unpositioned
	}
	return declarations, nil
}

// checkDeclaration refuses a declaration of a model the extension does not
// claim.
func checkDeclaration(extension Extension, declaration Declaration) error {
	kinds := make([]schemaext.Kind, 0, len(declaration.Objects)+1)
	for _, object := range declaration.Objects {
		kinds = append(kinds, schemaext.Kind(object.Ref.Kind))
	}
	if declaration.TableSetting != nil {
		kinds = append(kinds, declaration.TableSetting.Kind())
	}
	kinds = append(kinds, declaration.Facets.Kinds()...)
	for _, kind := range kinds {
		if !slices.Contains(extension.Kinds, kind) {
			return fmt.Errorf("%w: HCL decoder of %s answered a model of kind %q, which its extension does not declare",
				schemaext.ErrInvalidValue, extension.Owner, kind)
		}
	}
	return nil
}

// Coverage combines the claims of every extension of the set. document is
// what the whole document says: every header entry an owner reads, the
// captured header and the declared objects. Each extension is handed the
// entries of the kinds it reads, the captured header where it reads one, and
// the declared objects. A set without a claim has none. An extension's error
// is returned as it is.
func (s Set) Coverage(document CoverageRequest) (schemaext.Coverage, error) {
	var combined schemaext.Coverage
	for _, extension := range s.extensions {
		if extension.Coverage == nil {
			continue
		}
		request := CoverageRequest{Declared: document.Declared}
		for _, entry := range document.Limits {
			if containsKind(extension.Header, entry.Kind) {
				request.Limits = append(request.Limits, entry)
			}
		}
		if extension.CapturedCoverage {
			request.Captured = slices.Clone(document.Captured)
		}
		// An owner's refusal is the reader's refusal of the document, in the
		// owner's words, so it is not wrapped.
		claim, err := extension.Coverage(request)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if combined, err = combined.Combine(claim); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return combined, nil
}

// ReadsHeader reports whether an extension of the set reads a header entry of
// kind as a limit on its own claim; see [Extension.Header].
func (s Set) ReadsHeader(kind coverage.Kind) bool {
	return slices.ContainsFunc(s.extensions, func(extension Extension) bool { return containsKind(extension.Header, kind) })
}

// ReadsCapturedCoverage reports whether an extension of the set reads a
// captured feature coverage header; see [Extension.CapturedCoverage].
func (s Set) ReadsCapturedCoverage() bool {
	return slices.ContainsFunc(s.extensions, func(extension Extension) bool { return extension.CapturedCoverage })
}

// containsKind compares kinds as a header spells them, ignoring case and
// surrounding space. It lowers both rather than folding them, as core/sqlext
// does: [strings.EqualFold] also matches U+017F to "s".
func containsKind(kinds []coverage.Kind, kind coverage.Kind) bool {
	want := strings.ToLower(strings.TrimSpace(string(kind)))
	return slices.ContainsFunc(kinds, func(candidate coverage.Kind) bool {
		return strings.ToLower(strings.TrimSpace(string(candidate))) == want
	})
}

// Runtime supplies the HCL extensions of one explicit provider selection, so
// a caller parses HCL with the owners it renders and compares with.
// *ptah.run/engine.Runtime implements it.
type Runtime interface {
	HCL() Set
}
