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
// A [Set] freezes the owners one parse selects; the frontend imports no
// owner.
package hclext

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// ErrUnselected is returned for a [Set] nobody built: the zero value. A
// caller that wants no owner passes [None] instead, so a forgotten selection
// is refused rather than read as a decision to leave the owners' blocks
// unread.
var ErrUnselected = errors.New("no HCL schema owners were selected")

// Site is the place in a document where an owner's block may stand.
type Site string

// TopLevel is a block at the top of a document, beside `table` and `schema`.
// Its [Declaration] declares objects of the owner.
const TopLevel Site = "top-level"

// Naming is how a block's labels name the object it declares.
type Naming string

// SchemaAndName names the object by its labels: one label is the name, and
// two are the schema and the name. An optional `schema` attribute, a
// reference such as `schema.app` or a string, names the schema of a
// one-label block, and must agree with the schema label of a two-label one.
const SchemaAndName Naming = "schema-and-name"

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
)

// Attribute is one attribute a block takes.
type Attribute struct {
	// Name is the attribute's name.
	Name string
	// Kind is the kind of value it takes.
	Kind Kind
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
// request, so a document is refused where it is first wrong.
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
	// Coverage is the knowledge every HCL document holds about Kinds, such
	// as that a document without a synonym block describes a database
	// without a synonym. An HCL document names no target, so the claim
	// holds for every document. It is optional; without it the owner claims
	// nothing.
	Coverage func() (schemaext.Coverage, error)
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
// attribute named twice or named as the naming's own, and a block type two
// extensions read at one site.
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
		extension.Blocks = blocks
		set.extensions = append(set.extensions, extension)
	}
	return set, nil
}

func validateBlock(owner string, block Block) error {
	if block.Site != TopLevel {
		return fmt.Errorf("HCL block %q of %s names unknown site %q", block.Type, owner, block.Site)
	}
	if strings.TrimSpace(block.Type) == "" {
		return fmt.Errorf("HCL extension of %s declares a block without a type", owner)
	}
	if block.Naming != SchemaAndName {
		return fmt.Errorf("HCL block %q of %s names unknown naming %q", block.Type, owner, block.Naming)
	}
	names := map[string]bool{"schema": true}
	for _, attribute := range block.Attributes {
		if strings.TrimSpace(attribute.Name) == "" {
			return fmt.Errorf("HCL block %q of %s declares an attribute without a name", block.Type, owner)
		}
		if names[attribute.Name] {
			return fmt.Errorf("%w: HCL block %q of %s declares attribute %q twice or as its naming's own",
				schemaext.ErrDuplicate, block.Type, owner, attribute.Name)
		}
		names[attribute.Name] = true
		if attribute.Kind != String && attribute.Kind != Text {
			return fmt.Errorf("HCL attribute %q of %s block %q names unknown kind %q", attribute.Name, owner, block.Type, attribute.Kind)
		}
	}
	return nil
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
// matches.
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
	for _, index := range order {
		extension := s.extensions[index]
		request := Request{Claims: make([]Claim, 0, len(positions[index]))}
		for _, position := range positions[index] {
			request.Claims = append(request.Claims, matches[position].Claim)
		}
		result, err := extension.Decoder.DecodeHCL(ctx, request)
		if refused, ok := errors.AsType[*ClaimError](err); ok && refused.Claim >= 0 && refused.Claim < len(request.Claims) {
			return nil, &ClaimError{Claim: positions[index][refused.Claim], Err: refused.Err}
		}
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(result.Declarations) != len(request.Claims) {
			return nil, fmt.Errorf("%w: HCL decoder of %s answered %d claims with %d declarations",
				schemaext.ErrInvalidValue, extension.Owner, len(request.Claims), len(result.Declarations))
		}
		for i, position := range positions[index] {
			for _, object := range result.Declarations[i].Objects {
				if !slices.Contains(extension.Kinds, schemaext.Kind(object.Ref.Kind)) {
					return nil, fmt.Errorf("%w: HCL decoder of %s answered an object of kind %q, which its extension does not declare",
						schemaext.ErrInvalidValue, extension.Owner, object.Ref.Kind)
				}
			}
			declarations[position] = result.Declarations[i]
		}
	}
	return declarations, nil
}

// Coverage combines the claims of every extension of the set. A set without
// a claim has none.
func (s Set) Coverage() (schemaext.Coverage, error) {
	var combined schemaext.Coverage
	for _, extension := range s.extensions {
		if extension.Coverage == nil {
			continue
		}
		claim, err := extension.Coverage()
		if err != nil {
			return schemaext.Coverage{}, fmt.Errorf("coverage of %s: %w", extension.Owner, err)
		}
		if combined, err = combined.Combine(claim); err != nil {
			return schemaext.Coverage{}, fmt.Errorf("coverage of %s: %w", extension.Owner, err)
		}
	}
	return combined, nil
}

// Runtime supplies the HCL extensions of one explicit provider selection, so
// a caller parses HCL with the owners it renders and compares with.
// *ptah.run/engine.Runtime implements it.
type Runtime interface {
	HCL() Set
}
