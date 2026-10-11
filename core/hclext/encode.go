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
	// Coverage is what the document knows of the extension's Kinds, such
	// as a limit the format may have no spelling for.
	Coverage schemaext.Coverage
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
}

// Placement is where among the writer's own top-level blocks an encoded
// block stands. Within one placement the writer orders blocks by type,
// schema and name, so the order of a document does not depend on the order
// owners were registered in.
type Placement string

// AfterTables places a block after the tables: a block that stands for a
// table or is attached to one comes after the structure it points at.
const AfterTables Placement = "after-tables"

// EncodedBlock is one top-level block an owner writes.
type EncodedBlock struct {
	// Type is the block's type, such as "synonym".
	Type string
	// Placement is where the block stands.
	Placement Placement
	// Schema is the schema the block's object is in, written as the
	// block's `schema` reference. The writer writes an empty Schema of an
	// inspected document as the document's default schema, unless
	// SchemaIsAddress.
	Schema string
	// SchemaIsAddress marks an empty Schema as an address of its own, such
	// as a SQL Server extended property of the database, which the writer
	// leaves empty.
	SchemaIsAddress bool
	// Name is the block's label.
	Name string
	// Attributes are the attributes the block writes, in order. An attribute
	// the owner leaves out is not written.
	Attributes []EncodedAttribute
	// Object is the object the block writes, and Targets the targets reading
	// the block back binds that object to. An object whose declared target
	// scope is exactly Targets keeps its scope through the document; the
	// writer reports any other scope as one HCL does not represent.
	Object  objectidentity.ID
	Targets []string
}

// EncodedAttribute is one attribute an encoded block writes.
type EncodedAttribute struct {
	// Name is the attribute's name.
	Name string
	// Value is its value, a [String] or a [Bool]. The writer quotes a
	// String.
	Value Value
}

// Encodes reports whether an extension of the set writes objects of kind.
func (s Set) Encodes(kind schemaext.Kind) bool {
	return slices.ContainsFunc(s.extensions, func(extension Extension) bool {
		return extension.Encoder != nil && slices.Contains(extension.Kinds, kind)
	})
}

// Encode hands each extension that has an [Encoder] what document holds of
// its Kinds -- the objects, the tables' facets and the coverage -- in one call
// per extension, also when the document holds none of them, since an owner
// may refuse a coverage record it cannot write. It returns every block they
// write, extension by extension. It refuses a block without a type, a name or
// a known placement, an attribute without a name, written twice or of a kind
// the writer cannot write, and a block that writes an object of a kind its
// extension does not claim, and a header line that is empty or holds a line
// break. The header lines are returned in the order of the extensions. An
// encoder's own error is returned as it is: it refuses the document, as a
// value the owner cannot write would otherwise be written wrong.
func (s Set) Encode(ctx context.Context, document EncodeRequest) (EncodeResult, error) {
	var encoded EncodeResult
	for _, extension := range s.extensions {
		if extension.Encoder == nil {
			continue
		}
		result, err := extension.Encoder.EncodeHCL(ctx, extension.selectRequest(document))
		if err != nil {
			return EncodeResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return EncodeResult{}, err
		}
		if err := checkEncodeResult(extension, result); err != nil {
			return EncodeResult{}, err
		}
		encoded.Blocks = append(encoded.Blocks, result.Blocks...)
		encoded.Header = append(encoded.Header, result.Header...)
	}
	return encoded, nil
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
		facets := table.Facets
		for _, kind := range facets.Kinds() {
			if !slices.Contains(e.Kinds, kind) {
				facets = facets.Without(kind)
			}
		}
		if facets.Len() > 0 {
			request.Tables = append(request.Tables, TableFacets{Table: table.Table, Facets: facets})
		}
	}
	return request
}

// checkEncodeResult refuses a result the writer cannot write: a block it
// cannot write, or a header line that is empty or holds a line break.
func checkEncodeResult(extension Extension, result EncodeResult) error {
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
	return nil
}

// checkEncodedBlock refuses a block the writer cannot write.
func checkEncodedBlock(extension Extension, block EncodedBlock) error {
	if strings.TrimSpace(block.Type) == "" || block.Name == "" {
		return fmt.Errorf("%w: HCL encoder of %s wrote a block without a type or a name", schemaext.ErrInvalidValue, extension.Owner)
	}
	if block.Placement != AfterTables {
		return fmt.Errorf("%w: HCL encoder of %s placed block %q at unknown placement %q",
			schemaext.ErrInvalidValue, extension.Owner, block.Type, block.Placement)
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
		if attribute.Value.Kind != String && attribute.Value.Kind != Bool {
			return fmt.Errorf("%w: HCL encoder of %s wrote attribute %q of block %q as %q, which the writer cannot write",
				schemaext.ErrInvalidValue, extension.Owner, attribute.Name, block.Type, attribute.Value.Kind)
		}
	}
	return nil
}
