package pgpublicationsource

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/objectowner"
)

// hclBlock is the HCL block a publication is declared with.
const hclBlock = "publication"

// HCL is the publication owner's contribution to the HCL frontend: the
// `publication` block, whose label is the publication's name.
//
//	publication "orders" {
//	  tables                     = [table.public.orders, table.public.items]
//	  schemas                    = ["audit"]
//	  publish                    = ["insert", "update"]
//	  publish_via_partition_root = true
//	  publish_generated_columns  = "stored"
//	  owner                      = role.replicator
//	}
//
// `all_tables = true` publishes every table of the database and takes no
// `tables` or `schemas`; a block with none of the three publishes nothing
// yet. A table reference that names no schema is the default schema's.
// `publish` left out publishes insert, update, delete and truncate, and
// `publish = []` publishes none. `owner` left out leaves the owner
// unmanaged.
//
// Its claim is [DocumentCoverage]'s. The writer writes each
// publication back as the same block, every table qualified by its schema,
// and refuses one with a row filter or a column list, which the block cannot
// say.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: pgpublication.Owner,
		Kinds: []schemaext.Kind{pgpublication.PublicationKind},
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: hclBlock, Naming: hclext.JoinedName,
			Attributes: []hclext.Attribute{
				{Name: "all_tables", Kind: hclext.Bool},
				{Name: "tables", Kind: hclext.Refs, Refers: "table"},
				{Name: "schemas", Kind: hclext.TextList},
				{Name: "publish", Kind: hclext.TextList},
				{Name: "publish_via_partition_root", Kind: hclext.Bool},
				{Name: "publish_generated_columns", Kind: hclext.String},
				{Name: "owner", Kind: hclext.Ref, Refers: "role", Optional: true},
			}}},
		Decoder: hclDecoder{},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) {
			return DocumentCoverage()
		},
		Encoder: hclEncoder{},
	}
}

// hclDecoder reads the publication blocks of one document.
type hclDecoder struct{}

// DecodeHCL declares one publication for each block, with no target scope, as
// an HCL document names no target.
func (hclDecoder) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		object, err := decodeClaim(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		declarations = append(declarations, hclext.Declaration{Objects: []schemaext.Object{object}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

func decodeClaim(claim hclext.Claim) (schemaext.Object, error) {
	attributes := claim.Attributes
	declared := pgpublication.DesiredPublication{AllTables: attributes["all_tables"].Bool,
		ViaPartitionRoot: attributes["publish_via_partition_root"].Bool, GeneratedColumns: attributes["publish_generated_columns"].Text,
		Schemas: slices.Clone(attributes["schemas"].Texts)}
	for _, reference := range attributes["tables"].References {
		declared.Tables = append(declared.Tables, pgpublication.Member{Schema: reference.Schema, Table: reference.Name})
	}
	if publish, written := attributes["publish"]; written {
		declared.Publish = append(make([]string, 0, len(publish.Texts)), publish.Texts...)
	}
	if owner, written := attributes["owner"]; written && owner.Reference != nil {
		if err := objectowner.ValidateRole(owner.Reference.Name); err != nil {
			return schemaext.Object{}, fmt.Errorf("publication %s: %w", claim.Name, err)
		}
		declared.Owner = owner.Reference.Name
	}
	return Declare(claim.Name, declared)
}

// hclEncoder writes the publications of one document.
type hclEncoder struct{}

// EncodeHCL writes one publication block for each publication, after the
// tables it names, as the reader reads it back. A value that is not a valid
// declaration, or one with a row filter or a column list, is refused rather
// than written.
func (hclEncoder) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	blocks := make([]hclext.EncodedBlock, 0, len(request.Objects))
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		publication, ok := object.Value.(*pgpublication.DesiredPublication)
		if !ok {
			return hclext.EncodeResult{}, fmt.Errorf("%w: HCL requires desired publication values", schemaext.ErrInvalidValue)
		}
		if err := pgpublication.ValidateRef(object.Ref); err != nil {
			return hclext.EncodeResult{}, err
		}
		if err := pgpublication.ValidateDesired(publication); err != nil {
			return hclext.EncodeResult{}, err
		}
		name := pgpublication.Name(object.Ref)
		if err := pgpublication.RefuseFiltered(name, publication); err != nil {
			return hclext.EncodeResult{}, err
		}
		// A publication belongs to the database, so the block names no
		// schema: an address left blank writes none.
		blocks = append(blocks, hclext.EncodedBlock{Type: hclBlock, Placement: hclext.AfterTables, Name: name,
			SchemaRole: hclext.SchemaAddress, Path: "publications." + name, Object: object.Ref, Attributes: encodedAttributes(publication)})
	}
	return hclext.EncodeResult{Blocks: blocks}, nil
}

// encodedAttributes are the attributes a publication writes, each only where
// it differs from what the block means without it.
func encodedAttributes(publication *pgpublication.DesiredPublication) []hclext.EncodedAttribute {
	var attributes []hclext.EncodedAttribute
	boolean := func(name string) hclext.EncodedAttribute {
		return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.Bool, Bool: true}}
	}
	if publication.AllTables {
		attributes = append(attributes, boolean("all_tables"))
	}
	if len(publication.Tables) != 0 {
		references := make([]hclext.Reference, 0, len(publication.Tables))
		for _, member := range pgpublication.SortMembers(publication.Tables) {
			references = append(references, hclext.Reference{Kind: "table", Schema: member.Schema, Name: member.Table})
		}
		attributes = append(attributes, hclext.EncodedAttribute{Name: "tables", Value: hclext.Value{Kind: hclext.Refs, References: references}})
	}
	if len(publication.Schemas) != 0 {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "schemas",
			Value: hclext.Value{Kind: hclext.TextList, Texts: pgpublication.SortNames(publication.Schemas)}})
	}
	if publication.Publish != nil {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "publish",
			Value: hclext.Value{Kind: hclext.TextList, Texts: publication.EffectivePublish()}})
	}
	if publication.ViaPartitionRoot {
		attributes = append(attributes, boolean("publish_via_partition_root"))
	}
	if publication.GeneratedColumns != "" {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "publish_generated_columns",
			Value: hclext.Value{Kind: hclext.String, Text: publication.GeneratedColumns}})
	}
	if publication.Owner != "" {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "owner",
			Value: hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "role", Name: publication.Owner}}})
	}
	return attributes
}
