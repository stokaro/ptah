package ydbsource

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// The HCL blocks the YDB owner reads.
const (
	coordinationBlock = "coordination_node"
	indexBlock        = "index"
)

// HCL is the YDB owner's extension of the HCL frontend:
//   - a top-level `coordination_node` block, whose settings are string
//     attributes spelled as the annotation spells them and checked by
//     ydbcoordination, so a value the annotation refuses is refused here too;
//   - a vector index's settings and an index's partitioning, written as
//     attributes of the frontend's own `index` block, read as the index
//     annotation reads them;
//   - a `ptah:not-described coordination_node` header entry, which leaves a
//     node unmanaged, and a captured `ptah:feature-coverage` header.
//
// All are Ptah extensions of the format: the pinned Atlas community binary has
// no YDB driver. The claim enrolls the coordination nodes a document can
// declare, less the ones its header leaves unmanaged, or exactly the captured
// header's account.
func HCL() hclext.Extension {
	nodeAttributes := make([]hclext.Attribute, 0, len(ydbcoordination.Settings()))
	for _, setting := range ydbcoordination.Settings() {
		nodeAttributes = append(nodeAttributes, hclext.Attribute{Name: setting, Kind: hclext.String})
	}
	indexAttributes := make([]hclext.Attribute, 0, len(ydbindex.VectorAttributes())+len(ydbpartition.Attributes()))
	for _, name := range slices.Concat(ydbindex.VectorAttributes(), ydbpartition.Attributes()) {
		indexAttributes = append(indexAttributes, hclext.Attribute{Name: name, Kind: hclext.Text})
	}
	return hclext.Extension{
		Owner: ydbschema.Owner,
		Kinds: []schemaext.Kind{ydbcoordination.Kind, ydbschema.VectorIndexKind, ydbschema.IndexPartitioningKind},
		Blocks: []hclext.Block{
			{Site: hclext.TopLevel, Type: coordinationBlock, Naming: hclext.SchemaAndName, Attributes: nodeAttributes},
			{Site: hclext.IndexAttributes, Type: indexBlock, Attributes: indexAttributes},
		},
		Decoder:          hclDecoder{},
		Header:           []coverage.Kind{coverage.Kind(sourceKindToken(ydbcoordination.Kind))},
		CapturedCoverage: true,
		Coverage:         hclClaim,
	}
}

// sourceKindToken returns the header spelling of a source family's kind.
func sourceKindToken(kind schemaext.Kind) string {
	for _, family := range sourceKinds {
		if family.kind == kind {
			return family.token
		}
	}
	return ""
}

// hclDecoder reads the YDB owner's blocks and index attributes of one
// document.
type hclDecoder struct{}

// DecodeHCL reads each claim in the document's order.
func (hclDecoder) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	var nodes schemaext.Objects
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		if claim.Index != nil {
			facets, err := hclIndexFacets(claim)
			if err != nil {
				return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
			}
			declarations = append(declarations, hclext.Declaration{Facets: facets})
			continue
		}
		object, err := hclCoordinationNode(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		if nodes, err = nodes.With(object); err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: fmt.Errorf("%s %q: %v", coordinationBlock, claim.Name, err)}
		}
		declarations = append(declarations, hclext.Declaration{Objects: []schemaext.Object{object}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// hclCoordinationNode reads a coordination_node block into the node it
// declares. A name YDB cannot give a node is refused first.
func hclCoordinationNode(claim hclext.Claim) (schemaext.Object, error) {
	if err := ydbcoordination.RefuseName(claim.Schema, claim.Name); err != nil {
		return schemaext.Object{}, err
	}
	values := make(map[string]string, len(claim.Attributes))
	for name, value := range claim.Attributes {
		values[name] = value.Text
	}
	spec, err := ydbcoordination.ParseDeclaration(values)
	if err != nil {
		return schemaext.Object{}, fmt.Errorf("%s %q: %v", coordinationBlock, claim.Name, err)
	}
	return ydbcoordination.DesiredObject(claim.Schema, claim.Name, "", spec), nil
}

// hclIndexFacets reads an index's vector settings and partitioning. An index
// whose type is YDB's vector index is a vector index even when it writes none
// of the settings; one of another type writing none gets no facet.
func hclIndexFacets(claim hclext.Claim) (schemaext.Facets, error) {
	vectorValues := attributeTexts(claim, ydbindex.VectorAttributes())
	vector, err := ydbindex.DeclareVector(vectorValues, claim.Index.Type, claim.Index.Operator)
	if err != nil {
		return schemaext.Facets{}, fmt.Errorf("%s %q: %v", indexBlock, claim.Name, err)
	}
	var facets schemaext.Facets
	if vector != nil {
		if facets, err = facets.With(vector); err != nil {
			return schemaext.Facets{}, fmt.Errorf("%s %q: %v", indexBlock, claim.Name, err)
		}
	}
	partitioning, err := ydbindex.ParseDeclaration(attributeTexts(claim, ydbpartition.Attributes()))
	if err != nil {
		return schemaext.Facets{}, fmt.Errorf("%s %q: %v", indexBlock, claim.Name, err)
	}
	if facets, err = ydbindex.WithPartitioning(facets, partitioning); err != nil {
		return schemaext.Facets{}, fmt.Errorf("%s %q: %v", indexBlock, claim.Name, err)
	}
	return facets, nil
}

// attributeTexts returns the texts of the named attributes the claim writes.
func attributeTexts(claim hclext.Claim, names []string) map[string]string {
	values := make(map[string]string)
	for _, name := range names {
		if value, written := claim.Attributes[name]; written {
			values[name] = value.Text
		}
	}
	return values
}

// hclClaim is the claim of an HCL document: the captured header's exact
// account, or the coordination nodes the format declares less the ones the
// header leaves unmanaged.
func hclClaim(request hclext.CoverageRequest) (schemaext.Coverage, error) {
	var limits Limits
	for _, limit := range request.Limits {
		if _, err := limits.ConsumeHCLDirective(limit); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	known, _, err := readHCLCoverage(request.Captured, limits)
	return known, err
}
