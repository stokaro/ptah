package pgtablesource

import (
	"context"
	"fmt"

	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

// HCL reads and writes table persistence and partition keys. The frontend
// evaluates HCL expressions; the owner receives typed values and returns facets.
func HCL() hclext.Extension {
	return hclext.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.PersistenceKind, pgtable.PartitionKind},
		Blocks:  []hclext.Block{{Type: "table", Site: hclext.TableAttributes, Attributes: []hclext.Attribute{{Name: "unlogged", Kind: hclext.Bool}}}, partitionBlock()},
		Decoder: hclCodec{}, Encoder: hclCodec{}, Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) { return Coverage() }}
}

type hclCodec struct{}

func (hclCodec) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	if ctx == nil {
		return hclext.Result{}, fmt.Errorf("%w: table persistence HCL requires a context", schemaext.ErrInvalidValue)
	}
	result := hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		facets, err := hclPersistence(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		result.Declarations[i].Facets = facets
	}
	if err := ctx.Err(); err != nil {
		return hclext.Result{}, err
	}
	return result, nil
}

func hclPersistence(claim hclext.Claim) (schemaext.Facets, error) {
	if claim.Block == "partition" {
		return hclPartition(claim)
	}
	if claim.Block != "table" || claim.Table == nil {
		return schemaext.Facets{}, fmt.Errorf("%w: persistence requires a table claim", schemaext.ErrInvalidValue)
	}
	for name, value := range claim.Attributes {
		if name != "unlogged" || value.Kind != hclext.Bool {
			return schemaext.Facets{}, fmt.Errorf("%w: table persistence requires a boolean unlogged attribute, got %q", schemaext.ErrInvalidValue, name)
		}
	}
	value, present := claim.Attributes["unlogged"]
	if !present {
		return schemaext.Facets{}, nil
	}
	return schemaext.NewFacets(&pgtable.DesiredPersistence{Unlogged: value.Bool})
}

func (hclCodec) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if ctx == nil {
		return hclext.EncodeResult{}, fmt.Errorf("%w: table persistence HCL requires a context", schemaext.ErrInvalidValue)
	}
	if err := exportCoverage(request.Coverage); err != nil {
		return hclext.EncodeResult{}, err
	}
	result := hclext.EncodeResult{}
	for _, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		partition, err := encodePartition(table)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		if partition != nil {
			result.Blocks = append(result.Blocks, *partition)
		}
		value, found, err := pgtable.PersistenceOf(table.Facets)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		if !found {
			continue
		}
		result.Attributes = append(result.Attributes, hclext.SlotAttributes{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table.Table},
			Attributes: []hclext.EncodedAttribute{{Name: "unlogged", Value: hclext.Value{Kind: hclext.Bool, Bool: value.Unlogged}}}})
	}
	if err := ctx.Err(); err != nil {
		return hclext.EncodeResult{}, err
	}
	return result, nil
}

func exportCoverage(coverage schemaext.Coverage) error {
	for _, record := range coverage.SubjectRecords() {
		if (record.Kind == pgtable.PersistenceKind || record.Kind == pgtable.PartitionKind) && (record.Knowledge.State == schemaext.Uninspected || record.Knowledge.State == schemaext.Unrepresentable) {
			return fmt.Errorf("%w: table %s %s cannot be exported without losing its coverage: %s", ptaherr.ErrUnsupportedFeature, record.Subject, record.Kind, record.Knowledge.Reason)
		}
	}
	for _, record := range coverage.KindRecords() {
		if (record.Model.Kind == pgtable.PersistenceKind || record.Model.Kind == pgtable.PartitionKind) && record.Knowledge.State == schemaext.Unrepresentable {
			return fmt.Errorf("%w: table %s cannot be exported without losing its coverage: %s", ptaherr.ErrUnsupportedFeature, record.Model.Kind, record.Knowledge.Reason)
		}
	}
	return nil
}
