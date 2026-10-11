package tssource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// HCL is the TimescaleDB owner's extension of the HCL frontend: a
// `hypertable` block, which sets a hypertable on the table its labels name,
// and a `continuous_aggregate` block, which declares an aggregate.
//
// Both are Ptah extensions of the format. Their claim is that every HCL
// document describes the hypertables and the continuous aggregates of the
// database, so one without such a block describes a database without one.
// The writer writes each back as the same block.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: tsschema.Owner,
		Kinds: []schemaext.Kind{tsschema.HypertableKind, tsschema.ContinuousAggregateKind},
		Blocks: []hclext.Block{
			{Site: hclext.TopLevel, Type: hypertableBlock, Naming: hclext.SchemaAndName, Attributes: []hclext.Attribute{
				{Name: "column", Kind: hclext.String}, {Name: "chunk_interval", Kind: hclext.String},
				{Name: "if_not_exists", Kind: hclext.Bool}, {Name: "comment", Kind: hclext.Text},
			}},
			{Site: hclext.TopLevel, Type: aggregateBlock, Naming: hclext.SchemaAndQualifiedName, Attributes: []hclext.Attribute{
				{Name: "as", Kind: hclext.String}, {Name: "materialized_only", Kind: hclext.Bool}, {Name: "comment", Kind: hclext.Text},
			}},
		},
		Decoder: hclDecoder{},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) {
			return tsschema.CompleteCoverage(schemaext.Desired)
		},
		Encoder: hclEncoder{},
	}
}

const (
	hypertableBlock = "hypertable"
	aggregateBlock  = "continuous_aggregate"
)

// hclDecoder reads the owner's blocks of one document.
type hclDecoder struct{}

// DecodeHCL reads each block in the document's order. A hypertable needs a
// column to partition on, and a continuous aggregate is its SELECT, so a
// block without either is refused; a second aggregate of one name is refused
// too.
func (hclDecoder) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	var aggregates schemaext.Objects
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		if claim.Block == hypertableBlock {
			hypertable, err := hclHypertable(claim)
			if err != nil {
				return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
			}
			declarations = append(declarations, hclext.Declaration{TableSetting: hypertable})
			continue
		}
		object, err := hclAggregate(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		if aggregates, err = aggregates.With(object); err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: fmt.Errorf("%s %q: %v", aggregateBlock, claim.Name, err)}
		}
		declarations = append(declarations, hclext.Declaration{Objects: []schemaext.Object{object}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// hclHypertable reads a hypertable block into the hypertable settings of the
// table it names.
//
// The block's label is the TABLE it partitions, not a name of its own: a
// hypertable has no name, and `timescaledb_information.hypertables` is keyed
// by the relation. The frontend attaches the settings to that table once the
// load is read, because the table block may come later. `column` is required
// for the same reason a synonym's target is: a hypertable with no dimension is
// not one, and the server has no default to fall back on.
//
// `chunk_interval` is optional and is kept as the string it was written as.
// An omitted interval takes TimescaleDB's own default, which the catalog then
// reports; converting the declaration to compare would differ from the
// server's spelling on every run.
func hclHypertable(claim hclext.Claim) (*tsschema.DesiredHypertable, error) {
	column := claim.Attributes["column"].Text
	if strings.TrimSpace(column) == "" {
		return nil, fmt.Errorf("%s %q requires a column to partition on", hypertableBlock, claim.Name)
	}
	return &tsschema.DesiredHypertable{
		Column:        column,
		ChunkInterval: claim.Attributes["chunk_interval"].Text,
		IfNotExists:   claim.Attributes["if_not_exists"].Bool,
		Comment:       claim.Attributes["comment"].Text,
	}, nil
}

// hclAggregate reads a continuous_aggregate block into a continuous
// aggregate.
//
// Unlike a hypertable this object has a name of its own, so the block's label
// is that name and `schema` says where it lives -- the shape a view block has.
// `as` is required: a continuous aggregate is its SELECT, and there is no
// default body.
func hclAggregate(claim hclext.Claim) (schemaext.Object, error) {
	body := claim.Attributes["as"].Text
	if strings.TrimSpace(body) == "" {
		return schemaext.Object{}, fmt.Errorf("%s %q requires an `as` body; a continuous aggregate is its SELECT", aggregateBlock, claim.Name)
	}
	var materializedOnly *bool
	if value, written := claim.Attributes["materialized_only"]; written {
		materializedOnly = new(value.Bool)
	}
	return tsschema.DesiredContinuousAggregateObject(claim.Schema, claim.Name, tsschema.DesiredContinuousAggregate{
		Body:             body,
		MaterializedOnly: materializedOnly,
		Comment:          claim.Attributes["comment"].Text,
	}), nil
}
