package schemaext

import (
	"context"
	"fmt"
)

// ConversionRequest asks a selected target to interpret an ordered batch in
// the other schema representation. This is a semantic operation: a desired
// default need not equal an observed zero value. Services must preserve order
// and kind, return no partial result on failure, and leave inputs untouched.
type ConversionRequest struct {
	Target string
	From   Representation
	To     Representation
	Values []Value
}

// ConversionService converts a complete batch using target semantics. A local
// implementation and a process adapter obey the same cancellation and error
// contract. Clone and codec functions remain pure local operations.
type ConversionService interface {
	ConvertFeatures(context.Context, ConversionRequest) ([]Value, error)
}

// ConversionRuntime supplies selected semantic services and their local model
// codecs. Schema conversion accepts this narrow contract without importing
// concrete providers or the runtime's composition package.
type ConversionRuntime interface {
	ConversionService
	ModelRuntime
}

// SnapshotValues validates the selected concrete representation and clones an
// ordered batch without encoding it. No codec or service may retain the inputs.
func (r Registry) SnapshotValues(ctx context.Context, representation Representation, values []Value) ([]Value, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	if err := schemaRepresentation(representation); err != nil {
		return nil, err
	}
	result := make([]Value, 0, len(values))
	for _, value := range values {
		if err := codecContext(ctx); err != nil {
			return nil, err
		}
		if err := ValidatePayload(value); err != nil {
			return nil, err
		}
		codec, found := r.codecs[codecKey{kind: value.Kind(), representation: representation}]
		if !found {
			return nil, &UnknownCodecError{Kind: value.Kind(), Representation: representation}
		}
		cloned, err := codec.snapshot(value)
		if err != nil {
			return nil, err
		}
		typed, ok := cloned.(Value)
		if !ok {
			return nil, fmt.Errorf("%w: schema codec returned a non-value", ErrInvalidValue)
		}
		result = append(result, typed)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// ConvertCoverage carries source knowledge into another representation without
// enrolling additional kinds. It validates the recorded definitions before
// substituting the selected destination definitions. Unknown observations stay
// unknown; conversion is not inspection. A default request projects as an
// unknown observation for the same reason: what the owner's default holds is
// not in the declaration, and no projection can say it.
func (r Registry) ConvertCoverage(ctx context.Context, from, to Representation, source Coverage) (Coverage, error) {
	document, err := r.EncodeCoverage(ctx, from, source)
	if err != nil {
		return Coverage{}, err
	}
	if err := schemaRepresentation(to); err != nil {
		return Coverage{}, err
	}
	for i, record := range document.Kinds {
		codec, found := r.codecs[codecKey{kind: record.Model.Kind, representation: to}]
		if !found {
			return Coverage{}, &UnknownCodecError{Kind: record.Model.Kind, Representation: to}
		}
		if codec.owner != record.Model.Owner {
			return Coverage{}, fmt.Errorf("%w: conversion changes model owner", ErrIncompatibleCodec)
		}
		document.Kinds[i].Model = CodecIdentity{Owner: codec.owner, Kind: record.Model.Kind, Representation: to, Version: codec.version, Definition: codec.definition}
	}
	if to == Observed {
		for i, record := range document.Subjects {
			if record.Knowledge.State == Defaulted {
				document.Subjects[i].Knowledge = Knowledge{State: Uninspected,
					Reason: "the declaration requests the owner's default, which a projection cannot observe"}
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return Coverage{}, err
	}
	return NewCoverage(to, document.Kinds, document.Subjects)
}
