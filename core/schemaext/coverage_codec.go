package schemaext

import (
	"context"
	"fmt"
)

// CoverageDocument is the versioned, concrete wire form of source knowledge.
// Model identities bind even empty object namespaces to their recorded meanings.
type CoverageDocument struct {
	Format         uint32            `json:"format"`
	Representation Representation    `json:"representation"`
	Kinds          []KindCoverage    `json:"kinds"`
	Subjects       []SubjectCoverage `json:"subjects,omitempty"`
}

// EncodeCoverage validates every enrolled model without adding models from the
// runtime. A zero Coverage serializes an empty account, never complete coverage.
func (r Registry) EncodeCoverage(ctx context.Context, representation Representation, coverage Coverage) (CoverageDocument, error) {
	if err := codecContext(ctx); err != nil {
		return CoverageDocument{}, err
	}
	if err := schemaRepresentation(representation); err != nil {
		return CoverageDocument{}, err
	}
	if coverage.Representation() != "" && coverage.Representation() != representation {
		return CoverageDocument{}, fmt.Errorf("%w: coverage direction differs from its document", ErrInvalidValue)
	}
	for _, record := range coverage.KindRecords() {
		if err := r.requireModel(record.Model); err != nil {
			return CoverageDocument{}, err
		}
	}
	return CoverageDocument{Format: envelopeFormat, Representation: representation,
		Kinds: coverage.KindRecords(), Subjects: coverage.SubjectRecords()}, nil
}

// DecodeCoverage restores captured claims only when every recorded definition
// matches. Newly registered kinds remain unknown; incompatible definitions are
// errors rather than new authority over missing objects.
func (r Registry) DecodeCoverage(ctx context.Context, document CoverageDocument) (Coverage, error) {
	if err := codecContext(ctx); err != nil {
		return Coverage{}, err
	}
	if document.Format != envelopeFormat || document.Kinds == nil {
		return Coverage{}, fmt.Errorf("%w: invalid coverage document format or missing kinds", ErrIncompatibleCodec)
	}
	coverage, err := NewCoverage(document.Representation, document.Kinds, document.Subjects)
	if err != nil {
		return Coverage{}, err
	}
	for _, record := range document.Kinds {
		if err := r.requireModel(record.Model); err != nil {
			return Coverage{}, err
		}
	}
	return coverage, nil
}

func (r Registry) requireModel(model CodecIdentity) error {
	codec, found := r.codecs[codecKey{kind: model.Kind, representation: model.Representation}]
	if !found {
		return &UnknownCodecError{Kind: model.Kind, Representation: model.Representation}
	}
	if codec.owner != model.Owner || codec.version != model.Version || codec.definition != model.Definition {
		return fmt.Errorf("%w: coverage model %q/%s", ErrIncompatibleCodec, model.Kind, model.Representation)
	}
	return nil
}

func schemaRepresentation(representation Representation) error {
	if representation != Desired && representation != Observed {
		return fmt.Errorf("%w: schema data requires desired or observed representation", ErrInvalidValue)
	}
	return nil
}
