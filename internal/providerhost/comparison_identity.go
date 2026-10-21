package providerhost

import (
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
)

func comparisonPart(part objectidentity.Part) *protocol.ComparisonPart {
	return &protocol.ComparisonPart{Source: part.Source, Normalized: part.Normalized, Quoted: part.Quoted, Defaulted: part.Defaulted}
}

func boundPart(part *protocol.ComparisonPart) objectidentity.Part {
	return objectidentity.Part{Source: part.GetSource(), Normalized: part.GetNormalized(), Quoted: part.GetQuoted(), Defaulted: part.GetDefaulted()}
}

func comparisonParts(parts objectidentity.Parts) []*protocol.ComparisonPart {
	result := make([]*protocol.ComparisonPart, 0, len(parts.Values()))
	for _, part := range parts.Values() {
		result = append(result, comparisonPart(part))
	}
	return result
}

func boundParts(parts []*protocol.ComparisonPart) objectidentity.Parts {
	result := make([]objectidentity.Part, len(parts))
	for i, part := range parts {
		result[i] = boundPart(part)
	}
	return objectidentity.NewParts(result...)
}

func comparisonIdentity(subject objectidentity.ID) *protocol.ComparisonIdentity {
	return &protocol.ComparisonIdentity{Kind: string(subject.Kind),
		Database:  comparisonPart(subject.Database),
		Catalog:   comparisonPart(subject.Catalog),
		Schema:    comparisonPart(subject.Schema),
		Path:      comparisonParts(subject.Namespace),
		Parent:    comparisonPart(subject.Parent),
		Name:      comparisonPart(subject.Name),
		Signature: comparisonParts(subject.Signature)}
}

func boundIdentity(subject *protocol.ComparisonIdentity) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(subject.GetKind()),
		Database:  boundPart(subject.GetDatabase()),
		Catalog:   boundPart(subject.GetCatalog()),
		Schema:    boundPart(subject.GetSchema()),
		Namespace: boundParts(subject.GetPath()),
		Parent:    boundPart(subject.GetParent()),
		Name:      boundPart(subject.GetName()),
		Signature: boundParts(subject.GetSignature())}
}

func comparisonIdentities(subjects []objectidentity.ID) []*protocol.ComparisonIdentity {
	result := make([]*protocol.ComparisonIdentity, len(subjects))
	for i, subject := range subjects {
		result[i] = comparisonIdentity(subject)
	}
	return result
}
