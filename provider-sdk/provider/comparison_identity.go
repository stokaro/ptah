package provider

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ValidateComparisonIdentity checks a bound identity without folding names or
// parsing delimiters. The selected owner remains responsible for its meaning.
func ValidateComparisonIdentity(ref *protocol.ComparisonIdentity) error {
	if ref == nil || strings.TrimSpace(ref.GetKind()) == "" || ref.GetName().GetSource() == "" {
		return fmt.Errorf("%w: comparison identity requires a kind and name", ErrProtocol)
	}
	for _, part := range []*protocol.ComparisonPart{ref.GetDatabase(), ref.GetCatalog(), ref.GetSchema(), ref.GetParent(), ref.GetName()} {
		if err := validateComparisonPart(part); err != nil {
			return err
		}
	}
	for _, parts := range [][]*protocol.ComparisonPart{ref.GetPath(), ref.GetSignature()} {
		for _, part := range parts {
			if part == nil || part.GetSource() == "" {
				return fmt.Errorf("%w: comparison path has an empty component", ErrProtocol)
			}
			if err := validateComparisonPart(part); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateComparisonPart(part *protocol.ComparisonPart) error {
	if part != nil && (!utf8.ValidString(part.GetSource()) || !utf8.ValidString(part.GetNormalized()) || (part.GetSource() == "") != (part.GetNormalized() == "")) {
		return fmt.Errorf("%w: comparison component lacks source or normalized spelling", ErrProtocol)
	}
	return nil
}

// CompareIdentities orders bound identity components by their normalized forms.
// Source spelling, quoting, and default markers do not change equality.
func CompareIdentities(a, b *protocol.ComparisonIdentity) int {
	for _, compared := range []int{
		strings.Compare(a.GetKind(), b.GetKind()),
		compareComparisonParts(a.GetDatabase(), b.GetDatabase()),
		compareComparisonParts(a.GetCatalog(), b.GetCatalog()),
		compareComparisonParts(a.GetSchema(), b.GetSchema()),
		slices.CompareFunc(a.GetPath(), b.GetPath(), compareComparisonParts),
		compareComparisonParts(a.GetParent(), b.GetParent()),
		compareComparisonParts(a.GetName(), b.GetName()),
		slices.CompareFunc(a.GetSignature(), b.GetSignature(), compareComparisonParts),
	} {
		if compared != 0 {
			return compared
		}
	}
	return 0
}

func compareComparisonParts(a, b *protocol.ComparisonPart) int {
	return strings.Compare(a.GetNormalized(), b.GetNormalized())
}

func validateComparisonIdentities(refs []*protocol.ComparisonIdentity) error {
	for _, ref := range refs {
		if err := ValidateComparisonIdentity(ref); err != nil {
			return err
		}
	}
	ordered := slices.Clone(refs)
	slices.SortFunc(ordered, CompareIdentities)
	for i := 1; i < len(ordered); i++ {
		if CompareIdentities(ordered[i-1], ordered[i]) == 0 {
			return fmt.Errorf("%w: duplicate comparison identity", ErrProtocol)
		}
	}
	return nil
}
