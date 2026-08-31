package embedgen_test

import (
	"strings"
	"testing"

	"go.5x5.cz/ptah/internal/embedgen"
)

// BenchmarkCanonicalizeAndHash_PerRow measures what one row costs before it
// reaches the provider.
//
// This runs once per source row, twice: the backfill canonicalizes to build the
// input and the verification canonicalizes again to decide whether the stored
// hash still matches. Over a corpus of several million rows it is the only
// per-row work in the loop that is not the provider, so it is the number that
// says whether a backfill is bounded by Ptah or by the endpoint.
//
// A realistic document rather than a short string: normalization and whitespace
// collapse both walk the text, so a fixture of ten characters measures the call
// overhead and nothing else.
func BenchmarkCanonicalizeAndHash_PerRow(b *testing.B) {
	spec := baseSpec()
	row := documentRow(4_000)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		input, err := spec.Canonicalize(row)
		if err != nil {
			b.Fatal(err)
		}
		if spec.SourceInputHash(input) == "" {
			b.Fatal("the hash is empty")
		}
	}
}

// BenchmarkCanonicalizeAndHash_ByInputSize is the same question against the
// size of what is being embedded.
//
// The ladder is what shows whether the cost is linear in the text: the epic's
// specification bounds an input at a number of BYTES, and an operator choosing
// that bound is choosing this.
func BenchmarkCanonicalizeAndHash_ByInputSize(b *testing.B) {
	spec := baseSpec()
	for _, size := range []int{200, 2_000, 7_000} {
		b.Run(sizeName(size), func(b *testing.B) {
			row := documentRow(size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				input, err := spec.Canonicalize(row)
				if err != nil {
					b.Fatal(err)
				}
				_ = spec.SourceInputHash(input)
			}
		})
	}
}

// documentRow builds a row whose two fields together are about size bytes.
//
// The text carries runs of whitespace and a non-ASCII character, because both
// preprocessing steps the specification enables -- NFC normalization and
// whitespace collapse -- do nothing measurable over text that needs neither.
func documentRow(size int) embedgen.Row {
	unit := "the quick  brown foxé jumps over\tthe lazy dog. "
	body := strings.Repeat(unit, max(1, size/len(unit)))
	title := "A document of about " + sizeName(size)
	return embedgen.Row{Key: []string{"1"}, Fields: []*string{&title, &body}}
}

// sizeName names a benchmark case.
func sizeName(size int) string {
	switch {
	case size >= 1000:
		return itoa(size/1000) + "kB"
	default:
		return itoa(size) + "B"
	}
}

// itoa renders a small count without pulling in a formatter.
func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}
