package embedverify_test

import (
	"fmt"
	"testing"

	"go.5x5.cz/ptah/internal/embedverify"
)

// BenchmarkVerify_LargeCorpus measures what a verification costs at corpus
// scale.
//
// Two numbers matter and only one of them is time. Verification takes the whole
// source and the whole target as slices, so its memory is linear in the corpus
// -- which is the opposite of the backfill beside it, where a keyset scan and a
// bounded batch keep memory flat however many rows there are. A million-row
// corpus is where the difference stops being theoretical, so this measures it
// rather than leaving it to be discovered on somebody's production table.
//
// The row counts are a ladder rather than one size, because the question is
// whether the cost is linear and a single point cannot answer that.
func BenchmarkVerify_LargeCorpus(b *testing.B) {
	for _, rows := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("%d rows", rows), func(b *testing.B) {
			source, target := corpus(rows)
			expectation := embedverify.Expectation{
				Generation: "gen-1", ColumnType: "vector(768)", Dimension: 768,
			}
			structure := embedverify.Structure{
				ColumnExists: true, ColumnType: "vector(768)", Dimension: 768,
				ExtensionPresent: true,
			}
			state := embedverify.RunState{
				SnapshotComplete: true, CatchUpReached: true,
				ConsistencyMode: "outbox", SourceMutable: true,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				report := embedverify.Verify(expectation, structure, source, target, state)
				if !report.Passed() {
					b.Fatalf("the fixture should verify cleanly: %v", report.Findings)
				}
			}
		})
	}
}

// BenchmarkVerify_LargeCorpusWithFindings is the same corpus with a tenth of it
// stale.
//
// Findings carry bounded key lists, so a corpus where everything is wrong must
// not cost more than one where nothing is: a report that grew with the number of
// bad rows would turn the worst case into the one nobody can read.
func BenchmarkVerify_LargeCorpusWithFindings(b *testing.B) {
	const rows = 1_000_000
	source, target := corpus(rows)
	for index := range rows / 10 {
		target[index].InputHash = "stale"
	}
	expectation := embedverify.Expectation{
		Generation: "gen-1", ColumnType: "vector(768)", Dimension: 768,
	}
	structure := embedverify.Structure{
		ColumnExists: true, ColumnType: "vector(768)", Dimension: 768,
		ExtensionPresent: true,
	}
	state := embedverify.RunState{
		SnapshotComplete: true, CatchUpReached: true,
		ConsistencyMode: "outbox", SourceMutable: true,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		report := embedverify.Verify(expectation, structure, source, target, state)
		if report.Passed() {
			b.Fatal("a tenth of the corpus is stale and the report says nothing")
		}
	}
}

// corpus builds a source and a target that agree, at a chosen size.
//
// The strings differ per row rather than being shared, because a fixture of one
// repeated string measures a map of one key and says nothing about a corpus.
func corpus(rows int) ([]embedverify.SourceRow, []embedverify.TargetRow) {
	source := make([]embedverify.SourceRow, 0, rows)
	target := make([]embedverify.TargetRow, 0, rows)
	for index := range rows {
		key := fmt.Sprintf("row-%09d", index)
		hash := fmt.Sprintf("%064x", index)
		version := fmt.Sprintf("%d", index)
		source = append(source, embedverify.SourceRow{
			Key: key, Version: version, InputHash: hash,
		})
		target = append(target, embedverify.TargetRow{
			Key: key, Generation: "gen-1", Version: version, InputHash: hash,
			Dimension: 768,
		})
	}
	return source, target
}
