package embedverify_test

import (
	"fmt"
	"testing"

	"go.5x5.cz/ptah/internal/embedverify"
)

// BenchmarkVerify_LargeCorpus measures what a verification costs at corpus
// scale.
//
// Two numbers matter and only one of them is time. Verification is the one
// place in the lifecycle where the work could be proportional to the corpus
// rather than to a batch: the backfill beside it keeps memory flat however many
// rows there are, because a keyset scan bounds every batch. So this measures
// the memory as well, and it measures it over a GENERATED corpus rather than a
// materialized one -- a fixture built as two slices costs hundreds of megabytes
// by itself, and a benchmark that allocated it inside the timer would report
// its own fixture and call it the cost of verifying (stokaro/ptah#2621).
//
// The row counts are a ladder rather than one size, because the question is
// whether the cost is flat and a single point cannot answer that.
func BenchmarkVerify_LargeCorpus(b *testing.B) {
	for _, rows := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("%d rows", rows), func(b *testing.B) {
			corpus := generatedCorpus(rows, 0)
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
				report, err := embedverify.Verify(expectation, structure, corpus, state)
				if err != nil {
					b.Fatalf("the fixture walk cannot fail: %v", err)
				}
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
	corpus := generatedCorpus(rows, 10)
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
		report, err := embedverify.Verify(expectation, structure, corpus, state)
		if err != nil {
			b.Fatalf("the fixture walk cannot fail: %v", err)
		}
		if report.Passed() {
			b.Fatal("a tenth of the corpus is stale and the report says nothing")
		}
	}
}

// generatedCorpus walks a corpus of a chosen size.
//
// `staleEvery` marks one row in that many as computed from source text that has
// moved, zero for a corpus that agrees throughout.
//
// The per-row strings are built once, before the walk, so that what the
// benchmark reports is the cost of a row passing THROUGH Verify rather than the
// cost of producing it: three `fmt.Sprintf` calls per row inside the timer
// added eight million allocations and a quarter of a gigabyte, which is the
// fixture measuring itself. They differ per row rather than being shared,
// because a fixture of one repeated string exercises one key's worth of work
// and says nothing about a corpus.
//
// The two rows the walk points at are reused across positions, which
// `embedverify.Pair` allows: Verify copies what it needs before asking for the
// next one. That is the property being measured -- a corpus that has to
// allocate a pair per row is a corpus that cannot be walked in constant memory.
func generatedCorpus(rows, staleEvery int) embedverify.Corpus {
	keys := make([]string, rows)
	hashes := make([]string, rows)
	versions := make([]string, rows)
	for index := range rows {
		keys[index] = fmt.Sprintf("row-%09d", index)
		hashes[index] = fmt.Sprintf("%064x", index)
		versions[index] = fmt.Sprintf("%d", index)
	}
	return func(yield func(embedverify.Pair, error) bool) {
		var source embedverify.SourceRow
		var target embedverify.TargetRow
		pair := embedverify.Pair{Source: &source, Target: &target}
		for index := range rows {
			source = embedverify.SourceRow{
				Key: keys[index], Version: versions[index], InputHash: hashes[index],
			}
			target = embedverify.TargetRow{
				Key: keys[index], Generation: "gen-1", Version: versions[index],
				InputHash: hashes[index], Dimension: 768,
			}
			if staleEvery > 0 && index%staleEvery == 0 {
				target.InputHash = "stale"
			}
			if !yield(pair, nil) {
				return
			}
		}
	}
}
