package embedengine_test

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedprovider"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// fakeSource answers keyset pages from a fixed list.
//
// It answers by CURSOR rather than by call count, so a test that resumes gets
// the page the cursor asks for rather than the next one in a script. A source
// driven by call count agrees with a broken cursor.
type fakeSource struct {
	rows     []embedgen.Row
	versions []string
	// scans counts the calls, so a test can assert a resumed run did not start
	// over.
	scans int
	// failAfter makes the source fail once that many scans have succeeded,
	// negative for never.
	failAfter int
	// stalled makes every scan answer from the beginning, which is the shape
	// of a source whose cursor does not work.
	stalled bool
	// emptyPagesBefore answers that many pages with no rows before the real
	// ones, which is what a filter matching nothing for a stretch looks like.
	emptyPagesBefore int
	// emptyPageOnScan answers that scan with no rows, zero for never. It is
	// separate from emptyPagesBefore so a test can put the empty page AFTER
	// the cursor has moved -- which is the only place "no position" and "the
	// position you gave me" are different answers.
	emptyPageOnScan int
	// emptyPageMode decides what position those pages report: "advanced" is a
	// well-behaved source, "same" hands back the cursor it was given, and
	// "none" reports no position at all.
	emptyPageMode string
}

// emptyPageCursor is the position an empty page reports.
func (f *fakeSource) emptyPageCursor(after []string) []string {
	switch f.emptyPageMode {
	case "same":
		return after
	case "none":
		return nil
	default:
		return []string{"0"}
	}
}

// Scan returns the rows after a cursor.
func (f *fakeSource) Scan(_ context.Context, after []string, limit int) (embedengine.Page, error) {
	f.scans++
	if f.failAfter >= 0 && f.scans > f.failAfter {
		return embedengine.Page{}, errors.New("the source connection dropped")
	}
	if f.emptyPagesBefore > 0 || f.emptyPageOnScan == f.scans {
		if f.emptyPagesBefore > 0 {
			f.emptyPagesBefore--
		}
		// A cursor of its own, because a page reporting no rows AND no position
		// has told the caller nothing it can resume from.
		return embedengine.Page{Cursor: f.emptyPageCursor(after)}, nil
	}
	start := 0
	if len(after) > 0 && !f.stalled {
		start = indexAfter(f.rows, after[0])
	}
	end := min(start+limit, len(f.rows))
	return embedengine.Page{
		Rows:     f.rows[start:end],
		Versions: sliceVersions(f.versions, start, end),
		Cursor:   cursorAt(f.rows, end),
		Done:     end == len(f.rows),
	}, nil
}

// indexAfter finds the position after a key.
func indexAfter(rows []embedgen.Row, key string) int {
	for index, row := range rows {
		if row.Key[0] == key {
			return index + 1
		}
	}
	return 0
}

// sliceVersions returns the versions for a page.
func sliceVersions(versions []string, start, end int) []string {
	if start >= len(versions) {
		return nil
	}
	return versions[start:min(end, len(versions))]
}

// cursorAt returns the key at a position, or nothing past the end.
func cursorAt(rows []embedgen.Row, index int) []string {
	if index == 0 || index > len(rows) {
		return nil
	}
	return rows[index-1].Key
}

// fakeProvider answers with a vector derived from the input, so a test can tell
// which text produced which vector.
type fakeProvider struct {
	dimension int
	// calls records every batch of inputs it was asked about, in order, which
	// is what makes "the skipped row was not sent" assertable.
	calls [][]string
	// failOn makes the provider fail on that call number, zero for never.
	failOn int
	// shortBy drops that many vectors from the answer, which is the shape of a
	// provider that silently returned fewer than it was asked for.
	shortBy int
	// beforeEmbed runs at the start of a call, so a test can interrupt the run
	// while a provider request is the thing in flight.
	beforeEmbed func()
	// reportsUsageOn are the call numbers whose answer carries a usage object.
	// Nil means none of them, which is this fake's historical behavior and what
	// every test written before it stayed on.
	//
	// It exists because a page can now be several provider requests: the
	// distinction between a provider that reported zero and one that reported
	// nothing is per REQUEST, and collapsing several into one answer needs a
	// test that can make them differ.
	reportsUsageOn []int
}

// Profile describes the endpoint.
func (f *fakeProvider) Profile() embedprovider.Profile {
	return embedprovider.Profile{Provider: "fake", Model: "fake-model", Dimension: f.dimension}
}

// Embed answers one vector per input.
func (f *fakeProvider) Embed(ctx context.Context, inputs []string) (embedprovider.Result, error) {
	if f.beforeEmbed != nil {
		f.beforeEmbed()
	}
	f.calls = append(f.calls, append([]string(nil), inputs...))
	if err := ctx.Err(); err != nil {
		return embedprovider.Result{}, err
	}
	if f.failOn == len(f.calls) {
		return embedprovider.Result{}, errors.New("the provider returned 503")
	}
	vectors := make([]embedprovider.Vector, 0, len(inputs))
	for _, input := range inputs {
		vector := make(embedprovider.Vector, f.dimension)
		for component := range vector {
			vector[component] = float32(len(input) + component)
		}
		vectors = append(vectors, vector)
	}
	if f.shortBy > 0 && len(vectors) >= f.shortBy {
		vectors = vectors[:len(vectors)-f.shortBy]
	}
	// An answer that carried no usage object leaves BOTH counts at zero, which
	// is what an adapter produces: openaicompatible fills Usage only inside
	// `if decoded.Usage != nil`. Reporting counts beside Reported=false would
	// let a test assert token totals in a state no provider can produce
	// (stokaro/ptah#2740 review).
	if !slices.Contains(f.reportsUsageOn, len(f.calls)) {
		return embedprovider.Result{Vectors: vectors}, nil
	}
	return embedprovider.Result{
		Vectors: vectors,
		Usage: embedprovider.Usage{
			PromptTokens: len(inputs), TotalTokens: len(inputs) * 2, Reported: true,
		},
	}, nil
}

// fakeTarget records what was committed, and can refuse.
//
// It writes the run through the store inside the same call, which is what the
// interface promises: one transaction. A fake that recorded the writes and left
// the run to somebody else would let a test pass over an implementation that
// wrote them separately -- and separately is the failure the interface exists
// to make unwritable.
type fakeTarget struct {
	store *embedstore.Memory
	// commits are the transactions that landed.
	commits []commit
	// failOn makes the commit fail on that call number, zero for never.
	failOn int
	// beforeCommit runs just before the write, so a test can move the world
	// underneath one.
	beforeCommit func()
	// afterCommit runs just after a write that landed, which is the only place
	// a test can reach the window between the last checkpoint and the
	// bookkeeping write that follows the walk.
	afterCommit func()
}

// commit is one transaction's contents.
type commit struct {
	writes []embedrun.TargetWrite
	cursor []string
	rows   int64
}

// Commit writes a batch and its checkpoint.
func (f *fakeTarget) Commit(ctx context.Context, writes []embedrun.TargetWrite, run embedrun.Run) error {
	if f.beforeCommit != nil {
		f.beforeCommit()
	}
	if f.failOn == len(f.commits)+1 {
		return fmt.Errorf("the target rejected the write")
	}
	// The store's own fencing check is the transaction's: a refused save means
	// nothing in this call landed.
	if err := f.store.SaveRun(ctx, run); err != nil {
		return err
	}
	f.commits = append(f.commits, commit{
		writes: append([]embedrun.TargetWrite(nil), writes...),
		cursor: append([]string(nil), run.Cursor...),
		rows:   run.Progress.RowsEmbedded,
	})
	if f.afterCommit != nil {
		f.afterCommit()
	}
	return nil
}
