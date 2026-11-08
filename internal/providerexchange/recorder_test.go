package providerexchange_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"ptah.run/internal/providerexchange"
)

func names(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.Name())
	}
	return result
}

func readMessage(c *qt.C, path string, message proto.Message) {
	c.Helper()
	encoded, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(protojson.Unmarshal(encoded, message), qt.IsNil)
}

// A recorder creates its directory and writes each exchange under the next
// number: the request and the reply as the contract's JSON, which read back as
// the messages they record, or the request and the call's error.
func TestRecorder_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "exchanges")
	recorder, err := providerexchange.New(dir)
	c.Assert(err, qt.IsNil)
	request := &protocol.CompareModelsRequest{Engine: "ydb", ComparisonId: "ydb-transfers", DatabasePath: "/local"}
	reply := &protocol.ComparedModels{Complete: true}

	c.Assert(recorder.Record("CompareModels", "ydb-transfers", request, reply, nil), qt.IsNil)
	c.Assert(recorder.Record("CompareModels", "Odd/Label Ü", request, nil, errors.New("refused: the topic is missing")), qt.IsNil)

	c.Assert(names(c, dir), qt.DeepEquals, []string{
		"0001-comparemodels-ydb-transfers.reply.json",
		"0001-comparemodels-ydb-transfers.request.json",
		"0002-comparemodels-odd-label--.error.txt",
		"0002-comparemodels-odd-label--.request.json",
	})
	recorded := &protocol.CompareModelsRequest{}
	readMessage(c, filepath.Join(dir, "0001-comparemodels-ydb-transfers.request.json"), recorded)
	c.Assert(proto.Equal(recorded, request), qt.IsTrue)
	answered := &protocol.ComparedModels{}
	readMessage(c, filepath.Join(dir, "0001-comparemodels-ydb-transfers.reply.json"), answered)
	c.Assert(proto.Equal(answered, reply), qt.IsTrue)
	text, err := os.ReadFile(filepath.Join(dir, "0002-comparemodels-odd-label--.error.txt"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(text), qt.Equals, "refused: the topic is missing\n")
}

// Exchanges recorded at once each take their own number, so none overwrites
// another.
func TestRecorder_HappyPath_ConcurrentExchangesKeepTheirOwnFiles(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "exchanges")
	recorder, err := providerexchange.New(dir)
	c.Assert(err, qt.IsNil)
	const calls = 16
	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() {
			errs[i] = recorder.Record("CompareModels", "fixture", &protocol.CompareModelsRequest{Engine: "fixture"}, &protocol.ComparedModels{}, nil)
		})
	}

	wg.Wait()

	c.Assert(errors.Join(errs...), qt.IsNil)
	want := make([]string, 0, 2*calls)
	for i := range calls {
		want = append(want, fmt.Sprintf("%04d-comparemodels-fixture.reply.json", i+1), fmt.Sprintf("%04d-comparemodels-fixture.request.json", i+1))
	}
	slices.Sort(want)
	c.Assert(names(c, dir), qt.DeepEquals, want)
}

// A nil recorder records nothing, which is what a context without one hands
// the host.
func TestRecorder_HappyPath_NilRecordsNothing(t *testing.T) {
	c := qt.New(t)

	recorder := providerexchange.FromContext(context.Background())

	c.Assert(recorder, qt.IsNil)
	c.Assert(recorder.Record("CompareModels", "fixture", &protocol.CompareModelsRequest{}, &protocol.ComparedModels{}, nil), qt.IsNil)
}

// The recorder a context carries is the one With put there.
func TestWith_HappyPath(t *testing.T) {
	c := qt.New(t)
	recorder, err := providerexchange.New(filepath.Join(c.TempDir(), "exchanges"))
	c.Assert(err, qt.IsNil)

	got := providerexchange.FromContext(providerexchange.With(c.Context(), recorder))

	c.Assert(got, qt.Equals, recorder)
}

// A directory that holds anything is refused, so two runs never share one; so
// is a path that names a file, and an empty name.
func TestNew_FailurePath(t *testing.T) {
	t.Run("a directory that holds a file", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(dir, "0001-comparemodels-x.request.json"), []byte("{}"), 0o600), qt.IsNil)

		recorder, err := providerexchange.New(dir)

		c.Assert(err, qt.ErrorIs, providerexchange.ErrDirectoryNotEmpty)
		c.Assert(recorder, qt.IsNil)
	})
	t.Run("a file", func(t *testing.T) {
		c := qt.New(t)
		path := filepath.Join(c.TempDir(), "file")
		c.Assert(os.WriteFile(path, nil, 0o600), qt.IsNil)

		recorder, err := providerexchange.New(path)

		c.Assert(err, qt.ErrorMatches, `(?s)create the provider exchange directory: .*`)
		c.Assert(recorder, qt.IsNil)
	})
	t.Run("an empty name", func(t *testing.T) {
		c := qt.New(t)

		recorder, err := providerexchange.New(" ")

		c.Assert(err, qt.ErrorMatches, `the provider exchange directory is empty`)
		c.Assert(recorder, qt.IsNil)
	})
}
