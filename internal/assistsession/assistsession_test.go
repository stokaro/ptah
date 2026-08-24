package assistsession_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistsession"
)

// clock returns a time source that advances one second per read, so records
// written in one test have distinguishable, deterministic stamps.
func clock(start time.Time) func() time.Time {
	current := start
	return func() time.Time {
		current = current.Add(time.Second)
		return current
	}
}

// newStore opens a store over a temporary project.
func newStore(c *qt.C) (*assistsession.Store, string) {
	c.Helper()
	root := c.TempDir()
	store, err := assistsession.Open(assistsession.Options{
		Root:  root,
		Clock: clock(time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)),
	})
	c.Assert(err, qt.IsNil)
	return store, root
}

// write puts one complete conversation in the store.
func write(c *qt.C, store *assistsession.Store, id, request, answer string) {
	c.Helper()
	writer, err := store.Create(id, assistsession.Record{
		PtahVersion: "test", ProjectRoot: "/project", Provider: "local", Model: "a-model",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(writer.Append(assistsession.Record{
		Kind: assistsession.KindRequest, Text: request,
	}), qt.IsNil)
	c.Assert(writer.Append(assistsession.Record{
		Kind: assistsession.KindTool, Tool: "read_artifact", Result: `{"entries":[]}`,
	}), qt.IsNil)
	c.Assert(writer.Append(assistsession.Record{
		Kind: assistsession.KindAnswer, Text: answer, Turns: 2,
		StopReason: "answer", Verified: true,
	}), qt.IsNil)
	c.Assert(writer.Close(), qt.IsNil)
}

func TestCreate_WritesAHeaderAndAppends(t *testing.T) {
	c := qt.New(t)
	store, root := newStore(c)

	write(c, store, "session-1", "what is here?", "one migration pair")

	raw, err := os.ReadFile(filepath.Join(root, ".ptah", "sessions", "session-1.jsonl"))
	c.Assert(err, qt.IsNil)
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	c.Assert(lines, qt.HasLen, 4)

	var header map[string]any
	c.Assert(json.Unmarshal([]byte(lines[0]), &header), qt.IsNil)
	c.Assert(header["type"], qt.Equals, "session")
	c.Assert(header["session_id"], qt.Equals, "session-1")
	c.Assert(header["schema_version"], qt.Equals, float64(assistsession.SchemaVersion))
	c.Assert(header["model"], qt.Equals, "a-model")
	c.Assert(header["at"], qt.Equals, "2026-08-24T12:00:01Z")
}

func TestList_IsMostRecentlyUpdatedFirst(t *testing.T) {
	c := qt.New(t)
	store, _ := newStore(c)
	write(c, store, "older", "first question", "first answer")
	write(c, store, "newer", "second question", "second answer")

	summaries, err := store.List()

	c.Assert(err, qt.IsNil)
	c.Assert(summaries, qt.HasLen, 2)
	c.Assert(summaries[0].ID, qt.Equals, "newer")
	c.Assert(summaries[1].ID, qt.Equals, "older")
	c.Assert(summaries[0].First, qt.Equals, "second question")
	c.Assert(summaries[0].Requests, qt.Equals, 1)
	c.Assert(summaries[0].ToolCalls, qt.Equals, 1)
	c.Assert(summaries[0].Model, qt.Equals, "a-model")
}

func TestList_AnEmptyStoreIsNotAnError(t *testing.T) {
	c := qt.New(t)
	store, _ := newStore(c)

	summaries, err := store.List()

	c.Assert(err, qt.IsNil)
	c.Assert(summaries, qt.HasLen, 0)
}

func TestList_OneUnreadableFileDoesNotHideTheRest(t *testing.T) {
	// A session written by a version this build does not understand is still
	// somebody's history, and it must not take the listing down with it.
	c := qt.New(t)
	store, root := newStore(c)
	write(c, store, "good", "a question", "an answer")
	c.Assert(os.WriteFile(
		filepath.Join(root, ".ptah", "sessions", "broken.jsonl"),
		[]byte("this is not JSON\n"), 0o600), qt.IsNil)

	summaries, err := store.List()

	c.Assert(err, qt.IsNil)
	c.Assert(summaries, qt.HasLen, 1)
	c.Assert(summaries[0].ID, qt.Equals, "good")
}

func TestRead_TolerantOfATruncatedFinalLine(t *testing.T) {
	// The shape a process that stopped mid-write leaves. Every complete record
	// before it is still what happened, and refusing the file would lose them.
	c := qt.New(t)
	store, root := newStore(c)
	write(c, store, "session-1", "a question", "an answer")
	path := filepath.Join(root, ".ptah", "sessions", "session-1.jsonl")
	raw, err := os.ReadFile(path) // #nosec G703 -- the path is this test's own temporary directory
	c.Assert(err, qt.IsNil)
	// #nosec G703 -- same path, same temporary directory
	c.Assert(os.WriteFile(path, append(raw, []byte(`{"type":"answer","te`)...), 0o600), qt.IsNil)

	records, err := store.Read("session-1")

	c.Assert(err, qt.IsNil)
	c.Assert(records, qt.HasLen, 4)
	c.Assert(records[3].Kind, qt.Equals, assistsession.KindAnswer)
}

func TestRead_AcceptsAUniquePrefix(t *testing.T) {
	c := qt.New(t)
	store, _ := newStore(c)
	write(c, store, "20260824T120000-aaaa", "a question", "an answer")

	records, err := store.Read("20260824T120000")

	c.Assert(err, qt.IsNil)
	c.Assert(records[0].SessionID, qt.Equals, "20260824T120000-aaaa")
}

func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantErr string
	}{
		{name: "nothing named", id: "", wantErr: `no such session: no session named`},
		{name: "no such session", id: "nope", wantErr: `no such session: "nope"`},
		{
			name:    "an ambiguous prefix",
			id:      "shared",
			wantErr: `"shared" names 2 sessions: shared-a, shared-b`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			store, _ := newStore(c)
			write(c, store, "shared-a", "a", "a")
			write(c, store, "shared-b", "b", "b")

			records, err := store.Read(test.id)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(records, qt.IsNil)
		})
	}
}

func TestDelete_RemovesOneSession(t *testing.T) {
	c := qt.New(t)
	store, _ := newStore(c)
	write(c, store, "keep", "a", "a")
	write(c, store, "remove", "b", "b")

	path, err := store.Delete("remove")

	c.Assert(err, qt.IsNil)
	c.Assert(path, qt.Contains, "remove.jsonl")
	summaries, err := store.List()
	c.Assert(err, qt.IsNil)
	c.Assert(summaries, qt.HasLen, 1)
	c.Assert(summaries[0].ID, qt.Equals, "keep")
}

func TestPrune_RemovesOnlyWhatIsOldEnough(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	old := time.Date(2026, time.June, 1, 12, 0, 0, 0, time.UTC)
	oldStore, err := assistsession.Open(assistsession.Options{Root: root, Clock: clock(old)})
	c.Assert(err, qt.IsNil)
	write(c, oldStore, "ancient", "a", "a")

	recent := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	recentStore, err := assistsession.Open(assistsession.Options{Root: root, Clock: clock(recent)})
	c.Assert(err, qt.IsNil)
	write(c, recentStore, "fresh", "b", "b")

	removed, err := recentStore.Prune(30 * 24 * time.Hour)

	c.Assert(err, qt.IsNil)
	c.Assert(removed, qt.DeepEquals, []string{"ancient"})
	summaries, err := recentStore.List()
	c.Assert(err, qt.IsNil)
	c.Assert(summaries, qt.HasLen, 1)
	c.Assert(summaries[0].ID, qt.Equals, "fresh")
}

func TestMessages_ReplayTheConversationAndNotTheToolResults(t *testing.T) {
	// The tool results described the project as it was. A resumed session that
	// fed them back as current would have the model reasoning about a directory
	// that may have changed; it re-reads instead, which costs a tool call and is
	// the answer that is still true.
	c := qt.New(t)
	records := []assistsession.Record{
		{Kind: assistsession.KindHeader, SessionID: "s"},
		{Kind: assistsession.KindRequest, Text: "what is here?"},
		{Kind: assistsession.KindTool, Tool: "read_artifact", Result: `{"entries":["a.sql"]}`},
		{Kind: assistsession.KindAnswer, Text: "one file, a.sql"},
	}

	messages := assistsession.Messages(records)

	c.Assert(messages, qt.HasLen, 2)
	c.Assert(messages[0].Role, qt.Equals, aiprovider.RoleUser)
	c.Assert(messages[0].Content, qt.Equals, "what is here?")
	c.Assert(messages[1].Role, qt.Equals, aiprovider.RoleAssistant)
	c.Assert(messages[1].Content, qt.Equals, "one file, a.sql")
}

func TestMessages_SkipAnAnswerThatWasNeverGiven(t *testing.T) {
	// A turn that ended at a limit has no answer text. Replaying it as an empty
	// assistant message would tell the model it once said nothing.
	c := qt.New(t)
	records := []assistsession.Record{
		{Kind: assistsession.KindRequest, Text: "a question"},
		{Kind: assistsession.KindAnswer, Text: "", StopReason: "tool call limit"},
	}

	messages := assistsession.Messages(records)

	c.Assert(messages, qt.HasLen, 1)
	c.Assert(messages[0].Role, qt.Equals, aiprovider.RoleUser)
}

func TestDiscard_KeepsNothingAndHasNoIdentity(t *testing.T) {
	// --ephemeral. An identity would invite a `--resume` that cannot work.
	c := qt.New(t)
	var recorder assistsession.Recorder = assistsession.Discard{}

	c.Assert(recorder.Append(assistsession.Record{Kind: assistsession.KindRequest}), qt.IsNil)
	c.Assert(recorder.ID(), qt.Equals, "")
	c.Assert(recorder.Path(), qt.Equals, "")
	c.Assert(recorder.Close(), qt.IsNil)
}

func TestNewID_IsSortableAndDistinct(t *testing.T) {
	c := qt.New(t)
	moment := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)

	first, err := assistsession.NewID(moment, strings.NewReader("abcd"))
	c.Assert(err, qt.IsNil)
	second, err := assistsession.NewID(moment, strings.NewReader("efgh"))
	c.Assert(err, qt.IsNil)

	c.Assert(first, qt.Matches, `20260824T120000-[0-9a-f]{8}`)
	c.Assert(second, qt.Not(qt.Equals), first)
	c.Assert(strings.HasPrefix(second, "20260824T120000-"), qt.IsTrue)
}

func TestOpen_RequiresAProject(t *testing.T) {
	c := qt.New(t)

	store, err := assistsession.Open(assistsession.Options{})

	c.Assert(err, qt.ErrorMatches, "session store requires a project root")
	c.Assert(store, qt.IsNil)
}

func TestRead_RefusesAnIdentifierThatWouldLeaveTheDirectory(t *testing.T) {
	// Read and Delete take this straight from a person's command line and turn
	// it into a path. The file outside the store is the assertion with teeth: a
	// refusal that had still removed it would be a refusal in name only.
	c := qt.New(t)
	store, root := newStore(c)
	write(c, store, "session-1", "hello", "hi")

	outside := filepath.Join(root, ".ptah", "keep-me.jsonl")
	c.Assert(os.WriteFile(outside, []byte("not a session\n"), 0o600), qt.IsNil)

	tests := []struct {
		name string
		id   string
	}{
		{name: "the parent directory", id: "../keep-me"},
		{name: "further up", id: "../../../keep-me"},
		{name: "a separator", id: "sub/session-1"},
		{name: "an absolute path", id: "/etc/hosts"},
		{name: "a backslash", id: `..\keep-me`},
		{name: "nothing at all", id: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, readErr := store.Read(test.id)
			c.Assert(readErr, qt.ErrorIs, assistsession.ErrNotFound)

			_, deleteErr := store.Delete(test.id)
			c.Assert(deleteErr, qt.ErrorIs, assistsession.ErrNotFound)
		})
	}

	_, err := os.Stat(outside)
	c.Assert(err, qt.IsNil, qt.Commentf("a file outside the store must survive every attempt"))
}
