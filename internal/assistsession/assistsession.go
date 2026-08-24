// Package assistsession stores what a Ptah Assist conversation did, so it can
// be read afterwards and continued later.
//
// # One file per session, one JSON object per line
//
// Append-only JSON lines rather than one document or a database. A session is
// written while it happens, and the failure mode of a conversation is the
// process ending in the middle of one: a truncated JSON array is unreadable,
// and a truncated JSONL file is every record up to the last complete one. The
// same reasoning internal/agentaudit gives for the audit log applies here, and
// the two are deliberately different files -- the audit record is what Ptah
// decided and must not be editable by a `sessions delete`.
//
// # Where they live
//
// `<project>/.ptah/sessions/`. The project rather than the home directory,
// because a conversation is about one repository and reading it later means
// reading it beside that repository; `.ptah` because that is the one directory
// name this tree keeps state under, beside the approval keys and the audit log.
//
// # What is in one, and what a reader should know
//
// The conversation, including the tool results -- which means the file contains
// whatever Ptah read on the model's behalf: migration text, schema files,
// database object names. It is written 0600 and it belongs in .gitignore.
// [Store.Notice] is the sentence a surface shows the first time it writes one,
// because a person who did not know that should find out from Ptah rather than
// from a code review.
//
// No credential is stored. The profile's NAME is, because a session that could
// not say which provider answered cannot be read later; the key it resolves is
// never in the record.
package assistsession

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/aiprovider"
)

// SchemaVersion identifies the record contract. A change to the shape of any
// record below changes it.
const SchemaVersion = 1

// DirName is where sessions live inside the project's Ptah directory.
const DirName = "sessions"

// Notice is what a surface tells a person the first time it writes a session.
const Notice = "Ptah is saving this conversation, including what it read on the model's " +
	"behalf, under .ptah/sessions. Add that directory to .gitignore, or run with --ephemeral."

// Kind names a record's type, which is the first thing a reader branches on.
type Kind string

const (
	// KindHeader opens a file and is written once.
	KindHeader Kind = "session"
	// KindRequest is something the person asked.
	KindRequest Kind = "request"
	// KindTool is one tool call and what Ptah answered.
	KindTool Kind = "tool"
	// KindAnswer is the model's reply to one request.
	KindAnswer Kind = "answer"
)

var (
	// ErrNotFound reports a session identifier nothing matches.
	ErrNotFound = errors.New("no such session")
	// ErrCorrupt reports a file whose first record is not a header.
	ErrCorrupt = errors.New("session file is not readable")
)

// Record is one line of a session file.
//
// One struct rather than four, because a reader that has to know the type
// before it can decode is a reader that cannot skip a record it does not
// understand -- and a session written by a later Ptah should still be listable
// by an earlier one.
type Record struct {
	Kind Kind      `json:"type"`
	At   time.Time `json:"at"`

	// Header fields, present when Kind is KindHeader.
	SchemaVersion int    `json:"schema_version,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	PtahVersion   string `json:"ptah_version,omitempty"`
	ProjectRoot   string `json:"project_root,omitempty"`
	Provider      string `json:"provider,omitempty"`
	Model         string `json:"model,omitempty"`

	// Text carries the request or the answer.
	Text string `json:"text,omitempty"`

	// Tool fields, present when Kind is KindTool.
	Tool      string          `json:"tool,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Failed    bool            `json:"failed,omitempty"`
	Result    string          `json:"result,omitempty"`
	Truncated bool            `json:"truncated,omitempty"`

	// Answer fields, present when Kind is KindAnswer.
	Turns      int              `json:"turns,omitempty"`
	StopReason string           `json:"stop_reason,omitempty"`
	Usage      aiprovider.Usage `json:"usage,omitzero"`
	// Verified reports that at least one Ptah tool answered during the turn.
	// A session that did not record this could not tell a checked answer from
	// an unchecked one when it is read back.
	Verified bool `json:"verified,omitempty"`
	// Error is why the run ended badly, empty when it did not.
	//
	// A run that hit a limit, lost the endpoint or was refused still produces
	// an answer record, and without this the record is an empty answer that
	// reads exactly like a model with nothing to say. A consumer of the record
	// stream has no other channel to learn it from.
	Error string `json:"error,omitempty"`
}

// Summary is one session as a listing shows it.
type Summary struct {
	ID        string    `json:"id"`
	Path      string    `json:"-"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Provider  string    `json:"provider"`
	Model     string    `json:"model"`
	Requests  int       `json:"requests"`
	ToolCalls int       `json:"tool_calls"`
	// First is the opening request, truncated, because that is what a person
	// recognizes a session by.
	First string `json:"first_request"`
}

// Store is a project's session directory.
type Store struct {
	dir   string
	clock func() time.Time
}

// Options configures a store.
type Options struct {
	// Root is the project the sessions belong to. Sessions land in
	// <Root>/.ptah/sessions.
	Root string
	// Clock is the time source, injectable so a test can pin identifiers.
	Clock func() time.Time
}

// Open prepares a store. It creates nothing until a session is written.
func Open(opts Options) (*Store, error) {
	if opts.Root == "" {
		return nil, errors.New("session store requires a project root")
	}
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Store{
		dir:   filepath.Join(opts.Root, ".ptah", DirName),
		clock: clock,
	}, nil
}

// Dir is where the sessions are, for a surface that tells a person.
func (s *Store) Dir() string { return s.dir }

// Writer appends to one session file.
type Writer struct {
	id   string
	path string
	file *os.File

	clock func() time.Time
}

// Create starts a session and writes its header.
//
// Extra recorders are mirrors: every record, the header included, is written to
// them as well as to the file. That is how `--format jsonl` prints a
// conversation without keeping a second copy of the code that formats one.
func (s *Store) Create(id string, header Record, mirrors ...Recorder) (Recorder, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return nil, fmt.Errorf("create the session directory: %w", err)
	}
	path := filepath.Join(s.dir, id+".jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open the session file: %w", err)
	}

	writer := &Writer{id: id, path: path, file: file, clock: s.clock}
	recorder := Recorder(writer)
	if len(mirrors) > 0 {
		recorder = NewTee(s.clock, append([]Recorder{writer}, mirrors...)...)
	}

	header.Kind = KindHeader
	header.SchemaVersion = SchemaVersion
	header.SessionID = id
	if err := recorder.Append(header); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return recorder, nil
}

// Append writes one record, stamping it with the store's clock.
//
// The timestamp is the writer's rather than the caller's for the same reason
// the audit log's is: a caller that could set it could write a record that
// appears to belong to a different moment.
func (w *Writer) Append(record Record) error {
	record.At = w.clock().UTC()
	return w.appendStamped(record)
}

// appendStamped writes a record whose timestamp is already set.
//
// [Tee] stamps once and hands the same record to every sink, so that the copy a
// person reads on stdout and the copy on disk are the same bytes rather than
// two encodings of the same moment. It is unexported because that is the only
// caller allowed to decide a record's time.
func (w *Writer) appendStamped(record Record) error {
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode the session record: %w", err)
	}
	if _, err := w.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write the session record: %w", err)
	}
	return nil
}

// ID is the session's identifier.
func (w *Writer) ID() string { return w.id }

// Path is the file being written.
func (w *Writer) Path() string { return w.path }

// Close releases the file. Closing twice is a no-op.
func (w *Writer) Close() error {
	if w.file == nil {
		return nil
	}
	file := w.file
	w.file = nil
	return file.Close()
}

// Discard is a writer that keeps nothing, for `--ephemeral`.
//
// A type rather than a nil check at every call site, because a nil check is how
// one call site comes to be missing one.
type Discard struct{}

// Append accepts and drops the record.
func (Discard) Append(Record) error { return nil }

// ID answers the empty string: an ephemeral session has no identity to resume.
func (Discard) ID() string { return "" }

// Path answers the empty string.
func (Discard) Path() string { return "" }

// Close does nothing.
func (Discard) Close() error { return nil }

// Recorder is what a surface writes through, so `--ephemeral` and a real store
// are the same code path.
type Recorder interface {
	Append(record Record) error
	ID() string
	Path() string
	Close() error
}

// List summarizes every session, most recently updated first.
func (s *Store) List() ([]Summary, error) {
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return make([]Summary, 0), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the session directory: %w", err)
	}

	summaries := make([]Summary, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".jsonl" {
			continue
		}
		summary, summaryErr := s.summarize(filepath.Join(s.dir, entry.Name()))
		if summaryErr != nil {
			// One unreadable file must not hide the rest: a session written by
			// a version this build does not know is still somebody's history.
			continue
		}
		summaries = append(summaries, summary)
	}
	slices.SortFunc(summaries, func(a, b Summary) int {
		return b.UpdatedAt.Compare(a.UpdatedAt)
	})
	return summaries, nil
}

// summarize reads one file into a listing entry.
func (s *Store) summarize(path string) (Summary, error) {
	records, err := readRecords(path)
	if err != nil {
		return Summary{}, err
	}
	if len(records) == 0 || records[0].Kind != KindHeader {
		return Summary{}, fmt.Errorf("%w: %s", ErrCorrupt, path)
	}

	header := records[0]
	summary := Summary{
		ID:        header.SessionID,
		Path:      path,
		StartedAt: header.At,
		UpdatedAt: header.At,
		Provider:  header.Provider,
		Model:     header.Model,
	}
	for _, record := range records[1:] {
		summary.UpdatedAt = record.At
		switch record.Kind {
		case KindRequest:
			summary.Requests++
			if summary.First == "" {
				summary.First = firstLine(record.Text)
			}
		case KindTool:
			summary.ToolCalls++
		}
	}
	return summary, nil
}

// Read returns every record of one session.
func (s *Store) Read(id string) ([]Record, error) {
	path, err := s.resolve(id)
	if err != nil {
		return nil, err
	}
	return readRecords(path)
}

// Delete removes one session.
func (s *Store) Delete(id string) (string, error) {
	path, err := s.resolve(id)
	if err != nil {
		return "", err
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("delete the session: %w", err)
	}
	return path, nil
}

// Prune removes sessions older than the given age, and reports what it removed.
func (s *Store) Prune(olderThan time.Duration) ([]string, error) {
	summaries, err := s.List()
	if err != nil {
		return nil, err
	}
	cutoff := s.clock().Add(-olderThan)
	removed := make([]string, 0)
	for _, summary := range summaries {
		if summary.UpdatedAt.After(cutoff) {
			continue
		}
		if err := os.Remove(summary.Path); err != nil {
			return removed, fmt.Errorf("delete %s: %w", summary.ID, err)
		}
		removed = append(removed, summary.ID)
	}
	return removed, nil
}

// resolve turns an identifier, or a unique prefix of one, into a path.
//
// A prefix is accepted because an identifier is long enough to be worth not
// retyping, and an ambiguous prefix is refused by naming the candidates rather
// than picking one.
// validateID refuses an identifier that is not a plain name.
//
// Read and Delete take this straight from a person's command line, and the
// identifier becomes a path. Without this, "sessions delete ../../../x" removes
// a file outside the session directory, and "--resume ../../../x" reads one and
// sends it to a model provider -- which is the boundary this whole surface
// exists to keep. The generated form is a timestamp and hex, so the alphabet
// below accepts every identifier Ptah mints and every prefix of one.
func validateID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: no session named", ErrNotFound)
	}
	if strings.Contains(id, "..") {
		return fmt.Errorf("%w: %q is not a session identifier", ErrNotFound, id)
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf("%w: %q is not a session identifier", ErrNotFound, id)
		}
	}
	return nil
}

func (s *Store) resolve(id string) (string, error) {
	if err := validateID(id); err != nil {
		return "", err
	}
	exact := filepath.Join(s.dir, id+".jsonl")
	if _, err := os.Stat(exact); err == nil {
		return exact, nil
	}

	summaries, err := s.List()
	if err != nil {
		return "", err
	}
	matches := make([]Summary, 0, 1)
	for _, summary := range summaries {
		if strings.HasPrefix(summary.ID, id) {
			matches = append(matches, summary)
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("%w: %q", ErrNotFound, id)
	case 1:
		return matches[0].Path, nil
	}
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		names = append(names, match.ID)
	}
	// Sorted rather than in listing order: the listing is by recency, and a
	// diagnostic whose candidates reorder between runs is one a person cannot
	// compare with the last time they saw it.
	slices.Sort(names)
	return "", fmt.Errorf("%q names %d sessions: %s", id, len(matches), strings.Join(names, ", "))
}

// readRecords decodes one file.
//
// A truncated final line is dropped rather than failing the read: it is the
// shape a process that stopped mid-write leaves, and every complete record
// before it is still what happened.
func readRecords(path string) ([]Record, error) {
	raw, err := os.ReadFile(path) // #nosec G304 -- the path comes from this store's own directory listing
	if err != nil {
		return nil, fmt.Errorf("read the session file: %w", err)
	}
	records := make([]Record, 0, 8)
	for line := range strings.SplitSeq(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

// Messages reconstructs the conversation a resumed session continues from.
//
// Requests and answers only. The tool results are in the file and are
// deliberately not replayed: they described the project as it was, and a
// resumed session that fed them back as current would have the model reasoning
// about a directory that may have changed since. It re-reads instead, which
// costs a tool call and is the answer that is still true.
func Messages(records []Record) []aiprovider.Message {
	messages := make([]aiprovider.Message, 0, len(records))
	for _, record := range records {
		switch record.Kind {
		case KindRequest:
			messages = append(messages, aiprovider.Message{
				Role: aiprovider.RoleUser, Content: record.Text,
			})
		case KindAnswer:
			if record.Text == "" {
				continue
			}
			messages = append(messages, aiprovider.Message{
				Role: aiprovider.RoleAssistant, Content: record.Text,
			})
		}
	}
	return messages
}

// firstLine renders a request for a listing.
func firstLine(text string) string {
	const width = 72
	line, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	if len(line) <= width {
		return line
	}
	return line[:width] + "..."
}

// NewID mints a session identifier: a sortable stamp and a random tail.
//
// The stamp makes a directory listing chronological without reading any file,
// and the tail keeps two sessions started in the same second apart.
func NewID(now time.Time, random io.Reader) (string, error) {
	tail := make([]byte, 4)
	if _, err := io.ReadFull(random, tail); err != nil {
		return "", fmt.Errorf("mint a session identifier: %w", err)
	}
	return fmt.Sprintf("%s-%x", now.UTC().Format("20060102T150405"), tail), nil
}
