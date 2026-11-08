// Package providerexchange records the comparison exchanges between Ptah and an
// executable provider so that a person can read them: each CompareModels
// request and the reply or the error it got, written as the contract's own
// JSON. A native command's --provider-exchanges flag installs a [Recorder] in
// the command's context, and internal/providerhost records each comparison it
// sends under that context.
//
// Only comparisons are recorded. A comparison carries identities, model values
// and coverage, the facts a saved plan already holds, and a secret travels in
// one only as the name of the input its value comes from. The calls that can
// carry a credential are never recorded: Open with its connection address,
// Render and Execute with their execution inputs, and ReadSQLSchema with the
// passwords a document gives its roles.
package providerexchange

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ErrDirectoryNotEmpty reports a recording directory that already holds
// files. A second run's files would sort among the first run's, and a reader
// could not tell the two apart.
var ErrDirectoryNotEmpty = errors.New("the provider exchange directory is not empty")

// Recorder writes the exchanges of one run into one directory. Each exchange
// takes the next number, so the files sort in the order the calls were sent;
// it is safe for concurrent use. A nil *Recorder records nothing.
type Recorder struct {
	dir  string
	next atomic.Int64
}

// New returns a recorder that writes into dir, creating it when it does not
// exist. It refuses a path that is not a directory, and a directory that holds
// anything, with [ErrDirectoryNotEmpty].
func New(dir string) (*Recorder, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("the provider exchange directory is empty")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create the provider exchange directory: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read the provider exchange directory: %w", err)
	}
	if len(entries) > 0 {
		return nil, fmt.Errorf("%w: %s holds %d entries; name a new or empty directory", ErrDirectoryNotEmpty, dir, len(entries))
	}
	return &Recorder{dir: dir}, nil
}

// Record writes one exchange: method is the contract method, such as
// CompareModels, and label names the call within it, such as the comparison
// service's ID. The request is written as <number>-<method>-<label>.request.json,
// and then either the reply as .reply.json or the call's error as .error.txt.
// A file that exists is never overwritten.
func (r *Recorder) Record(method, label string, request, reply proto.Message, callErr error) error {
	if r == nil {
		return nil
	}
	base := fmt.Sprintf("%04d-%s-%s", r.next.Add(1), fileWord(method), fileWord(label))
	if err := r.write(base+".request.json", request); err != nil {
		return err
	}
	if callErr != nil {
		return r.create(base+".error.txt", []byte(callErr.Error()+"\n"))
	}
	return r.write(base+".reply.json", reply)
}

func (r *Recorder) write(name string, message proto.Message) error {
	encoded, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode the provider exchange %s: %w", name, err)
	}
	return r.create(name, append(encoded, '\n'))
}

func (r *Recorder) create(name string, content []byte) error {
	path := filepath.Join(r.dir, name)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- the directory is the one the operator named, and the name is built from a number and two file words.
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("write the provider exchange %s: the file exists", path)
		}
		return fmt.Errorf("write the provider exchange: %w", err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return fmt.Errorf("write the provider exchange %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("write the provider exchange %s: %w", path, err)
	}
	return nil
}

// fileWord keeps the letters, digits, dots and hyphens of word, lower-cased,
// and turns every other character into a hyphen, so a comparison ID makes a
// file name every platform accepts.
func fileWord(word string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(word) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return "call"
	}
	return b.String()
}

type contextKey struct{}

// With returns ctx carrying r, so that every comparison sent under it is
// recorded.
func With(ctx context.Context, r *Recorder) context.Context {
	return context.WithValue(ctx, contextKey{}, r)
}

// FromContext returns the recorder ctx carries, or nil.
func FromContext(ctx context.Context) *Recorder {
	recorder, _ := ctx.Value(contextKey{}).(*Recorder)
	return recorder
}
