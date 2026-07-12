// Package migratesum provides a migration-directory integrity check: a
// committed checksum file (ptah.sum) records the hash of every migration file
// plus a directory-level hash, so an out-of-band edit to an already-applied
// migration is caught in CI instead of silently breaking reproducibility
// (issue #161).
//
// # File format
//
// The sum file is line-oriented. The first line is the directory hash; each
// following line is a migration file and its hash:
//
//	h1:<base64 sha256 over the entries below>
//	0000000001_init.up.sql h1:<base64 sha256 of file contents>
//	0000000001_init.down.sql h1:<base64 sha256 of file contents>
//
// The layout and the h1: (base64-encoded SHA-256) scheme mirror Atlas's
// atlas.sum so a future "atlas mode" (issue #250) can converge on it; the
// file is named ptah.sum and its exact byte-compatibility with Atlas is
// intentionally out of scope until that decision lands.
package migratesum

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/stokaro/ptah/migration/migrator"
)

// FileName is the conventional integrity file inside a migrations directory.
const FileName = "ptah.sum"

// hashPrefix marks a base64-encoded SHA-256 hash, matching Atlas's h1 scheme.
const hashPrefix = "h1:"

// Entry is one migration file and its content hash.
type Entry struct {
	// Name is the slash-separated path of the file relative to the
	// migrations directory.
	Name string
	// Hash is the h1: content hash of the file.
	Hash string
}

// SumFile is the parsed integrity file: a directory hash over all entries and
// the per-file entries themselves (sorted by name).
type SumFile struct {
	DirHash string
	Entries []Entry
}

// Compute walks fsys and builds the sum over every migration file the
// migrator recognizes (NNNNNNNNNN_description.(up|down).sql), so the checksum
// covers exactly what migrate-up/down would execute. The ptah.sum file itself
// and any non-migration file are excluded.
func Compute(fsys fs.FS) (*SumFile, error) {
	var entries []Entry
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !migrator.ValidateMigrationFileName(path.Base(p)) {
			return nil
		}
		data, err := fs.ReadFile(fsys, p)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", p, err)
		}
		entries = append(entries, Entry{Name: p, Hash: hashContent(data)})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan migrations directory: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })

	return &SumFile{DirHash: dirHash(entries), Entries: entries}, nil
}

// Bytes renders the sum file in its on-disk form.
func (s *SumFile) Bytes() []byte {
	var b strings.Builder
	b.WriteString(s.DirHash)
	b.WriteByte('\n')
	for _, e := range s.Entries {
		b.WriteString(e.Name)
		b.WriteByte(' ')
		b.WriteString(e.Hash)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// Parse reads a sum file. It tolerates a trailing newline and CRLF line
// endings (a checkout on Windows, or git autocrlf, must not report false
// drift) but rejects structurally malformed content so a corrupt sum file is
// an explicit error rather than a silent mismatch.
func Parse(data []byte) (*SumFile, error) {
	lines := strings.Split(strings.TrimRight(string(data), "\r\n"), "\n")
	dirLine := strings.TrimRight(lines[0], "\r")
	if dirLine == "" {
		return nil, fmt.Errorf("empty or missing directory hash line")
	}
	if !validHash(dirLine) {
		return nil, fmt.Errorf("malformed directory hash line: %q", dirLine)
	}
	sum := &SumFile{DirHash: dirLine}
	for _, line := range lines[1:] {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		// Split on the LAST space: a migration description may legally
		// contain spaces (the migrator's name regex allows any character),
		// but an h1: base64 hash never does, so the trailing token is the
		// hash and everything before it is the file name.
		idx := strings.LastIndex(line, " ")
		if idx <= 0 {
			return nil, fmt.Errorf("malformed entry line: %q", line)
		}
		name, hash := line[:idx], line[idx+1:]
		if !validHash(hash) {
			return nil, fmt.Errorf("malformed entry line: %q", line)
		}
		sum.Entries = append(sum.Entries, Entry{Name: name, Hash: hash})
	}
	return sum, nil
}

// validHash reports whether s is a well-formed h1: hash: the prefix plus a
// standard-base64 SHA-256 digest. Rejecting a syntactically broken hash at
// parse time keeps a corrupt ptah.sum a usage error (exit 2) rather than
// masquerading as content drift (exit 1).
func validHash(s string) bool {
	rest, ok := strings.CutPrefix(s, hashPrefix)
	if !ok {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(rest)
	return err == nil && len(decoded) == sha256.Size
}

// hashContent returns the h1: content hash of a migration file.
func hashContent(data []byte) string {
	sum := sha256.Sum256(data)
	return hashPrefix + base64.StdEncoding.EncodeToString(sum[:])
}

// dirHash returns the h1: hash over the (sorted) entries, binding every file
// name and content hash into a single directory-level checksum.
func dirHash(entries []Entry) string {
	h := sha256.New()
	for _, e := range entries {
		fmt.Fprintf(h, "%s %s\n", e.Name, e.Hash)
	}
	return hashPrefix + base64.StdEncoding.EncodeToString(h.Sum(nil))
}
