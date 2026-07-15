// Package migratesum provides a migration-directory integrity check: a
// committed checksum file records the hash of every migration file plus a
// directory-level hash, so an out-of-band edit to an already-applied migration
// is caught in CI instead of silently breaking reproducibility (issue #161).
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
// Ptah-format integrity uses ptah.sum and hashes each file independently.
// Atlas-format integrity uses atlas.sum-compatible chained hashes, matching
// Atlas's migration directory integrity file byte for byte.
package migratesum

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/stokaro/ptah/migration/migrator"
)

// FileName is the conventional integrity file inside a migrations directory.
const FileName = "ptah.sum"

// AtlasFileName is the conventional Atlas integrity file inside a migrations
// directory.
const AtlasFileName = "atlas.sum"

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
	return ComputeWithFormat(fsys, migrator.MigrationDirFormatAuto)
}

// ComputeWithFormat walks fsys and builds the sum over every migration file
// recognized by the selected directory format.
func ComputeWithFormat(fsys fs.FS, format migrator.MigrationDirFormat) (*SumFile, error) {
	normalized, err := migrator.ParseMigrationDirFormat(string(format))
	if err != nil {
		return nil, err
	}

	useAtlasHash, err := shouldUseAtlasHash(fsys, normalized)
	if err != nil {
		return nil, err
	}
	if useAtlasHash {
		return computeAtlas(fsys)
	}

	files, err := migrator.DiscoverMigrationFiles(fsys, normalized)
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, 0, len(files))
	for _, file := range files {
		data, err := fs.ReadFile(fsys, file.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", file.Path, err)
		}
		entries = append(entries, Entry{Name: file.Path, Hash: hashContent(data)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })

	return &SumFile{DirHash: dirHash(entries), Entries: entries}, nil
}

// FileNameForFormat returns the integrity file written by the selected
// explicit format. Auto mode preserves Ptah's default ptah.sum output.
func FileNameForFormat(format migrator.MigrationDirFormat) (string, error) {
	normalized, err := migrator.ParseMigrationDirFormat(string(format))
	if err != nil {
		return "", err
	}
	if normalized == migrator.MigrationDirFormatAtlas {
		return AtlasFileName, nil
	}
	return FileName, nil
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

func shouldUseAtlasHash(fsys fs.FS, format migrator.MigrationDirFormat) (bool, error) {
	switch format {
	case migrator.MigrationDirFormatAtlas:
		return true, nil
	case migrator.MigrationDirFormatPtah:
		return false, nil
	}
	return hasFile(fsys, AtlasFileName)
}

func hasFile(fsys fs.FS, name string) (bool, error) {
	_, err := fs.Stat(fsys, name)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func computeAtlas(fsys fs.FS) (*SumFile, error) {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	entries := make([]Entry, 0, len(names))
	h := sha256.New()
	for _, name := range names {
		data, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", name, err)
		}
		_, _ = h.Write([]byte(name))
		if atlasSumIgnored(data) {
			continue
		}
		_, _ = h.Write(data)
		entries = append(entries, Entry{Name: name, Hash: hashPrefix + base64.StdEncoding.EncodeToString(h.Sum(nil))})
	}

	return &SumFile{DirHash: atlasDirHash(entries), Entries: entries}, nil
}

func atlasSumIgnored(data []byte) bool {
	name, args, ok := atlasDirective(data)
	return ok && name == "sum" && args == "ignore"
}

func atlasDirective(data []byte) (name, args string, ok bool) {
	content := string(data)
	prefix, rest, ok := strings.Cut(content, "atlas:")
	if !ok {
		return "", "", false
	}
	for _, r := range prefix {
		if r < ' ' || r > '~' {
			return "", "", false
		}
	}

	line, _, _ := strings.Cut(rest, "\n")
	nameEnd := 0
	for nameEnd < len(line) && isDirectiveNameChar(line[nameEnd]) {
		nameEnd++
	}
	if nameEnd == 0 {
		return "", "", false
	}
	if nameEnd < len(line) && line[nameEnd] == ' ' {
		args = strings.TrimLeft(line[nameEnd+1:], " ")
	}
	return line[:nameEnd], args, true
}

func isDirectiveNameChar(b byte) bool {
	return b == '_' || ('0' <= b && b <= '9') || ('A' <= b && b <= 'Z') || ('a' <= b && b <= 'z')
}

func atlasDirHash(entries []Entry) string {
	h := sha256.New()
	for _, entry := range entries {
		_, _ = h.Write([]byte(entry.Name))
		_, _ = h.Write([]byte(strings.TrimPrefix(entry.Hash, hashPrefix)))
	}
	return hashPrefix + base64.StdEncoding.EncodeToString(h.Sum(nil))
}
