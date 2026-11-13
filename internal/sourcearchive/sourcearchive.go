// Package sourcearchive reads the files a schema or a migration directory is
// selected from out of one downloaded object: the object itself, a
// gzip-compressed tar archive, or a zip archive.
//
// The reader trusts nothing in the object. Every entry name is checked, in the
// selection or not, by the rules here and whatever GODEBUG says about insecure
// paths; inside the selection only regular files and directories are read, and
// every limit counts what the object claims and what it actually expands to.
// The files it returns are the input of [sourcecontent.Digest].
package sourcearchive

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"path"
	"strings"

	"ptah.run/internal/sourcecontent"
)

// Limits on what one object may hold and what a read may expand.
const (
	// MaxEntries bounds the entries of an archive, selected or not.
	MaxEntries = 10_000
	// MaxExpandedBytes bounds the bytes a tar archive's gzip stream
	// decompresses to, every entry of it, selected or not. A zip read inflates
	// only the selected entries, which MaxSelectedBytes bounds.
	MaxExpandedBytes = 1 << 30
	// MaxSelectedBytes bounds the selected files together.
	MaxSelectedBytes = 64 << 20
)

// The refusals, matched with errors.Is. A duplicate selected path, or one that
// is both a file and a directory, is [sourcecontent.ErrDuplicate] or
// [sourcecontent.ErrPathConflict].
var (
	ErrFormat      = errors.New("unknown source format")
	ErrSelection   = errors.New("selected path refused")
	ErrNotFound    = errors.New("selected path not in the archive")
	ErrEntryName   = errors.New("archive entry name refused")
	ErrEntryType   = errors.New("archive entry is not a regular file or a directory")
	ErrEncrypted   = errors.New("archive entry is encrypted")
	ErrCompression = errors.New("archive entry compression method refused")
	ErrCorrupt     = errors.New("archive is corrupt")
	ErrTooLarge    = errors.New("archive too large")
)

// Format is how the selected files are carried.
type Format string

const (
	// FormatFile is one file, the object itself.
	FormatFile Format = "file"
	// FormatTarGzip is a tar archive compressed with gzip.
	FormatTarGzip Format = "tar.gz"
	// FormatZip is a zip archive.
	FormatZip Format = "zip"
)

// Formats are the formats a source may name, in the order a refusal lists
// them.
var Formats = []Format{FormatFile, FormatTarGzip, FormatZip}

// ParseFormat reads a format by its name.
func ParseFormat(name string) (Format, error) {
	for _, format := range Formats {
		if string(format) == name {
			return format, nil
		}
	}
	return "", fmt.Errorf("%w %q: give file, tar.gz or zip", ErrFormat, name)
}

// Kind is what a path selects.
type Kind int

const (
	// File selects one file, as a schema is.
	File Kind = iota
	// Directory selects every file under a directory, as a migration
	// directory is.
	Directory
)

// Options say what to read.
type Options struct {
	Format Format
	// Path, with FormatFile, is the one name the object is given, which names
	// the file in the manifest. With an archive it is the member a File
	// selects, or the directory a Directory selects, where "." is the root.
	Path string
	Kind Kind
}

// Check refuses options no object could satisfy, so a caller can refuse them
// before it fetches anything.
func Check(o Options) error {
	_, err := selection(o)
	return err
}

// selection cleans the selected path: "" for an archive's root.
func selection(o Options) (string, error) {
	if _, err := ParseFormat(string(o.Format)); err != nil {
		return "", err
	}
	if o.Path == "" {
		return "", fmt.Errorf("%w: no path given", ErrSelection)
	}
	if o.Format == FormatFile {
		switch {
		case o.Kind == Directory:
			return "", fmt.Errorf("%w: a directory is read from an archive; give tar.gz or zip, not file", ErrSelection)
		case strings.Contains(o.Path, "/"):
			return "", fmt.Errorf("%w: %q: with file, the path is the file's one name", ErrSelection, o.Path)
		}
	}
	if o.Path == "." && o.Kind == Directory {
		return "", nil
	}
	if _, err := sourcecontent.Manifest([]sourcecontent.File{{Path: o.Path}}); err != nil {
		return "", fmt.Errorf("%w: %w", ErrSelection, err)
	}
	return o.Path, nil
}

// Read returns the selected files of the object of size bytes that object
// reads. A file's path is its path relative to the selected directory, or the
// selected file's own name.
func Read(object io.ReaderAt, size int64, o Options) ([]sourcecontent.File, error) {
	selected, err := selection(o)
	if err != nil {
		return nil, err
	}
	s := &selector{selected: selected, kind: o.Kind, paths: make(map[string]bool)}
	switch o.Format {
	case FormatFile:
		if size > MaxSelectedBytes {
			return nil, fmt.Errorf("%w: the file holds more than %d bytes", ErrTooLarge, MaxSelectedBytes)
		}
		data, err := io.ReadAll(io.NewSectionReader(object, 0, size))
		if err != nil {
			return nil, fmt.Errorf("read the file: %w", err)
		}
		return []sourcecontent.File{{Path: o.Path, Data: data}}, nil
	case FormatTarGzip:
		err = s.tarGzip(io.NewSectionReader(object, 0, size))
	case FormatZip:
		err = s.zip(object, size)
	}
	if err != nil {
		return nil, err
	}
	return s.result()
}

// selector collects the selected files of one archive.
type selector struct {
	selected string
	kind     Kind
	entries  int
	expanded int64
	bytes    int64
	files    []sourcecontent.File
	// paths holds every selected path seen, true for a directory: a path
	// that is both, or a file seen twice, is refused.
	paths map[string]bool
	// directory is set once an entry shows the selected path is a directory.
	directory bool
}

// entry is what the selector learns of one archive entry before reading it.
type entry struct {
	name    string
	isDir   bool
	isFile  bool
	special string
}

// visit checks one entry's name and counts it. It reports the entry's path in
// the result, and whether the entry is selected.
func (s *selector) visit(e entry) (string, bool, error) {
	s.entries++
	if s.entries > MaxEntries {
		return "", false, fmt.Errorf("%w: more than %d entries", ErrTooLarge, MaxEntries)
	}
	name, err := entryName(e.name)
	if err != nil {
		return "", false, err
	}
	relative, selected := s.relative(name)
	if !selected {
		return "", false, nil
	}
	if e.special != "" {
		return "", false, fmt.Errorf("%w: %q is %s", ErrEntryType, e.name, e.special)
	}
	if !e.isDir && !e.isFile {
		return "", false, fmt.Errorf("%w: %q", ErrEntryType, e.name)
	}
	if s.kind == File {
		// The selected path itself, or anything under it. A directory there,
		// or a file under it, makes the selection a directory.
		if e.isDir || relative != "" {
			s.directory = true
			return "", false, nil
		}
		key := path.Base(name)
		return key, true, s.admit(key)
	}
	switch {
	case relative == "" && e.isDir:
		s.directory = true
		return "", false, nil
	case relative == "":
		return "", false, fmt.Errorf("%w: %q is a file, not a directory", ErrSelection, e.name)
	}
	if _, err := sourcecontent.Manifest([]sourcecontent.File{{Path: relative}}); err != nil {
		return "", false, fmt.Errorf("%w: %w", ErrSelection, err)
	}
	if e.isDir {
		return "", false, s.claim(relative, true)
	}
	return relative, true, s.admit(relative)
}

// admit claims a selected file's path, within the bound on selected files.
func (s *selector) admit(relative string) error {
	if err := s.claim(relative, false); err != nil {
		return err
	}
	if len(s.files) == sourcecontent.MaxFiles {
		return fmt.Errorf("%w: more than %d selected files", ErrTooLarge, sourcecontent.MaxFiles)
	}
	return nil
}

// relative is name relative to the selected path, and whether the selection
// holds it at all. A File's own path is "" relative to itself.
func (s *selector) relative(name string) (string, bool) {
	switch {
	case s.selected == "":
		return name, name != ""
	case name == s.selected:
		return "", true
	case strings.HasPrefix(name, s.selected+"/"):
		return name[len(s.selected)+1:], true
	}
	return "", false
}

// claim records a selected path, refusing a second file at it and a path that
// is a file and a directory at once, its parents included.
func (s *selector) claim(relative string, isDir bool) error {
	for parent := path.Dir(relative); parent != "."; parent = path.Dir(parent) {
		if dir, seen := s.paths[parent]; seen && !dir {
			return fmt.Errorf("%w: %q", sourcecontent.ErrPathConflict, parent)
		}
		s.paths[parent] = true
	}
	dir, seen := s.paths[relative]
	switch {
	case !seen:
		s.paths[relative] = isDir
	case dir != isDir:
		return fmt.Errorf("%w: %q", sourcecontent.ErrPathConflict, relative)
	case !isDir:
		return fmt.Errorf("%w: %q", sourcecontent.ErrDuplicate, relative)
	}
	return nil
}

// take reads one selected file of the declared size, charging it to the
// selection's bound before reading any of it. archive/tar and archive/zip
// refuse an entry that holds more or less than it declares once the read
// reaches its end, which the one byte past the size lets it do.
func (s *selector) take(relative string, reader io.Reader, declared int64) error {
	if s.bytes += declared; declared < 0 || s.bytes > MaxSelectedBytes {
		return fmt.Errorf("%w: the selected files hold more than %d bytes", ErrTooLarge, MaxSelectedBytes)
	}
	data, err := io.ReadAll(io.LimitReader(reader, declared+1))
	if err != nil {
		return err
	}
	s.files = append(s.files, sourcecontent.File{Path: relative, Data: data})
	return nil
}

// result is what the selection found, or why it found nothing.
func (s *selector) result() ([]sourcecontent.File, error) {
	if s.kind == File {
		switch {
		case s.directory && len(s.files) == 0:
			return nil, fmt.Errorf("%w: %q is a directory, and a file is selected", ErrSelection, s.selected)
		case s.directory:
			return nil, fmt.Errorf("%w: %q", sourcecontent.ErrPathConflict, s.selected)
		case len(s.files) == 0:
			return nil, fmt.Errorf("%w: %s", ErrNotFound, s.selected)
		}
		return s.files, nil
	}
	if len(s.files) == 0 && !s.directory && len(s.paths) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, shown(s.selected))
	}
	if _, err := sourcecontent.Manifest(s.files); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrSelection, shown(s.selected), err)
	}
	return s.files, nil
}

func shown(selected string) string {
	if selected == "" {
		return "."
	}
	return selected
}

// entryName is an archive entry's name as the selection compares it: a
// leading "./" and a directory's trailing "/" dropped. It refuses a name that
// could reach outside the place an archive is unpacked, wherever the entry
// sits: an absolute path, a ".." element, a backslash, a drive letter or a NUL.
func entryName(raw string) (string, error) {
	switch {
	case strings.ContainsRune(raw, 0):
		return "", fmt.Errorf("%w: %q holds a NUL", ErrEntryName, raw)
	case strings.ContainsRune(raw, '\\'):
		return "", fmt.Errorf("%w: %q holds a backslash", ErrEntryName, raw)
	case strings.HasPrefix(raw, "/"):
		return "", fmt.Errorf("%w: %q is absolute", ErrEntryName, raw)
	case len(raw) >= 2 && raw[1] == ':' && isLetter(raw[0]):
		return "", fmt.Errorf("%w: %q names a drive", ErrEntryName, raw)
	}
	for element := range strings.SplitSeq(raw, "/") {
		if element == ".." {
			return "", fmt.Errorf("%w: %q has the element \"..\"", ErrEntryName, raw)
		}
	}
	name := strings.TrimSuffix(strings.TrimPrefix(raw, "./"), "/")
	if name == "." {
		name = ""
	}
	return name, nil
}

func isLetter(b byte) bool {
	return 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z'
}

// tarGzip reads a gzip-compressed tar archive to its end, the gzip trailer
// included.
func (s *selector) tarGzip(compressed io.Reader) error {
	decompressed, err := gzip.NewReader(compressed)
	if err != nil {
		return fmt.Errorf("%w: gzip: %w", ErrCorrupt, err)
	}
	defer func() { _ = decompressed.Close() }()
	expansion := &expansionReader{reader: decompressed, budget: &s.expanded}
	archive := tar.NewReader(expansion)
	for {
		header, err := archive.Next()
		switch {
		case errors.Is(err, io.EOF):
			// The tar archive ends before the gzip stream does: read the rest,
			// so a truncated stream or a trailer that does not match is seen.
			if _, err := io.Copy(io.Discard, expansion); err != nil {
				return s.corrupt(expansion, "gzip", err)
			}
			return nil
		case errors.Is(err, tar.ErrInsecurePath):
			// GODEBUG=tarinsecurepath=0 says so too; the name is checked below
			// either way.
		case err != nil:
			return s.corrupt(expansion, "tar", err)
		}
		relative, selected, err := s.visit(tarEntry(header))
		if err != nil {
			return err
		}
		if !selected {
			continue
		}
		if err := s.take(relative, archive, header.Size); err != nil {
			return s.corrupt(expansion, "tar", err)
		}
	}
}

// corrupt says why a read stopped: the expansion bound, a refusal of the
// selector's own, or a stream the format cannot read.
func (s *selector) corrupt(expansion *expansionReader, format string, err error) error {
	switch {
	case expansion.exceeded:
		return fmt.Errorf("%w: it expands past %d bytes", ErrTooLarge, MaxExpandedBytes)
	case errors.Is(err, ErrTooLarge), errors.Is(err, ErrCorrupt):
		return err
	}
	return fmt.Errorf("%w: %s: %w", ErrCorrupt, format, err)
}

func tarEntry(header *tar.Header) entry {
	e := entry{name: header.Name}
	for key := range header.PAXRecords {
		if strings.HasPrefix(key, "GNU.sparse.") {
			e.special = "a sparse file"
			return e
		}
	}
	switch header.Typeflag {
	case tar.TypeReg, tar.TypeRegA: //nolint:staticcheck // TypeRegA is what an old archive writes for a regular file
		e.isFile = true
	case tar.TypeDir:
		e.isDir = true
	case tar.TypeSymlink:
		e.special = "a symbolic link"
	case tar.TypeLink:
		e.special = "a hard link"
	case tar.TypeChar, tar.TypeBlock:
		e.special = "a device"
	case tar.TypeFifo:
		e.special = "a FIFO"
	case tar.TypeGNUSparse:
		e.special = "a sparse file"
	}
	return e
}

// expansionReader fails a read past the expansion bound.
type expansionReader struct {
	reader   io.Reader
	budget   *int64
	exceeded bool
}

func (e *expansionReader) Read(p []byte) (int, error) {
	n, err := e.reader.Read(p)
	*e.budget += int64(n)
	if *e.budget > MaxExpandedBytes {
		e.exceeded = true
		return n, ErrTooLarge
	}
	return n, err
}

// zip reads the selected entries of a zip archive, and inflates no other.
func (s *selector) zip(object io.ReaderAt, size int64) error {
	archive, err := zip.NewReader(object, size)
	if err != nil && !errors.Is(err, zip.ErrInsecurePath) {
		// GODEBUG=zipinsecurepath=0 says so with a usable reader; the names
		// are checked below either way.
		return fmt.Errorf("%w: zip: %w", ErrCorrupt, err)
	}
	for _, file := range archive.File {
		relative, selected, err := s.visit(zipEntry(file))
		if err != nil {
			return err
		}
		if !selected {
			continue
		}
		if err := s.inflate(file, relative); err != nil {
			return err
		}
	}
	return nil
}

// zipFlagEncrypted is the general purpose bit an encrypted entry sets.
const zipFlagEncrypted = 0x1

// inflate reads one selected zip entry and checks its CRC.
func (s *selector) inflate(file *zip.File, relative string) error {
	switch {
	case file.Flags&zipFlagEncrypted != 0:
		return fmt.Errorf("%w: %q", ErrEncrypted, file.Name)
	case file.Method != zip.Store && file.Method != zip.Deflate:
		return fmt.Errorf("%w: %q uses method %d; store and deflate are read", ErrCompression, file.Name, file.Method)
	}
	// A size past the largest int64 turns negative, which take refuses.
	declared := int64(file.UncompressedSize64) // #nosec G115 -- take refuses a negative size
	reader, err := file.Open()
	if err != nil {
		return fmt.Errorf("%w: zip: %q: %w", ErrCorrupt, file.Name, err)
	}
	defer func() { _ = reader.Close() }()
	if err := s.take(relative, reader, declared); err != nil {
		if errors.Is(err, ErrTooLarge) || errors.Is(err, ErrCorrupt) {
			return err
		}
		return fmt.Errorf("%w: zip: %q: %w", ErrCorrupt, file.Name, err)
	}
	// archive/zip skips its own check when the archive records a CRC of 0.
	if sum := crc32.ChecksumIEEE(s.files[len(s.files)-1].Data); sum != file.CRC32 {
		return fmt.Errorf("%w: zip: %q: CRC %08x, the archive says %08x", ErrCorrupt, file.Name, sum, file.CRC32)
	}
	return nil
}

func zipEntry(file *zip.File) entry {
	e := entry{name: file.Name}
	mode := file.Mode()
	switch {
	case mode&fs.ModeSymlink != 0:
		e.special = "a symbolic link"
	case mode&(fs.ModeDevice|fs.ModeCharDevice) != 0:
		e.special = "a device"
	case mode&fs.ModeNamedPipe != 0:
		e.special = "a FIFO"
	case mode&(fs.ModeSocket|fs.ModeIrregular) != 0:
		e.special = "not a regular file"
	case mode.IsDir() || strings.HasSuffix(file.Name, "/"):
		e.isDir = true
	default:
		e.isFile = true
	}
	return e
}
