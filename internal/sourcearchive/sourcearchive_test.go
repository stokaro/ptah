package sourcearchive_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"cmp"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sourcearchive"
	"ptah.run/internal/sourcecontent"
)

// entry is one member of a test archive.
type entry struct {
	name, data string
	// typeflag is the tar type; a regular file when zero.
	typeflag byte
	// mode is the zip mode; a plain file when zero.
	mode fs.FileMode
	// zipRaw writes the entry as it is, under zipMethod (stored when zero),
	// with zipFlags and, when not zero, zipCRC and zipSize in place of the
	// computed CRC and size. zipZeroCRC records a CRC of 0.
	zipRaw     bool
	zipMethod  uint16
	zipFlags   uint16
	zipCRC     uint32
	zipZeroCRC bool
	zipSize    uint64
}

func tarGzip(c *qt.C, entries ...entry) []byte {
	var archive bytes.Buffer
	compressed := gzip.NewWriter(&archive)
	writer := tar.NewWriter(compressed)
	for _, e := range entries {
		header := &tar.Header{Name: e.name, Typeflag: cmp.Or(e.typeflag, tar.TypeReg), Mode: 0o644}
		if header.Typeflag == tar.TypeSymlink || header.Typeflag == tar.TypeLink {
			header.Linkname = "db/target.sql"
		}
		// Only a regular file carries data; anything else is its header.
		if header.Typeflag != tar.TypeReg {
			c.Assert(writer.WriteHeader(header), qt.IsNil)
			continue
		}
		header.Size = int64(len(e.data))
		c.Assert(writer.WriteHeader(header), qt.IsNil)
		_, err := writer.Write([]byte(e.data))
		c.Assert(err, qt.IsNil)
	}
	c.Assert(writer.Close(), qt.IsNil)
	c.Assert(compressed.Close(), qt.IsNil)
	return archive.Bytes()
}

func zipOf(c *qt.C, entries ...entry) []byte {
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	for _, e := range entries {
		if e.zipRaw {
			crc := cmp.Or(e.zipCRC, crc32.ChecksumIEEE([]byte(e.data)))
			if e.zipZeroCRC {
				crc = 0
			}
			header := &zip.FileHeader{
				Name: e.name, Method: e.zipMethod, Flags: e.zipFlags, CRC32: crc,
				CompressedSize64:   uint64(len(e.data)),
				UncompressedSize64: cmp.Or(e.zipSize, uint64(len(e.data))),
			}
			header.SetMode(cmp.Or(e.mode, 0o644))
			raw, err := writer.CreateRaw(header)
			c.Assert(err, qt.IsNil)
			_, err = raw.Write([]byte(e.data))
			c.Assert(err, qt.IsNil)
			continue
		}
		header := &zip.FileHeader{Name: e.name, Method: zip.Deflate}
		header.SetMode(cmp.Or(e.mode, 0o644))
		file, err := writer.CreateHeader(header)
		c.Assert(err, qt.IsNil)
		_, err = file.Write([]byte(e.data))
		c.Assert(err, qt.IsNil)
	}
	c.Assert(writer.Close(), qt.IsNil)
	return archive.Bytes()
}

func read(object []byte, o sourcearchive.Options) ([]sourcecontent.File, error) {
	return sourcearchive.Read(bytes.NewReader(object), int64(len(object)), o)
}

var migrations = sourcearchive.Options{Path: "db/migrations", Kind: sourcearchive.Directory}

func withFormat(o sourcearchive.Options, format sourcearchive.Format) sourcearchive.Options {
	o.Format = format
	return o
}

// A migration archive as a CI job packs it: a leading "./", directory
// entries, files beside the selection, and a symbolic link outside it, which
// is skipped like any other entry there.
var packed = []entry{
	{name: "./", typeflag: tar.TypeDir, mode: fs.ModeDir | 0o755},
	{name: "./README.md", data: "# orders\n"},
	{name: "./db/", typeflag: tar.TypeDir, mode: fs.ModeDir | 0o755},
	{name: "./db/schema.hcl", data: "table \"orders\" {}\n"},
	{name: "./db/migrations/", typeflag: tar.TypeDir, mode: fs.ModeDir | 0o755},
	{name: "./db/migrations/0001_create.up.sql", data: "CREATE TABLE orders (id int);\r\n"},
	{name: "./db/migrations/0001_create.down.sql", data: ""},
	{name: "./db/migrations/seed/", typeflag: tar.TypeDir, mode: fs.ModeDir | 0o755},
	{name: "./db/migrations/seed/orders.sql", data: "INSERT INTO orders VALUES (1);\n"},
	{name: "./db/latest", typeflag: tar.TypeSymlink, mode: fs.ModeSymlink | 0o777, data: "migrations"},
}

var packedMigrations = []sourcecontent.File{
	{Path: "0001_create.up.sql", Data: []byte("CREATE TABLE orders (id int);\r\n")},
	{Path: "0001_create.down.sql", Data: []byte("")},
	{Path: "seed/orders.sql", Data: []byte("INSERT INTO orders VALUES (1);\n")},
}

func TestRead_HappyPath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		object  []byte
		options sourcearchive.Options
		want    []sourcecontent.File
	}{
		{name: "a file", object: []byte("table \"orders\" {}\r\n"),
			options: sourcearchive.Options{Format: sourcearchive.FormatFile, Path: "schema.hcl", Kind: sourcearchive.File},
			want:    []sourcecontent.File{{Path: "schema.hcl", Data: []byte("table \"orders\" {}\r\n")}}},
		{name: "a tar.gz directory", object: tarGzip(c, packed...), options: withFormat(migrations, sourcearchive.FormatTarGzip), want: packedMigrations},
		{name: "a zip directory", object: zipOf(c, packed...), options: withFormat(migrations, sourcearchive.FormatZip), want: packedMigrations},
		{name: "a tar.gz member", object: tarGzip(c, packed...),
			options: sourcearchive.Options{Format: sourcearchive.FormatTarGzip, Path: "db/schema.hcl", Kind: sourcearchive.File},
			want:    []sourcecontent.File{{Path: "schema.hcl", Data: []byte("table \"orders\" {}\n")}}},
		{name: "a zip member", object: zipOf(c, packed...),
			options: sourcearchive.Options{Format: sourcearchive.FormatZip, Path: "db/schema.hcl", Kind: sourcearchive.File},
			want:    []sourcecontent.File{{Path: "schema.hcl", Data: []byte("table \"orders\" {}\n")}}},
		{name: "a tar.gz root", object: tarGzip(c, entry{name: "0001_a.sql", data: "a"}, entry{name: "./sub/0002_b.sql", data: "b"}),
			options: sourcearchive.Options{Format: sourcearchive.FormatTarGzip, Path: ".", Kind: sourcearchive.Directory},
			want:    []sourcecontent.File{{Path: "0001_a.sql", Data: []byte("a")}, {Path: "sub/0002_b.sql", Data: []byte("b")}}},
		{name: "a stored zip entry", object: zipOf(c, entry{name: "db/migrations/0001_a.sql", data: "a", zipRaw: true}),
			options: withFormat(migrations, sourcearchive.FormatZip),
			want:    []sourcecontent.File{{Path: "0001_a.sql", Data: []byte("a")}}},
		// An entry outside the selection is never inflated, so nothing about
		// how it is compressed or encrypted is refused.
		{name: "a zip entry outside the selection under another method", object: zipOf(c,
			entry{name: "docs/manual.pdf", data: "????", zipRaw: true, zipMethod: 12, zipFlags: 0x1},
			entry{name: "db/migrations/0001_a.sql", data: "a"}),
			options: withFormat(migrations, sourcearchive.FormatZip),
			want:    []sourcecontent.File{{Path: "0001_a.sql", Data: []byte("a")}}},
		{name: "an empty selected directory beside a selected file", object: tarGzip(c,
			entry{name: "db/migrations/empty/", typeflag: tar.TypeDir}, entry{name: "db/migrations/0001_a.sql", data: "a"}),
			options: withFormat(migrations, sourcearchive.FormatTarGzip),
			want:    []sourcecontent.File{{Path: "0001_a.sql", Data: []byte("a")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := read(test.object, test.options)

			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.DeepEquals, test.want)
		})
	}
}

func TestRead_FailurePath(t *testing.T) {
	c := qt.New(t)
	tarMigrations := withFormat(migrations, sourcearchive.FormatTarGzip)
	zipMigrations := withFormat(migrations, sourcearchive.FormatZip)
	schemaInTar := sourcearchive.Options{Format: sourcearchive.FormatTarGzip, Path: "db/schema.hcl", Kind: sourcearchive.File}
	good := entry{name: "db/migrations/0001_a.sql", data: "a"}
	tests := []struct {
		name    string
		object  []byte
		options sourcearchive.Options
		is      error
		says    string
	}{
		// Names are checked on every entry, inside the selection or not.
		{name: "an absolute name", object: tarGzip(c, good, entry{name: "/etc/cron.d/x", data: "x"}), options: tarMigrations,
			is: sourcearchive.ErrEntryName, says: `.*"/etc/cron.d/x" is absolute`},
		{name: "a .. element", object: tarGzip(c, good, entry{name: "db/../../x", data: "x"}), options: tarMigrations,
			is: sourcearchive.ErrEntryName, says: `.*"db/../../x" has the element "\.\."`},
		{name: "a backslash", object: zipOf(c, good, entry{name: `db\migrations\0002_b.sql`, data: "x"}), options: zipMigrations,
			is: sourcearchive.ErrEntryName, says: `.*holds a backslash`},
		{name: "a drive letter", object: zipOf(c, good, entry{name: "C:/db/x.sql", data: "x"}), options: zipMigrations,
			is: sourcearchive.ErrEntryName, says: `.*"C:/db/x.sql" names a drive`},
		{name: "a NUL", object: zipOf(c, good, entry{name: "db/x\x00.sql", data: "x"}), options: zipMigrations,
			is: sourcearchive.ErrEntryName, says: `.*holds a NUL`},
		// Inside the selection only regular files and directories are read.
		{name: "a symbolic link", object: tarGzip(c, good, entry{name: "db/migrations/0002_b.sql", typeflag: tar.TypeSymlink}), options: tarMigrations,
			is: sourcearchive.ErrEntryType, says: `.*"db/migrations/0002_b.sql" is a symbolic link`},
		{name: "a zip symbolic link", object: zipOf(c, good, entry{name: "db/migrations/0002_b.sql", data: "/etc/passwd", mode: fs.ModeSymlink | 0o777}),
			options: zipMigrations, is: sourcearchive.ErrEntryType, says: `.*is a symbolic link`},
		{name: "a hard link", object: tarGzip(c, good, entry{name: "db/migrations/0002_b.sql", typeflag: tar.TypeLink}), options: tarMigrations,
			is: sourcearchive.ErrEntryType, says: `.*is a hard link`},
		{name: "a device", object: tarGzip(c, good, entry{name: "db/migrations/0002_b.sql", typeflag: tar.TypeChar}), options: tarMigrations,
			is: sourcearchive.ErrEntryType, says: `.*is a device`},
		{name: "a FIFO", object: tarGzip(c, good, entry{name: "db/migrations/0002_b.sql", typeflag: tar.TypeFifo}), options: tarMigrations,
			is: sourcearchive.ErrEntryType, says: `.*is a FIFO`},
		{name: "a sparse file", object: sparse(c, "db/migrations/0002_b.sql"), options: tarMigrations,
			is: sourcearchive.ErrEntryType, says: `.*"db/migrations/0002_b.sql" is a sparse file`},
		{name: "a duplicate", object: tarGzip(c, good, good), options: tarMigrations,
			is: sourcecontent.ErrDuplicate, says: `.*"0001_a.sql"`},
		{name: "a duplicate selected file", object: tarGzip(c, entry{name: "db/schema.hcl", data: "a"}, entry{name: "./db/schema.hcl", data: "b"}),
			options: schemaInTar, is: sourcecontent.ErrDuplicate, says: `.*"schema.hcl"`},
		{name: "a directory entry under a file", object: tarGzip(c, good, entry{name: "db/migrations/0001_a.sql/sub/", typeflag: tar.TypeDir}),
			options: tarMigrations, is: sourcecontent.ErrPathConflict, says: `.*"0001_a.sql"`},
		{name: "a file and a directory at one path", object: zipOf(c, good, entry{name: "db/migrations/0001_a.sql/x.sql", data: "x"}),
			options: zipMigrations, is: sourcecontent.ErrPathConflict, says: `.*"0001_a.sql"`},
		{name: "a directory entry at a file's path", object: tarGzip(c, good, entry{name: "db/migrations/0001_a.sql/", typeflag: tar.TypeDir}),
			options: tarMigrations, is: sourcecontent.ErrPathConflict, says: `.*"0001_a.sql"`},
		{name: "too many entries", object: tarGzip(c, append([]entry{good}, numbered("vendor/%05d.txt", sourcearchive.MaxEntries)...)...),
			options: tarMigrations, is: sourcearchive.ErrTooLarge, says: `.*more than 10000 entries`},
		{name: "too many zip entries", object: zipOf(c, append([]entry{good}, numbered("vendor/%05d.txt", sourcearchive.MaxEntries)...)...),
			options: zipMigrations, is: sourcearchive.ErrTooLarge, says: `.*more than 10000 entries`},
		{name: "expansion past its bound", object: expandsPast(c), options: tarMigrations,
			is: sourcearchive.ErrTooLarge, says: `.*expands past 1073741824 bytes`},
		{name: "selected bytes past their bound", object: declaresPast(c), options: tarMigrations,
			is: sourcearchive.ErrTooLarge, says: `.*the selected files hold more than 67108864 bytes`},
		{name: "selected zip bytes past their bound", object: zipOf(c, entry{name: "db/migrations/0001_a.sql", data: "a", zipRaw: true, zipSize: sourcearchive.MaxSelectedBytes + 1}),
			options: zipMigrations, is: sourcearchive.ErrTooLarge, says: `.*the selected files hold more than 67108864 bytes`},
		{name: "selected files past their bound", object: tarGzip(c, numbered("db/migrations/%04d.sql", sourcecontent.MaxFiles+1)...),
			options: tarMigrations, is: sourcearchive.ErrTooLarge, says: `.*more than 1000 selected files`},
		{name: "an encrypted zip entry", object: zipOf(c, entry{name: "db/migrations/0001_a.sql", data: "a", zipRaw: true, zipFlags: 0x1}),
			options: zipMigrations, is: sourcearchive.ErrEncrypted, says: `.*"db/migrations/0001_a.sql"`},
		{name: "an unsupported zip method", object: zipOf(c, entry{name: "db/migrations/0001_a.sql", data: "a", zipRaw: true, zipMethod: 12}),
			options: zipMigrations, is: sourcearchive.ErrCompression, says: `.*uses method 12; store and deflate are read`},
		{name: "a zip CRC that does not match", object: zipOf(c, entry{name: "db/migrations/0001_a.sql", data: "a", zipRaw: true, zipCRC: 0xdeadbeef}),
			options: zipMigrations, is: sourcearchive.ErrCorrupt, says: `.*"db/migrations/0001_a.sql".*`},
		// archive/zip skips its own check when the archive records a CRC of 0.
		{name: "a zip CRC of 0 for content whose CRC is not", object: zipOf(c, entry{name: "db/migrations/0001_a.sql", data: "a", zipRaw: true, zipZeroCRC: true}),
			options: zipMigrations, is: sourcearchive.ErrCorrupt, says: `.*"db/migrations/0001_a.sql": CRC e8b7be43, the archive says 00000000`},
		{name: "a zip entry longer than it declares", object: zipOf(c, entry{name: "db/migrations/0001_a.sql", data: "abcd", zipRaw: true, zipSize: 2}),
			options: zipMigrations, is: sourcearchive.ErrCorrupt, says: `.*"db/migrations/0001_a.sql".*`},
		{name: "a truncated gzip stream", object: truncated(tarGzip(c, good)), options: tarMigrations,
			is: sourcearchive.ErrCorrupt, says: `.*`},
		{name: "a gzip trailer that does not match", object: badTrailer(tarGzip(c, good)), options: tarMigrations,
			is: sourcearchive.ErrCorrupt, says: `.*gzip.*`},
		{name: "not gzip", object: []byte("PK\x03\x04 a zip, perhaps"), options: tarMigrations,
			is: sourcearchive.ErrCorrupt, says: `.*gzip.*`},
		{name: "not zip", object: tarGzip(c, good), options: zipMigrations,
			is: sourcearchive.ErrCorrupt, says: `.*zip.*`},
		{name: "a missing selected path", object: tarGzip(c, entry{name: "db/schema.hcl", data: "x"}), options: tarMigrations,
			is: sourcearchive.ErrNotFound, says: `.*db/migrations`},
		{name: "a missing selected file", object: tarGzip(c, good), options: schemaInTar,
			is: sourcearchive.ErrNotFound, says: `.*db/schema.hcl`},
		{name: "a directory where a file is selected", object: tarGzip(c, entry{name: "db/schema.hcl/", typeflag: tar.TypeDir}), options: schemaInTar,
			is: sourcearchive.ErrSelection, says: `.*"db/schema.hcl" is a directory, and a file is selected`},
		{name: "files under the selected file", object: zipOf(c, entry{name: "db/schema.hcl/main.hcl", data: "x"}),
			options: sourcearchive.Options{Format: sourcearchive.FormatZip, Path: "db/schema.hcl", Kind: sourcearchive.File},
			is:      sourcearchive.ErrSelection, says: `.*is a directory, and a file is selected`},
		{name: "a file where a directory is selected", object: tarGzip(c, entry{name: "db/migrations", data: "x"}), options: tarMigrations,
			is: sourcearchive.ErrSelection, says: `.*"db/migrations" is a file, not a directory`},
		{name: "an empty selected directory", object: tarGzip(c, entry{name: "db/migrations/", typeflag: tar.TypeDir}), options: tarMigrations,
			is: sourcecontent.ErrEmpty, says: `.*`},
		{name: "an empty element in a selected name", object: tarGzip(c, entry{name: "db/migrations//0001_a.sql", data: "x"}), options: tarMigrations,
			is: sourcecontent.ErrInvalidPath, says: `.*"/0001_a.sql" has an empty element`},
		{name: "a control character in a selected name", object: zipOf(c, entry{name: "db/migrations/0001\x01.sql", data: "x"}), options: zipMigrations,
			is: sourcecontent.ErrInvalidPath, says: `.*`},
		{name: "a file over the bound", object: make([]byte, sourcearchive.MaxSelectedBytes+1),
			options: sourcearchive.Options{Format: sourcearchive.FormatFile, Path: "schema.sql", Kind: sourcearchive.File},
			is:      sourcearchive.ErrTooLarge, says: `.*more than 67108864 bytes`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := read(test.object, test.options)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.says)
			c.Assert(files, qt.IsNil)
		})
	}
}

// The name rules hold whatever GODEBUG says about insecure paths: with
// tarinsecurepath=0 and zipinsecurepath=0 archive/tar and archive/zip refuse
// such names themselves, and the refusal is still sourcearchive's.
func TestRead_RefusesInsecureNamesWhateverGODEBUGSays(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		godebug string
		object  []byte
		format  sourcearchive.Format
	}{
		{godebug: "tarinsecurepath=0", object: tarGzip(c, entry{name: "../x.sql", data: "x"}), format: sourcearchive.FormatTarGzip},
		{godebug: "tarinsecurepath=1", object: tarGzip(c, entry{name: "../x.sql", data: "x"}), format: sourcearchive.FormatTarGzip},
		{godebug: "zipinsecurepath=0", object: zipOf(c, entry{name: "../x.sql", data: "x"}), format: sourcearchive.FormatZip},
		{godebug: "zipinsecurepath=1", object: zipOf(c, entry{name: "../x.sql", data: "x"}), format: sourcearchive.FormatZip},
	}
	for _, test := range tests {
		t.Run(test.godebug, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("GODEBUG", test.godebug)

			files, err := read(test.object, withFormat(migrations, test.format))

			c.Assert(err, qt.ErrorIs, sourcearchive.ErrEntryName)
			c.Assert(files, qt.IsNil)
		})
	}
}

func TestCheck_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		options sourcearchive.Options
		is      error
		says    string
	}{
		{name: "an unknown format", options: sourcearchive.Options{Format: "tar", Path: "x", Kind: sourcearchive.File},
			is: sourcearchive.ErrFormat, says: `unknown source format "tar": give file, tar.gz or zip`},
		{name: "no path", options: sourcearchive.Options{Format: sourcearchive.FormatZip, Kind: sourcearchive.File},
			is: sourcearchive.ErrSelection, says: `selected path refused: no path given`},
		{name: "a directory from a file", options: sourcearchive.Options{Format: sourcearchive.FormatFile, Path: "migrations", Kind: sourcearchive.Directory},
			is: sourcearchive.ErrSelection, says: `.*a directory is read from an archive; give tar.gz or zip, not file`},
		{name: "a file given a path", options: sourcearchive.Options{Format: sourcearchive.FormatFile, Path: "db/schema.hcl", Kind: sourcearchive.File},
			is: sourcearchive.ErrSelection, says: `.*"db/schema.hcl": with file, the path is the file's one name`},
		{name: "a .. element", options: sourcearchive.Options{Format: sourcearchive.FormatZip, Path: "db/../x", Kind: sourcearchive.Directory},
			is: sourcecontent.ErrInvalidPath, says: `.*`},
		{name: "a file at the root", options: sourcearchive.Options{Format: sourcearchive.FormatZip, Path: ".", Kind: sourcearchive.File},
			is: sourcecontent.ErrInvalidPath, says: `.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := sourcearchive.Check(test.options)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.says)
		})
	}
}

func TestParseFormat_HappyPath(t *testing.T) {
	for _, format := range sourcearchive.Formats {
		t.Run(string(format), func(t *testing.T) {
			c := qt.New(t)
			got, err := sourcearchive.ParseFormat(string(format))
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, format)
		})
	}
}

// contentCases are the content digest cases Ptah Operator holds both
// implementations to.
type contentCases struct {
	Accepted []struct {
		Name  string `json:"name"`
		Files []struct {
			Path   string `json:"path"`
			Base64 string `json:"base64"`
		} `json:"files"`
		Digest string `json:"digest"`
	} `json:"accepted"`
}

// Every accepted case, packed into a tar.gz and a zip under the selected
// directory, reads back as files with the case's digest: the reader keeps every
// byte and every path.
func TestRead_GivesTheContentDigestOfEveryCase(t *testing.T) {
	c := qt.New(t)
	body, err := os.ReadFile(filepath.Join("..", "sourcecontent", "testdata", "cases.json"))
	c.Assert(err, qt.IsNil)
	var cases contentCases
	c.Assert(json.Unmarshal(body, &cases), qt.IsNil)
	c.Assert(len(cases.Accepted) >= 10, qt.IsTrue, qt.Commentf("%d accepted cases", len(cases.Accepted)))
	for _, fixture := range cases.Accepted {
		entries := make([]entry, 0, len(fixture.Files))
		for _, file := range fixture.Files {
			data, err := base64.StdEncoding.DecodeString(file.Base64)
			c.Assert(err, qt.IsNil)
			entries = append(entries, entry{name: "db/migrations/" + file.Path, data: string(data)})
		}
		for _, archive := range []struct {
			format sourcearchive.Format
			object []byte
		}{{sourcearchive.FormatTarGzip, tarGzip(c, entries...)}, {sourcearchive.FormatZip, zipOf(c, entries...)}} {
			t.Run(fixture.Name+"/"+string(archive.format), func(t *testing.T) {
				c := qt.New(t)
				files, err := read(archive.object, withFormat(migrations, archive.format))
				c.Assert(err, qt.IsNil)
				digest, err := sourcecontent.Digest(files)
				c.Assert(err, qt.IsNil)
				c.Assert(digest, qt.Equals, fixture.Digest)
			})
		}
	}
}

// numbered is count small files at paths format makes from 0.
func numbered(format string, count int) []entry {
	entries := make([]entry, count)
	for i := range entries {
		entries[i] = entry{name: fmt.Sprintf(format, i), data: "x"}
	}
	return entries
}

// expandsPast is a tar.gz whose one entry, outside any selection, holds a
// GiB of zeros. Its gzip stream is one member of the tar header and 64 MiB,
// then the same compressed 64 MiB again until the whole passes the bound, so
// the test compresses 128 MiB to build it.
func expandsPast(c *qt.C) []byte {
	var header bytes.Buffer
	c.Assert(tar.NewWriter(&header).WriteHeader(&tar.Header{
		Name: "padding.bin", Typeflag: tar.TypeReg, Mode: 0o644, Size: sourcearchive.MaxExpandedBytes,
	}), qt.IsNil)
	zeros := make([]byte, 64<<20)
	var first, repeated bytes.Buffer
	for _, member := range []struct {
		out   *bytes.Buffer
		parts [][]byte
	}{{&first, [][]byte{header.Bytes(), zeros}}, {&repeated, [][]byte{zeros}}} {
		writer, err := gzip.NewWriterLevel(member.out, gzip.BestSpeed)
		c.Assert(err, qt.IsNil)
		for _, part := range member.parts {
			_, err := writer.Write(part)
			c.Assert(err, qt.IsNil)
		}
		c.Assert(writer.Close(), qt.IsNil)
	}
	object := bytes.Clone(first.Bytes())
	for range sourcearchive.MaxExpandedBytes / len(zeros) {
		object = append(object, repeated.Bytes()...)
	}
	return object
}

// declaresPast is a tar.gz whose selected entry declares one byte more than
// the selection may hold, and ends after its header: the size is refused
// before any of it is read.
func declaresPast(c *qt.C) []byte {
	var object bytes.Buffer
	compressed := gzip.NewWriter(&object)
	c.Assert(tar.NewWriter(compressed).WriteHeader(&tar.Header{
		Name: "db/migrations/0001_a.sql", Typeflag: tar.TypeReg, Mode: 0o644, Size: sourcearchive.MaxSelectedBytes + 1,
	}), qt.IsNil)
	c.Assert(compressed.Close(), qt.IsNil)
	return object.Bytes()
}

// sparse is a tar.gz whose one entry is a GNU sparse file in the PAX 0.1
// format, which archive/tar reads and its writer cannot write: the writer drops
// GNU.sparse records. The extended header is written as a regular entry
// holding the records, and its type is then set to 'x'.
func sparse(c *qt.C, name string) []byte {
	var records strings.Builder
	for _, record := range [][2]string{
		{"GNU.sparse.major", "0"}, {"GNU.sparse.minor", "1"}, {"GNU.sparse.numblocks", "1"},
		{"GNU.sparse.map", "0,4"}, {"GNU.sparse.size", "4"},
	} {
		// Each record is "<length> <key>=<value>\n", its length counting
		// itself; every one here is under 100 bytes and over 9.
		body := " " + record[0] + "=" + record[1] + "\n"
		records.WriteString(strconv.Itoa(len(body)+2) + body)
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, member := range []struct{ name, data string }{{"PaxHeaders/0002_b.sql", records.String()}, {name, "abcd"}} {
		c.Assert(writer.WriteHeader(&tar.Header{Name: member.name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(member.data))}), qt.IsNil)
		_, err := writer.Write([]byte(member.data))
		c.Assert(err, qt.IsNil)
	}
	c.Assert(writer.Close(), qt.IsNil)
	raw := archive.Bytes()
	// A ustar header holds its type at byte 156 and the sum of its bytes, the
	// sum's own field read as spaces, at bytes 148 to 155.
	raw[156] = tar.TypeXHeader
	copy(raw[148:156], "        ")
	sum := 0
	for _, b := range raw[:512] {
		sum += int(b)
	}
	copy(raw[148:156], fmt.Sprintf("%06o\x00 ", sum))
	var object bytes.Buffer
	compressed := gzip.NewWriter(&object)
	_, err := compressed.Write(raw)
	c.Assert(err, qt.IsNil)
	c.Assert(compressed.Close(), qt.IsNil)
	return object.Bytes()
}

// truncated is a gzip stream cut short of its end.
func truncated(object []byte) []byte {
	return object[:len(object)-12]
}

// badTrailer is a gzip stream whose CRC in the trailer does not match.
func badTrailer(object []byte) []byte {
	broken := bytes.Clone(object)
	broken[len(broken)-8] ^= 0xff
	return broken
}
