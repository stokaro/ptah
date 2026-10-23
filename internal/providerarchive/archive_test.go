package providerarchive_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerarchive"
	"ptah.run/internal/providerarchive/archivetest"
)

func write(c *qt.C, name string, data []byte) string {
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, data, 0o600), qt.IsNil)
	return path
}

func TestCopyExecutable_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		archive string
		data    []byte
	}{
		{"tar.gz", "a.tar.gz", archivetest.TarGz(
			archivetest.Entry{Name: "README.md", Body: "docs"},
			archivetest.Entry{Name: "./tool", Body: "binary"},
		)},
		{"zip", "a.zip", archivetest.Zip(
			archivetest.Entry{Name: "README.md", Body: "docs"},
			archivetest.Entry{Name: "tool", Body: "binary"},
		)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer
			c.Assert(providerarchive.CopyExecutable(write(c, test.archive, test.data), "tool", &out), qt.IsNil)
			c.Assert(out.String(), qt.Equals, "binary")
		})
	}
}

func TestCopyExecutable_FailurePath(t *testing.T) {
	tool := archivetest.Entry{Name: "tool", Body: "binary"}
	for _, test := range []struct {
		name    string
		archive string
		data    []byte
		want    error
	}{
		{"missing tar", "a.tar.gz", archivetest.TarGz(archivetest.Entry{Name: "other", Body: "x"}), providerarchive.ErrMissingExecutable},
		{"missing zip", "a.zip", archivetest.Zip(archivetest.Entry{Name: "other", Body: "x"}), providerarchive.ErrMissingExecutable},
		{"nested executable is not the top-level one", "a.tar.gz", archivetest.TarGz(archivetest.Entry{Name: "dir/tool", Body: "x"}), providerarchive.ErrMissingExecutable},
		{"parent element tar", "a.tar.gz", archivetest.TarGz(tool, archivetest.Entry{Name: "../evil", Body: "x"}), providerarchive.ErrUnsafePath},
		{"parent element zip", "a.zip", archivetest.Zip(tool, archivetest.Entry{Name: "a/../../evil", Body: "x"}), providerarchive.ErrUnsafePath},
		{"absolute tar", "a.tar.gz", archivetest.TarGz(tool, archivetest.Entry{Name: "/etc/evil", Body: "x"}), providerarchive.ErrUnsafePath},
		{"backslash zip", "a.zip", archivetest.Zip(tool, archivetest.Entry{Name: `..\evil`, Body: "x"}), providerarchive.ErrUnsafePath},
		{"drive letter zip", "a.zip", archivetest.Zip(tool, archivetest.Entry{Name: `C:evil`, Body: "x"}), providerarchive.ErrUnsafePath},
		{"duplicate tar", "a.tar.gz", archivetest.TarGz(tool, tool), providerarchive.ErrDuplicateExecutable},
		{"duplicate zip", "a.zip", archivetest.Zip(tool, tool), providerarchive.ErrDuplicateExecutable},
		{"symlink tar", "a.tar.gz", archivetest.TarGz(archivetest.Entry{Name: "tool", Link: "/bin/sh"}), providerarchive.ErrNotRegular},
		{"symlink zip", "a.zip", archivetest.Zip(archivetest.Entry{Name: "tool", Link: "/bin/sh"}), providerarchive.ErrNotRegular},
		{"unsupported format", "a.rar", []byte("x"), providerarchive.ErrUnsupportedFormat},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := providerarchive.CopyExecutable(write(c, test.archive, test.data), "tool", &bytes.Buffer{})
			c.Assert(err, qt.ErrorIs, test.want)
		})
	}
}

func TestCopyExecutable_CorruptArchive(t *testing.T) {
	good := archivetest.TarGz(archivetest.Entry{Name: "tool", Body: "binary"})
	for _, test := range []struct {
		name    string
		archive string
		data    []byte
	}{
		{"truncated gzip", "a.tar.gz", good[:len(good)/2]},
		{"not gzip", "a.tar.gz", []byte("plain text")},
		{"not zip", "a.zip", []byte("plain text")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := providerarchive.CopyExecutable(write(c, test.archive, test.data), "tool", &bytes.Buffer{})
			c.Assert(err, qt.IsNotNil)
		})
	}
}
