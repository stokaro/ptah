// Package archivetest builds small provider archives for tests. The bytes are
// deterministic: entries are stored without compression and carry fixed times,
// so a digest computed from a fixture does not move with the Go toolchain.
package archivetest

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"time"
)

// Entry is one archive member. A non-empty Link makes it a symbolic link.
type Entry struct {
	Name string
	Body string
	Link string
}

var epoch = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// TarGz returns a ".tar.gz" archive holding entries in order.
func TarGz(entries ...Entry) []byte {
	var out bytes.Buffer
	gz, _ := gzip.NewWriterLevel(&out, gzip.NoCompression)
	writer := tar.NewWriter(gz)
	for _, e := range entries {
		header := &tar.Header{Name: e.Name, Mode: 0o755, Size: int64(len(e.Body)), ModTime: epoch, Typeflag: tar.TypeReg}
		header.Typeflag, header.Linkname = linkType(e)
		if e.Link != "" {
			header.Size = 0
		}
		_ = writer.WriteHeader(header)
		_, _ = writer.Write([]byte(e.Body)[:header.Size])
	}
	_ = writer.Close()
	_ = gz.Close()
	return out.Bytes()
}

func linkType(e Entry) (byte, string) {
	return map[bool]byte{true: tar.TypeSymlink, false: tar.TypeReg}[e.Link != ""], e.Link
}

// Zip returns a ".zip" archive holding entries in order.
func Zip(entries ...Entry) []byte {
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	for _, e := range entries {
		header := &zip.FileHeader{Name: e.Name, Method: zip.Store, Modified: epoch}
		header.SetMode(map[bool]fs.FileMode{true: fs.ModeSymlink | 0o777, false: 0o755}[e.Link != ""])
		file, _ := writer.CreateHeader(header)
		_, _ = file.Write([]byte(e.Body + e.Link))
	}
	_ = writer.Close()
	return out.Bytes()
}

// SHA256 returns the lowercase hexadecimal SHA-256 digest of data.
func SHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
