//go:build integration

package integration_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/httpsource"
	"ptah.run/internal/sourcecontent"
)

// httpsPullReport is the part of an HTTPS pull's --json document a consumer,
// such as Ptah Operator's fetch, reads.
type httpsPullReport struct {
	Source         string `json:"source"`
	Format         string `json:"format"`
	DownloadDigest string `json:"download_digest"`
	DownloadSize   int64  `json:"download_size"`
	Path           string `json:"path"`
	ContentDigest  string `json:"content_digest"`
	Files          []struct {
		Path string `json:"path"`
		Size int    `json:"size"`
	} `json:"files"`
}

const httpsToken = "artifact-token"

// downloads is an artifact server: it serves each body at its path to a
// request that carries the Bearer token, and nothing else.
type downloads struct {
	*httptest.Server
	caFile string
}

func serveDownloads(c *qt.C, bodies map[string][]byte) downloads {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, found := bodies[r.URL.Path]
		switch {
		case r.Header.Get("Authorization") != "Bearer "+httpsToken:
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		case !found:
			http.NotFound(w, r)
		default:
			_, _ = w.Write(body)
		}
	}))
	c.Cleanup(server.Close)
	caFile := filepath.Join(c.TempDir(), "ca.pem")
	c.Assert(os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600), qt.IsNil)
	return downloads{Server: server, caFile: caFile}
}

func downloadDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// tarGzipOf packs the files in the order given, each name as written, the way
// `tar -C dir -czf` names them with a leading ./.
func tarGzipOf(c *qt.C, files [][2]string) []byte {
	var buffer bytes.Buffer
	compressed := gzip.NewWriter(&buffer)
	archive := tar.NewWriter(compressed)
	for _, file := range files {
		c.Assert(archive.WriteHeader(&tar.Header{Name: file[0], Mode: 0o644, Size: int64(len(file[1])), Typeflag: tar.TypeReg}), qt.IsNil)
		_, err := archive.Write([]byte(file[1]))
		c.Assert(err, qt.IsNil)
	}
	c.Assert(archive.Close(), qt.IsNil)
	c.Assert(compressed.Close(), qt.IsNil)
	return buffer.Bytes()
}

func zipOfFiles(c *qt.C, files [][2]string) []byte {
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	for _, file := range files {
		writer, err := archive.Create(file[0])
		c.Assert(err, qt.IsNil)
		_, err = writer.Write([]byte(file[1]))
		c.Assert(err, qt.IsNil)
	}
	c.Assert(archive.Close(), qt.IsNil)
	return buffer.Bytes()
}

// The pull commands read a schema file served as itself and from a zip, and a
// migration directory from a tar.gz, each pinned by its download digest; they
// write the selected bytes and report both digests. A later pull held to the
// content digest it reported is admitted.
func TestHTTPSPullCommandsE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	up, down := "CREATE TABLE t (id int);\r\n", "DROP TABLE t;\n"
	schemaZip := zipOfFiles(c, [][2]string{{"app-1.4.0/README", "unread"}, {"app-1.4.0/db/schema.hcl", schemaV1}})
	migrationsTarGzip := tarGzipOf(c, [][2]string{
		{"./app-1.4.0/db/migrations/0001_create.up.sql", up},
		{"./app-1.4.0/db/migrations/0001_create.down.sql", down},
		{"./app-1.4.0/db/schema.hcl", schemaV1},
	})
	server := serveDownloads(c, map[string][]byte{
		"/app/1.4.0/schema":     []byte(schemaV1),
		"/app/1.4.0/app.zip":    schemaZip,
		"/app/1.4.0/app.tar.gz": migrationsTarGzip,
	})
	t.Setenv(httpsource.TokenEnv, httpsToken)
	dir := c.TempDir()

	schemaOut := filepath.Join(dir, "schema.hcl")
	var schema httpsPullReport
	c.Assert(json.Unmarshal(runPtahStdout(c, "schema", "pull", server.URL+"/app/1.4.0/schema",
		"--format", "file", "--path", "schema.hcl", "--expect-download-digest", downloadDigest([]byte(schemaV1)),
		"--ca-file", server.caFile, "--out", schemaOut, "--json"), &schema), qt.IsNil)

	c.Assert(schema.Source, qt.Equals, "https")
	c.Assert(schema.Format, qt.Equals, "file")
	c.Assert(schema.DownloadDigest, qt.Equals, downloadDigest([]byte(schemaV1)))
	c.Assert(schema.DownloadSize, qt.Equals, int64(len(schemaV1)))
	c.Assert(schema.ContentDigest, qt.Equals, schemaDigest(c, schemaV1))
	written, err := os.ReadFile(schemaOut)
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Equals, schemaV1)

	// The same file from inside a zip has the same content digest and another
	// download digest.
	zippedOut := filepath.Join(dir, "zipped.hcl")
	var zipped httpsPullReport
	c.Assert(json.Unmarshal(runPtahStdout(c, "schema", "pull", server.URL+"/app/1.4.0/app.zip",
		"--format", "zip", "--path", "app-1.4.0/db/schema.hcl", "--expect-download-digest", downloadDigest(schemaZip),
		"--ca-file", server.caFile, "--out", zippedOut, "--json"), &zipped), qt.IsNil)

	c.Assert(zipped.DownloadDigest, qt.Equals, downloadDigest(schemaZip))
	c.Assert(zipped.ContentDigest, qt.Equals, schema.ContentDigest)
	written, err = os.ReadFile(zippedOut)
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Equals, schemaV1)

	migrationsOut := filepath.Join(dir, "migrations")
	var migrations httpsPullReport
	c.Assert(json.Unmarshal(runPtahStdout(c, "migrations", "pull", server.URL+"/app/1.4.0/app.tar.gz",
		"--format", "tar.gz", "--path", "app-1.4.0/db/migrations", "--expect-download-digest", downloadDigest(migrationsTarGzip),
		"--ca-file", server.caFile, "--out", migrationsOut, "--json"), &migrations), qt.IsNil)

	c.Assert(migrations.Format, qt.Equals, "tar.gz")
	c.Assert(migrations.Path, qt.Equals, "app-1.4.0/db/migrations")
	c.Assert(migrations.Files, qt.HasLen, 2)
	written, err = os.ReadFile(filepath.Join(migrationsOut, "0001_create.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Equals, up, qt.Commentf("line endings are kept as packed"))
	digest, err := sourcecontent.Digest([]sourcecontent.File{
		{Path: "0001_create.up.sql", Data: []byte(up)},
		{Path: "0001_create.down.sql", Data: []byte(down)},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(migrations.ContentDigest, qt.Equals, digest)

	// What an operator does for every operation after the first: the same
	// download, held to the content digest it was planned from.
	var again httpsPullReport
	c.Assert(json.Unmarshal(runPtahStdout(c, "migrations", "pull", server.URL+"/app/1.4.0/app.tar.gz",
		"--format", "tar.gz", "--path", "app-1.4.0/db/migrations", "--expect-download-digest", downloadDigest(migrationsTarGzip),
		"--expect-digest", digest, "--ca-file", server.caFile, "--out", filepath.Join(dir, "again"), "--json"), &again), qt.IsNil)
	c.Assert(again.ContentDigest, qt.Equals, digest)
}

func TestHTTPSPullCommandsE2E_FailurePath(t *testing.T) {
	t.Run("other bytes than the download digest names", func(t *testing.T) {
		c := qt.New(t)
		server := serveDownloads(c, map[string][]byte{"/schema": []byte(schemaV2)})
		t.Setenv(httpsource.TokenEnv, httpsToken)
		out := filepath.Join(c.TempDir(), "schema.hcl")

		printed, err := runPtahNativeWithError("schema", "pull", server.URL+"/schema",
			"--format", "file", "--path", "schema.hcl", "--expect-download-digest", downloadDigest([]byte(schemaV1)),
			"--ca-file", server.caFile, "--out", out)

		c.Assert(err, qt.ErrorIs, httpsource.ErrDownloadDigest)
		c.Assert(printed, qt.Contains, "download digest differs")
		_, err = os.Lstat(out)
		c.Assert(os.IsNotExist(err), qt.IsTrue, qt.Commentf("a refused pull wrote %s", out))
	})
	t.Run("another content digest than expected", func(t *testing.T) {
		c := qt.New(t)
		server := serveDownloads(c, map[string][]byte{"/schema": []byte(schemaV2)})
		t.Setenv(httpsource.TokenEnv, httpsToken)
		out := filepath.Join(c.TempDir(), "schema.hcl")

		printed, err := runPtahNativeWithError("schema", "pull", server.URL+"/schema",
			"--format", "file", "--path", "schema.hcl", "--expect-download-digest", downloadDigest([]byte(schemaV2)),
			"--expect-digest", schemaDigest(c, schemaV1), "--ca-file", server.caFile, "--out", out)

		c.Assert(err, qt.ErrorIs, httpsource.ErrDigest)
		c.Assert(printed, qt.Contains, "content digest differs")
		_, err = os.Lstat(out)
		c.Assert(os.IsNotExist(err), qt.IsTrue, qt.Commentf("a refused pull wrote %s", out))
	})
	t.Run("no credential", func(t *testing.T) {
		c := qt.New(t)
		server := serveDownloads(c, map[string][]byte{"/schema": []byte(schemaV1)})

		printed, err := runPtahNativeWithError("schema", "pull", server.URL+"/schema",
			"--format", "file", "--path", "schema.hcl", "--expect-download-digest", downloadDigest([]byte(schemaV1)),
			"--ca-file", server.caFile, "--out", filepath.Join(c.TempDir(), "schema.hcl"))

		c.Assert(err, qt.ErrorIs, httpsource.ErrStatus)
		c.Assert(printed, qt.Contains, "401")
		c.Assert(printed, qt.Not(qt.Contains), "unauthorized", qt.Commentf("the response body is never printed"))
	})
	t.Run("a Git flag on an https reference", func(t *testing.T) {
		c := qt.New(t)
		printed, err := runPtahNativeWithError("migrations", "pull", "https://artifacts.example.com/app.tar.gz",
			"--branch", "main", "--out", filepath.Join(c.TempDir(), "migrations"))
		c.Assert(err, qt.ErrorMatches, `--branch is for a git\+https:// or git\+ssh:// reference`)
		c.Assert(printed, qt.Contains, "--branch is for a git+https:// or git+ssh:// reference")
	})
}
