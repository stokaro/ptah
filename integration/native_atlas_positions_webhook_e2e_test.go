//go:build integration

package integration_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
)

// webhookBodies starts a local listener that answers every request with 200
// and returns its URL and a function reporting the bodies it received, in
// order.
func webhookBodies(c *qt.C) (url string, bodies func() []string) {
	c.Helper()
	var mu sync.Mutex
	var received []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, string(body))
		mu.Unlock()
		c.Check(err, qt.IsNil)
	}))
	c.Cleanup(server.Close)
	return server.URL, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), received...)
	}
}

// TestNativeAtlasPositionsWebhookE2E posts the payload of two runs of
// `migrations up --webhook`. In a directory whose files run under their
// positions, 1_x, 2_x, 10_x and v1_x run as 10_x, 1_x, 2_x and v1_x, so the
// second run ends at position 4, a number no file carries: the payload names
// the migrations by their keys and carries no number. A numbered directory is
// the control, and its payload keeps the numeric members.
func TestNativeAtlasPositionsWebhookE2E(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		want  []string
	}{
		{
			name:  "positions",
			files: mixedPositionFiles,
			want: []string{
				`{"direction":"up","dialect":"sqlite","current_version_key":"","target_version_key":"1","database_url":"sqlite://app.db"}`,
				`{"direction":"up","dialect":"sqlite","current_version_key":"1","target_version_key":"v1","database_url":"sqlite://app.db"}`,
			},
		},
		{
			name:  "numbered",
			files: []string{"1_x.sql", "2_x.sql", "3_x.sql", "4_x.sql"},
			want: []string{
				`{"direction":"up","dialect":"sqlite","current_version":0,"target_version":2,"database_url":"sqlite://app.db"}`,
				`{"direction":"up","dialect":"sqlite","current_version":2,"target_version":4,"database_url":"sqlite://app.db"}`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writePositionFiles(c, dir, tt.files)
			url, bodies := webhookBodies(c)

			first := nativePtah(c, dir, "migrations", "up", "--limit", "2", "--webhook", url)
			second := nativePtah(c, dir, "migrations", "up", "--webhook", url)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", first.Stderr))
			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", second.Stderr))
			c.Assert(bodies(), qt.DeepEquals, tt.want)
		})
	}
}

// writeReversibleFiles writes files, each a txtar body that creates a table
// and drops it again, with their atlas.sum into dir.
func writeReversibleFiles(c *qt.C, dir string, files []string) {
	c.Helper()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	for _, name := range files {
		table := "t_" + strings.NewReplacer(".", "_").Replace(strings.TrimSuffix(name, ".sql"))
		body := fmt.Sprintf("-- atlas:txtar\n\n-- migration.sql --\nCREATE TABLE %[1]s (id int);\n\n-- down.sql --\nDROP TABLE %[1]s;\n", table)
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	hashed := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())("migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
}

// TestNativeAtlasPositionsDownWebhookE2E posts the payload of `migrations down
// --target 1 --webhook` from the last file. In the positional directory the
// rollback starts at v1_x.sql, position 4, and ends at 1_x.sql, position 2,
// and the payload names both by their keys. The numbered directory is the
// control.
func TestNativeAtlasPositionsDownWebhookE2E(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		want  string
	}{
		{
			name:  "positions",
			files: mixedPositionFiles,
			want:  `{"direction":"down","dialect":"sqlite","current_version_key":"v1","target_version_key":"1","database_url":"sqlite://app.db"}`,
		},
		{
			name:  "numbered",
			files: []string{"1_x.sql", "2_x.sql", "3_x.sql", "4_x.sql"},
			want:  `{"direction":"down","dialect":"sqlite","current_version":4,"target_version":1,"database_url":"sqlite://app.db"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writeReversibleFiles(c, dir, tt.files)
			url, bodies := webhookBodies(c)
			up := nativePtah(c, dir, "migrations", "up")
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))

			down := nativePtah(c, dir, "migrations", "down", "--target", "1", "--confirm", "--webhook", url)

			c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", down.Stderr))
			c.Assert(bodies(), qt.DeepEquals, []string{tt.want})
		})
	}
}
