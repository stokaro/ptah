package ydbmonitor_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbmonitor"
)

func TestRead_RejectsOversizedDescription(t *testing.T) {
	c := qt.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 5<<20)))
	}))
	defer server.Close()
	endpoint, err := url.Parse(server.URL)
	c.Assert(err, qt.IsNil)
	body, err := ydbmonitor.Read(context.Background(), endpoint, "column table", "/viewer/json/describe", "/local", "", nil)
	c.Assert(err, qt.ErrorMatches, `read YDB column table from .*: response exceeds 4194304 bytes`)
	c.Assert(body, qt.IsNil)
}
