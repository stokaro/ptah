package schemaserve_test

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/schemaserve"
)

// handler builds a handler against a database that does not exist, which is
// deliberate: every assertion below is about the surface rather than about the
// comparison, and a handler that only behaves when a database answers is the
// wrong shape.
func handler(c *qt.C) http.Handler {
	c.Helper()
	built, err := schemaserve.Handler(schemaserve.Options{
		DatabaseURL: "postgres://unreachable.invalid:1/none?sslmode=disable",
		Title:       "Test dashboard",
	})
	c.Assert(err, qt.IsNil)
	return built
}

// methodRow is one request method and the status it must receive.
type methodRow struct {
	method string
	want   int
}

// TestHandler_IsReadOnly pins the guarantee the dashboard makes about itself.
//
// A dashboard that can change a database is a different security question, and
// running one on a machine holding production credentials should not also mean
// it can change production. The refusal lives in one wrapper so a route added
// later inherits it rather than depending on somebody remembering.
func TestHandler_IsReadOnly(t *testing.T) {
	rows := []methodRow{
		{method: http.MethodGet, want: http.StatusOK},
		{method: http.MethodHead, want: http.StatusOK},
		{method: http.MethodPost, want: http.StatusMethodNotAllowed},
		{method: http.MethodPut, want: http.StatusMethodNotAllowed},
		{method: http.MethodPatch, want: http.StatusMethodNotAllowed},
		{method: http.MethodDelete, want: http.StatusMethodNotAllowed},
	}

	for _, row := range rows {
		t.Run(row.method, func(t *testing.T) {
			c := qt.New(t)
			recorder := httptest.NewRecorder()

			handler(c).ServeHTTP(recorder, httptest.NewRequest(row.method, "/", nil))

			c.Assert(recorder.Code, qt.Equals, row.want)
		})
	}
}

// TestHandler_SaysWhenItCannotReachTheDatabase pins that a failed comparison is
// named.
//
// Rendering zero drift when the database could not be read would be the worst
// answer available: a reader would see a green page and conclude their schema
// is in sync.
func TestHandler_SaysWhenItCannotReachTheDatabase(t *testing.T) {
	c := qt.New(t)
	recorder := httptest.NewRecorder()

	handler(c).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	body := recorder.Body.String()
	c.Assert(body, qt.Contains, "The database could not be compared")
	c.Assert(body, qt.Not(qt.Contains), "matches the declared schema")
}

// TestHandler_RefusesWithoutADatabase pins that the missing URL is an error at
// construction rather than a page that explains itself on every request.
func TestHandler_RefusesWithoutADatabase(t *testing.T) {
	c := qt.New(t)

	_, err := schemaserve.Handler(schemaserve.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "database URL is required")
}

// TestHandler_CarriesNoScript pins that the page reloads itself without
// JavaScript, the same way the exported document renders without any.
func TestHandler_CarriesNoScript(t *testing.T) {
	c := qt.New(t)
	built, err := schemaserve.Handler(schemaserve.Options{
		DatabaseURL: "postgres://unreachable.invalid:1/none?sslmode=disable",
		Refresh:     15_000_000_000,
	})
	c.Assert(err, qt.IsNil)
	recorder := httptest.NewRecorder()

	built.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	body := recorder.Body.String()
	c.Assert(body, qt.Contains, `<meta http-equiv="refresh" content="15">`)
	c.Assert(body, qt.Not(qt.Contains), "<script")
}

// TestHandler_KeepsCredentialsOffThePage pins that the database a reader is
// looking at is named without the password that reaches it, because a
// dashboard that printed one would put it in every screenshot of itself.
func TestHandler_KeepsCredentialsOffThePage(t *testing.T) {
	c := qt.New(t)
	built, err := schemaserve.Handler(schemaserve.Options{
		DatabaseURL: "postgres://someone:hunter2@db.invalid:5432/app?sslmode=disable",
	})
	c.Assert(err, qt.IsNil)
	recorder := httptest.NewRecorder()

	built.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	c.Assert(recorder.Body.String(), qt.Not(qt.Contains), "hunter2")
}

// TestHandler_ResolvesEveryCustomPropertyItUses is what keeps this view's
// stylesheet honest about the one it is added to.
//
// The page's appearance is internal/schemadoc's tokens plus the arrangement
// this view needs, and only the second half lives here. A var() naming a token
// that stylesheet stopped declaring does not fail: the browser discards the
// whole declaration and says nothing, so a retired token leaves a dashboard
// that renders, renders wrongly, and passes every other test in this file.
func TestHandler_ResolvesEveryCustomPropertyItUses(t *testing.T) {
	c := qt.New(t)
	recorder := httptest.NewRecorder()

	handler(c).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	c.Assert(recorder.Code, qt.Equals, http.StatusOK)
	styles := regexp.MustCompile(`(?s)<style>(.*?)</style>`).FindStringSubmatch(recorder.Body.String())
	c.Assert(styles, qt.HasLen, 2, qt.Commentf("the page carries exactly one stylesheet"))

	declared := make(map[string]bool)
	for _, match := range regexp.MustCompile(`(--[a-z0-9-]+)\s*:`).FindAllStringSubmatch(styles[1], -1) {
		declared[match[1]] = true
	}
	used := make(map[string]bool)
	for _, match := range regexp.MustCompile(`var\((--[a-z0-9-]+)`).FindAllStringSubmatch(styles[1], -1) {
		used[match[1]] = true
	}
	c.Assert(len(used) > 0, qt.IsTrue, qt.Commentf("the stylesheet uses no tokens at all"))
	for _, token := range slices.Sorted(maps.Keys(used)) {
		c.Assert(declared[token], qt.IsTrue,
			qt.Commentf("var(%s) resolves to nothing: no block declares it", token))
	}
}
