package schemaserve_test

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/schemaserve"
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

// registrySourceRow is one --schema-file value naming a registry artifact.
type registrySourceRow struct {
	name        string
	schemaFiles []string
}

// TestHandler_RefusesARegistrySchemaSource_FailurePath pins that an oci://
// source is refused where the server starts.
//
// The refusal is the decision this view makes about its own contract: it reads
// its source again on every request, and a registry artifact fits neither
// reading of that. Deciding by omission is what left the whole flag out
// (stokaro/ptah#3103), so the decision is stated and asserted instead.
func TestHandler_RefusesARegistrySchemaSource_FailurePath(t *testing.T) {
	rows := []registrySourceRow{
		{name: "alone", schemaFiles: []string{"oci://ghcr.io/acme/schema:v1"}},
		{name: "padded", schemaFiles: []string{"  oci://ghcr.io/acme/schema:v1"}},
		// Second in the list, because a check that reads only the first entry
		// passes every single-source row above it.
		{name: "after a local file", schemaFiles: []string{"schema.sql", "oci://ghcr.io/acme/schema:v1"}},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			built, err := schemaserve.Handler(schemaserve.Options{
				DatabaseURL: "postgres://unreachable.invalid:1/none?sslmode=disable",
				SchemaFiles: row.schemaFiles,
			})

			c.Assert(err, qt.ErrorIs, schemaserve.ErrRegistrySchemaSource)
			c.Assert(err, qt.ErrorMatches, `(?s).*ptah schema drift --schema-file .*`)
			c.Assert(built, qt.IsNil)
		})
	}
}

// TestHandler_AcceptsALocalSchemaFile_HappyPath is the control for the refusal
// above: it passes whether or not the oci:// check exists, and fails if that
// check grew into a refusal of schema files as a category.
func TestHandler_AcceptsALocalSchemaFile_HappyPath(t *testing.T) {
	rows := []registrySourceRow{
		{name: "sql", schemaFiles: []string{"schema.sql"}},
		{name: "yaml", schemaFiles: []string{"schema.yaml"}},
		{name: "hcl", schemaFiles: []string{"schema.hcl"}},
		{name: "dbml", schemaFiles: []string{"schema.dbml"}},
		// A path is not refused for the substring it contains.
		{name: "a directory named oci", schemaFiles: []string{"oci/schema.sql"}},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			built, err := schemaserve.Handler(schemaserve.Options{
				DatabaseURL: "postgres://unreachable.invalid:1/none?sslmode=disable",
				SchemaFiles: row.schemaFiles,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(built, qt.IsNotNil)
		})
	}
}

// writeSchemaFile writes a desired-schema file and returns its path.
func writeSchemaFile(c *qt.C, dir, body string) string {
	c.Helper()
	path := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// fileBackedHandler serves a schema file against an empty SQLite database, so
// every table the file declares is drift the page has to show.
func fileBackedHandler(c *qt.C, schemaFile string) http.Handler {
	c.Helper()
	built, err := schemaserve.Handler(schemaserve.Options{
		DatabaseURL: atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "app.db")),
		SchemaFiles: []string{schemaFile},
	})
	c.Assert(err, qt.IsNil)
	return built
}

// get renders the page once.
func get(c *qt.C, built http.Handler) string {
	c.Helper()
	recorder := httptest.NewRecorder()
	built.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	c.Assert(recorder.Code, qt.Equals, http.StatusOK)
	return recorder.Body.String()
}

// TestHandler_ServesTheSchemaFileItWasGiven_HappyPath pins that a schema file
// reaches the rendered page.
//
// The negative half is what makes it discriminate: a page that named every
// table in the world would satisfy the first assertion alone.
func TestHandler_ServesTheSchemaFileItWasGiven_HappyPath(t *testing.T) {
	c := qt.New(t)
	schemaFile := writeSchemaFile(c, c.TempDir(),
		"CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n")

	body := get(c, fileBackedHandler(c, schemaFile))

	c.Assert(body, qt.Contains, "products")
	c.Assert(body, qt.Not(qt.Contains), "invoices")
}

// TestHandler_ReadsTheSchemaFileOnEveryRequest is the contract that makes a
// schema file servable at all.
//
// An annotation root is re-scanned on every request, and this asserts a schema
// file is read on the same schedule. A handler that loaded its file once would
// serve a stale schema for as long as it ran, and every other test in this file
// would still pass: the first page is the control that says the second page's
// table arrived from the edit.
func TestHandler_ReadsTheSchemaFileOnEveryRequest(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	schemaFile := writeSchemaFile(c, dir, "CREATE TABLE products (id INTEGER PRIMARY KEY);\n")
	built := fileBackedHandler(c, schemaFile)

	before := get(c, built)
	writeSchemaFile(c, dir,
		"CREATE TABLE products (id INTEGER PRIMARY KEY);\nCREATE TABLE invoices (id INTEGER PRIMARY KEY);\n")
	after := get(c, built)

	c.Assert(before, qt.Not(qt.Contains), "invoices")
	c.Assert(after, qt.Contains, "invoices")
}

// writeAnnotationRoot writes a Go annotation root declaring one table and
// returns its directory.
func writeAnnotationRoot(c *qt.C, table string) string {
	c.Helper()
	dir := c.TempDir()
	source := "package models\n\n" +
		"//ptah:schema:table name=\"" + table + "\"\n" +
		"type Model struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary\n" +
		"\tID int `db:\"id\"`\n" +
		"}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(source), 0o600), qt.IsNil)
	return dir
}

// TestHandler_MergesASchemaFileWithAnAnnotationRoot_HappyPath pins that the two
// sources combine, which is what the flag help promises and what the source
// register records as a composite source.
//
// Asserting only the merged page would pass on a handler that dropped either
// source, so each table is asserted by name.
func TestHandler_MergesASchemaFileWithAnAnnotationRoot_HappyPath(t *testing.T) {
	c := qt.New(t)
	schemaFile := writeSchemaFile(c, c.TempDir(), "CREATE TABLE invoices (id INTEGER PRIMARY KEY);\n")
	built, err := schemaserve.Handler(schemaserve.Options{
		DatabaseURL: atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "app.db")),
		RootDirs:    []string{writeAnnotationRoot(c, "products")},
		SchemaFiles: []string{schemaFile},
	})
	c.Assert(err, qt.IsNil)

	body := get(c, built)

	c.Assert(body, qt.Contains, "invoices")
	c.Assert(body, qt.Contains, "products")
}
