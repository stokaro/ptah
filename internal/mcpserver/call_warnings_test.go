package mcpserver_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// warningsKey is where a call carries the warnings it raised. The literal is
// repeated for the reason [diagnosticKey] is: it is a wire contract.
const warningsKey = "ptah.run/warnings"

// cutNamesWarning is the warning a call raises for count index names
// PostgreSQL cuts, in the words of the MCP surface.
func cutNamesWarning(count string) string {
	return count + " names are longer than 63 bytes; the server keeps their first 63 bytes, and they are read" +
		" that way; the result's _meta lists them under " + warningsKey
}

// longIndexNames is a SQL schema with an index name per letter, each 68 bytes
// and distinct in its first byte, so PostgreSQL keeps 63 bytes of each and
// tells them apart.
func longIndexNames(letters string) string {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE t (a int);\n")
	for _, letter := range letters {
		sql.WriteString(`CREATE INDEX "` + string(letter) + strings.Repeat("x", 63) + `_idx" ON t (a);` + "\n")
	}
	return sql.String()
}

// validateFile calls validate_schema on name in dir for PostgreSQL.
func validateFile(c *qt.C, session *mcp.ClientSession, dir, name string) *mcp.CallToolResult {
	c.Helper()
	result := call(c, session, "validate_schema", map[string]any{
		"dialect": "postgres",
		"source":  map[string]any{"schema_files": []string{filepath.Join(dir, name)}},
	})
	c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, result)))
	return result
}

// callWarnings is what a result's _meta carries under warningsKey, read as a
// client program reads it.
func callWarnings(c *qt.C, result *mcp.CallToolResult) []map[string]any {
	c.Helper()
	encoded, err := json.Marshal(result.Meta[warningsKey])
	c.Assert(err, qt.IsNil)
	var warnings []map[string]any
	c.Assert(json.Unmarshal(encoded, &warnings), qt.IsNil)
	return warnings
}

// Each call carries the warnings it raised: a second call that reads the same
// file raises them again and says so, rather than finding them counted by the
// first. A model reads them after the answer in the content, and a program
// reads the category, the count and every object in _meta.
func TestServer_ReturnsTheWarningsACallRaisesInItsResult(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(writeFile(dir, "schema.sql", longIndexNames("abc")), qt.IsNil)
	session := connect(c, readOnlyConfig(c, dir), nil)

	for range 2 {
		result := validateFile(c, session, dir, "schema.sql")

		c.Assert(result.Content, qt.HasLen, 2)
		c.Assert(result.Content[1], qt.DeepEquals, &mcp.TextContent{Text: "warning: " + cutNamesWarning("3")})
		warnings := callWarnings(c, result)
		c.Assert(warnings, qt.HasLen, 1)
		c.Assert(warnings[0]["category"], qt.Equals, "truncated-name")
		c.Assert(warnings[0]["count"], qt.Equals, float64(3))
		c.Assert(warnings[0]["message"], qt.Equals, cutNamesWarning("3"))
		c.Assert(warnings[0]["objects"], qt.HasLen, 3)
	}
}

// The answer stays where it was: the first content item is the structured
// content as JSON, which a client without structured content reads.
func TestServer_KeepsTheAnswerFirstBesideWarnings(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(writeFile(dir, "schema.sql", longIndexNames("ab")), qt.IsNil)
	session := connect(c, readOnlyConfig(c, dir), nil)

	result := validateFile(c, session, dir, "schema.sql")

	answer, isText := result.Content[0].(*mcp.TextContent)
	c.Assert(isText, qt.IsTrue)
	structured, err := json.Marshal(result.StructuredContent)
	c.Assert(err, qt.IsNil)
	c.Assert(answer.Text, qt.JSONEquals, json.RawMessage(structured))
}

// One object is its own line, as on a terminal.
func TestServer_ReturnsOneObjectAsItsOwnLine(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(writeFile(dir, "schema.sql", longIndexNames("a")), qt.IsNil)
	session := connect(c, readOnlyConfig(c, dir), nil)

	result := validateFile(c, session, dir, "schema.sql")

	warnings := callWarnings(c, result)
	c.Assert(warnings, qt.HasLen, 1)
	c.Assert(warnings[0]["count"], qt.Equals, float64(1))
	c.Assert(warnings[0]["message"], qt.Matches, `schema file .*schema\.sql: index "ax{63}_idx" in schema "public" is 68 bytes; the server keeps its first 63: "ax{62}"`)
	c.Assert(warnings[0]["objects"], qt.DeepEquals, []any{warnings[0]["message"]})
}

// A call that raises nothing carries nothing: the content is the answer alone
// and _meta has no warnings key.
func TestServer_CarriesNoWarningsOnACleanCall(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(writeFile(dir, "schema.sql", "CREATE TABLE t (a int);\n"), qt.IsNil)
	session := connect(c, readOnlyConfig(c, dir), nil)

	result := validateFile(c, session, dir, "schema.sql")

	c.Assert(result.Content, qt.HasLen, 1)
	_, carried := result.Meta[warningsKey]
	c.Assert(carried, qt.IsFalse)
}
