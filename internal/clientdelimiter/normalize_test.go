package clientdelimiter_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clientdelimiter"
)

// Every word of the rewritten text stands at the same word of the written text:
// a removed directive line, a terminator rewritten to a semicolon and a
// terminator dropped each move what follows.
func TestOrigins_FindTheWrittenWord(t *testing.T) {
	tests := []struct {
		name   string
		script string
		words  []string
	}{
		{
			name:   "no directive",
			script: "CREATE TABLE a (id int);\nCREATE TABLE b (id int);\n",
			words:  []string{"CREATE TABLE a", "CREATE TABLE b"},
		},
		{
			name:   "a client directive and its reset",
			script: "CREATE TABLE a (id int);\nDELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 1; END$$\nDELIMITER ;\nCREATE TABLE b (id int);\n",
			words:  []string{"CREATE TABLE a", "CREATE PROCEDURE", "END", "CREATE TABLE b"},
		},
		{
			name:   "an Atlas directive",
			script: "-- atlas:delimiter ~~\nCREATE TABLE a (id int)~~\nCREATE TABLE b (id int)~~\n",
			words:  []string{"CREATE TABLE a", "CREATE TABLE b"},
		},
		{
			name:   "an Atlas delimiter that reads as a comment",
			script: "-- atlas:delimiter -- end\nCREATE TABLE a (id int)\n-- end\nCREATE TABLE b (id int)\n-- end\n",
			words:  []string{"CREATE TABLE a", "CREATE TABLE b"},
		},
		{
			name:   "a terminator after a semicolon, which is dropped",
			script: "DELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 1; END;$$\nCREATE TABLE b (id int)$$\n",
			words:  []string{"CREATE PROCEDURE", "END", "CREATE TABLE b"},
		},
		{
			name:   "a terminator of two newlines",
			script: "-- atlas:delimiter \\n\\n\nCREATE TABLE a (id int)\n\nCREATE TABLE b (id int)\n\n",
			words:  []string{"CREATE TABLE a", "CREATE TABLE b"},
		},
		{
			name:   "several directives in a row",
			script: "DELIMITER $$\nDELIMITER ~~\nDELIMITER ;\nCREATE TABLE a (id int);\n",
			words:  []string{"CREATE TABLE a"},
		},
		{
			name:   "CRLF around a directive",
			script: "CREATE TABLE a (id int);\r\nDELIMITER $$\r\nCREATE TABLE b (id int)$$\r\n",
			words:  []string{"CREATE TABLE a", "CREATE TABLE b"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rewritten, origins := clientdelimiter.Normalize(test.script)

			for _, word := range test.words {
				written := origins.Of(strings.Index(rewritten, word))
				c.Assert(test.script[written:written+len(word)], qt.Equals, word)
			}
			c.Assert(origins.Of(len(rewritten)), qt.Equals, len(test.script))
		})
	}
}

// The semicolon that stands for a terminator stands where the terminator stood.
func TestOrigins_ARewrittenTerminatorStandsWhereItWasWritten(t *testing.T) {
	c := qt.New(t)
	script := "DELIMITER $$\nCREATE TABLE a (id int)$$\n"

	rewritten, origins := clientdelimiter.Normalize(script)

	c.Assert(rewritten, qt.Equals, "CREATE TABLE a (id int);\n")
	c.Assert(origins.Of(strings.Index(rewritten, ";")), qt.Equals, strings.LastIndex(script, "$$"))
}

func TestOrigins_ZeroValueIsTheIdentity(t *testing.T) {
	c := qt.New(t)

	var origins clientdelimiter.Origins

	c.Assert(origins.Of(7), qt.Equals, 7)
}
