package dbschema_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestLiveCapabilityPathsAvoidVersionBlindFactories(t *testing.T) {
	forbidden := map[string][]string{
		"connection.go": {
			`NewPostgreSQLReaderWithCapabilities\([^)]*capability\.ForDialect\(info\.Dialect\)`,
			`capability\.ForDialect\(info\.Dialect\)`,
		},
		filepath.Join("..", "migration", "generator", "generator.go"): {
			`planner\.GenerateSchemaDiff(AST|SQL|SQLStatements)\([^)]*(conn\.Info\(\)|info)\.Dialect`,
			`safety\.AssessRendered\([^)]*(conn\.Info\(\)|info)\.Dialect`,
			`generateUpMigrationSQL\([^)]*(conn\.Info\(\)|info)\.Dialect\)`,
			`generateDownMigrationSQL\([^)]*(conn\.Info\(\)|info)\.Dialect\)`,
		},
		filepath.Join("..", "cmd", "migrate", "migrate.go"): {
			`planner\.GenerateSchemaDiff(AST|SQL|SQLStatements)\([^)]*(conn\.Info\(\)|info)\.Dialect`,
			`safety\.AssessRendered\([^)]*(conn\.Info\(\)|info)\.Dialect`,
			`renderer\.RenderSQL\([^)]*(conn\.Info\(\)|info)\.Dialect`,
		},
		filepath.Join("..", "cmd", "compare", "compare.go"): {
			`planner\.GenerateSchemaDiff(AST|SQL|SQLStatements)\([^)]*(conn\.Info\(\)|info)\.Dialect`,
		},
		filepath.Join("..", "cmd", "readdb", "readdb.go"): {
			`renderer\.GetOrderedCreateStatements\([^)]*(conn\.Info\(\)|info)\.Dialect`,
		},
		filepath.Join("..", "integration", "framework.go"): {
			`planner\.GenerateSchemaDiff(AST|SQL|SQLStatements)\([^)]*(conn\.Info\(\)|info)\.Dialect`,
		},
	}

	for path, snippets := range forbidden {
		t.Run(path, func(t *testing.T) {
			c := qt.New(t)
			content, err := os.ReadFile(path)
			c.Assert(err, qt.IsNil)
			source := string(content)
			for _, pattern := range snippets {
				matched, err := regexp.MatchString(pattern, source)
				c.Assert(err, qt.IsNil)
				c.Assert(matched, qt.IsFalse, qt.Commentf("matched forbidden live capability pattern %q", pattern))
			}
		})
	}
}
