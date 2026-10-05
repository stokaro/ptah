package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
)

func TestParse_StreamingQuery_RefusesInvalidDeclarations(t *testing.T) {
	for _, text := range []string{
		"streaming_queries: {q: {run: false}}",
		"streaming_queries: {q: {text: 'DROP TABLE t;'}}",
		"streaming_queries: {q: {text: 'SELECT 1;', run: maybe}}",
		"streaming_queries: {q: {text: 'SELECT 1;', allow_state_reset: maybe}}",
		"streaming_queries: {q: {text: 'SELECT 1;', checkpoint: ignore}}",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			_, err := yamlschema.Parse([]byte(text))
			c.Assert(err, qt.IsNotNil)
		})
	}
}
