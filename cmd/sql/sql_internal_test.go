package sql

// White-box testing required: writeSQLLintReport is the command package's
// internal formatter boundary, and writer-error propagation is not exposed
// through the public cobra command API.

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/sqllint"
)

func TestWriteSQLLintReport_TextPropagatesWriterError(t *testing.T) {
	c := qt.New(t)
	errBoom := errors.New("boom")

	err := writeSQLLintReport(failingWriter{err: errBoom}, formatText, sqlLintReport{
		Findings: []sqllint.Finding{{
			Rule:     sqllint.RuleUnsupportedStatement,
			Severity: sqllint.SeverityError,
			File:     "schema.sql",
			Line:     1,
			Column:   1,
			Message:  "bad",
		}},
	})

	c.Assert(err, qt.ErrorIs, errBoom)
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}
