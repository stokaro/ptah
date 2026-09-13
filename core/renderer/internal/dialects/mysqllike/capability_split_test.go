package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
)

// sequenceSplitRow is one target and what the shared sequence handler makes of
// a sequence there.
type sequenceSplitRow struct {
	dialect    string
	wantCreate string
	wantDrop   string
}

// TestSequenceCapabilitySplit_HappyPath pins that one handler answers two ways.
//
// MySQL and MariaDB share a single CREATE SEQUENCE handler. It reads the
// target's capability set, so the same code writes DDL on MariaDB and a refusal
// comment on MySQL. A completeness table that records one verdict per handler
// records the wrong answer for this cell, whichever verdict it picks; only the
// pair states what the handler does.
//
// The same split runs through DROP SEQUENCE, which is the second signal: a
// change that lost the capability read would flip both columns of one row, and
// a change that hard-coded one target's answer would flip one column of both
// rows.
func TestSequenceCapabilitySplit_HappyPath(t *testing.T) {
	tests := []sequenceSplitRow{
		{
			dialect:    platform.MySQL,
			wantCreate: "-- CREATE SEQUENCE seq1 not supported in mysql\n",
			wantDrop:   "-- DROP SEQUENCE seq1 not supported in mysql\n",
		},
		{
			dialect:    platform.MariaDB,
			wantCreate: "CREATE SEQUENCE `seq1`;\n",
			wantDrop:   "DROP SEQUENCE `seq1`;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			created, err := renderer.RenderSQL(test.dialect, ast.NewCreateSequence("seq1"))
			c.Assert(err, qt.IsNil)
			c.Assert(created, qt.Equals, test.wantCreate)

			dropped, err := renderer.RenderSQL(test.dialect, ast.NewDropSequence("seq1"))
			c.Assert(err, qt.IsNil)
			c.Assert(dropped, qt.Equals, test.wantDrop)
		})
	}
}
