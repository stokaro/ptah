package datamigrate

// White-box testing required: tableBlock is unexported, and the label it
// writes comes from table names the exported planner reads from a database.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestTableBlock_KeepsTheLabelOnOneLine(t *testing.T) {
	c := qt.New(t)

	got := tableBlock("insert a\nDROP TABLE victim;", "INSERT INTO t VALUES (1);")

	c.Assert(got, qt.Equals, `-- data: insert a\nDROP TABLE victim;`+"\nINSERT INTO t VALUES (1);")
}
