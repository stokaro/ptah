package atlashcl_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

func TestParseRefusesMalformedIndexPlatformBlocks(t *testing.T) {
	for _, test := range []struct {
		name  string
		block string
		want  string
	}{
		{"missing target", `platform {}`, `.*index platform block requires exactly one dialect label.*`},
		{"unknown attribute", `platform "clickhouse" { type = "minmax" }`, `.*unsupported index platform attribute "type".*`},
		{"duplicate property", `platform "clickhouse" {
  override "type" { value = "minmax" }
  override "type" { value = "set(10)" }
}`, `.*index platform override "type" for dialect "clickhouse" is duplicated.*`},
		{"missing property", `platform "clickhouse" {
  override "" { value = "minmax" }
}`, `.*index platform override requires exactly one key label.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := fmt.Sprintf(`table "events" {
  column "id" { type = bigint }
  index "by_id" {
    columns = [column.id]
    %s
  }
}`, test.block)
			db, err := atlashcl.Parse([]byte(source), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}
