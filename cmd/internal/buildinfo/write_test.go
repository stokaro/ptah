package buildinfo

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestWritePrintsStableCLIFormat(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	Write(&out, Info{
		Version:  "v1.2.3",
		Commit:   "abc123",
		Date:     "2026-07-21T20:00:00Z",
		Go:       "go1.26.5",
		Platform: "darwin/arm64",
	})

	c.Assert(out.String(), qt.Equals, ""+
		"Version: v1.2.3\n"+
		"Commit: abc123\n"+
		"Date: 2026-07-21T20:00:00Z\n"+
		"Go: go1.26.5\n"+
		"Platform: darwin/arm64\n")
}
